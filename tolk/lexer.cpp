/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.
*/
#include "lexer.h"
#include "compiler-state.h"
#include "symtable.h"
#include <cassert>

namespace tolk {

// By 'chunk' in lexer I mean a token or a list of tokens parsed simultaneously.
// E.g., when we meet "str", ChunkString is called, it emits tok_string.
// E.g., when we meet "str"x, ChunkString emits not only tok_string, but tok_string_modifier.
// E.g., when we meet //, ChunkInlineComment is called, it emits nothing (just skips a line).
// We store all valid chunks lexers in a prefix tree (LexingTrie), see below.
struct ChunkLexerBase {
  ChunkLexerBase(const ChunkLexerBase&) = delete;
  ChunkLexerBase &operator=(const ChunkLexerBase&) = delete;
  ChunkLexerBase() = default;

  virtual bool parse(Lexer* lex) const = 0;
  virtual ~ChunkLexerBase() = default;
};

template <class T>
static T* singleton() {
  static T obj;
  return &obj;
}

// LexingTrie is a prefix tree storing all available Tolk language constructs.
// It's effectively a map of a prefix to ChunkLexerBase.
class LexingTrie {
  LexingTrie** next{nullptr};   // either nullptr or [256]
  ChunkLexerBase* val{nullptr}; // non-null for leafs

  GNU_ATTRIBUTE_ALWAYS_INLINE void ensure_next_allocated() {
    if (next == nullptr) {
      next = new LexingTrie*[256];
      std::memset(next, 0, 256 * sizeof(LexingTrie*));
    }
  }

  GNU_ATTRIBUTE_ALWAYS_INLINE void ensure_symbol_allocated(uint8_t symbol) const {
    if (next[symbol] == nullptr) {
      next[symbol] = new LexingTrie;
    }
  }

public:
  // Maps a prefix onto a chunk lexer.
  // E.g. "    -> ChunkString
  // E.g. """  -> ChunkMultilineString
  void add_prefix(const char* s, ChunkLexerBase* val) {
    LexingTrie* cur = this;

    for (; *s; ++s) {
      uint8_t symbol = static_cast<uint8_t>(*s);
      cur->ensure_next_allocated();
      cur->ensure_symbol_allocated(symbol);
      cur = cur->next[symbol];
    }

#ifdef TOLK_DEBUG
    assert(!cur->val);
#endif
    cur->val = val;
  }

  // Maps a pattern onto a chunk lexer.
  // E.g. -[0-9] -> ChunkNegativeNumber
  // Internally, it expands the pattern to all possible prefixes: -0, -1, etc.
  // (for example, [0-9][a-z_$] gives 10*28=280 prefixes)
  void add_pattern(const char* pattern, ChunkLexerBase* val) {
    std::vector<LexingTrie*> all_possible_trie{this};

    for (const char* c = pattern; *c; ++c) {
      std::string to_append;
      if (*c == '[') {
        c++;
        while (*c != ']') { // assume that input is corrent, no out-of-string checks
          if (*(c + 1) == '-') {
            char l = *c, r = *(c + 2);
            for (char symbol = l; symbol <= r; ++symbol) {
              to_append += symbol;
            }
            c += 3;
          } else {
            to_append += *c;
            c++;
          }
        }
      } else {
        to_append += *c;
      }

      std::vector<LexingTrie*> next_all_possible_trie;
      next_all_possible_trie.reserve(all_possible_trie.size() * to_append.size());
      for (LexingTrie* cur : all_possible_trie) {
        cur->ensure_next_allocated();
        for (uint8_t symbol : to_append) {
          cur->ensure_symbol_allocated(symbol);
          next_all_possible_trie.emplace_back(cur->next[symbol]);
        }
      }
      all_possible_trie = std::move(next_all_possible_trie);
    }

    for (LexingTrie* trie : all_possible_trie) {
      trie->val = val;
    }
  }

  // Looks up a chunk lexer given a string (in practice, s points to cur position in the middle of the file).
  // It returns the deepest case: pointing to ", it will return ChunkMultilineString if """, or ChunkString otherwize.
  ChunkLexerBase* get_deepest(const char* s) const {
    const LexingTrie* best = this;

    for (const LexingTrie* cur = this; cur && cur->next; ++s) {
      cur = cur->next[static_cast<uint8_t>(*s)];  // if s reaches \0, cur will just become nullptr, and loop will end
      if (cur && cur->val) {
        best = cur;
      }
    }

    return best->val;
  }
};

//
// ----------------------------------------------------------------------
// A list of valid parsed chunks.
//

// An inline comment, starting from '//'
struct ChunkInlineComment final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    lex->skip_line();
    return true;
  }
};

// A multiline comment, starting from '/*'
// Note, that nested comments are not supported.
struct ChunkMultilineComment final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    while (!lex->is_eof()) {
      // todo drop -} later
      if ((lex->char_at() == '-' && lex->char_at(1) == '}') || (lex->char_at() == '*' && lex->char_at(1) == '/')) {
        lex->skip_chars(2);
        return true;
      }
      lex->skip_chars(1);
    }
    return true; // it's okay if comment extends past end of file
  }
};

// A string, starting from "
// Note, that there are no escape symbols inside: the purpose of strings in Tolk just doesn't need it.
// After a closing quote, a string modifier may be present, like "Ef8zMzMzMzMzMzMzMzMzMzM0vF"a.
// If present, it emits a separate tok_string_modifier.
struct ChunkString final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    const char* str_begin = lex->c_str();
    lex->skip_chars(1);
    while (!lex->is_eof() && lex->char_at() != '"' && lex->char_at() != '\n') {
      lex->skip_chars(1);
    }
    if (lex->char_at() != '"') {
      lex->error("string extends past end of line");
    }

    std::string_view str_val(str_begin + 1, lex->c_str() - str_begin - 1);
    lex->skip_chars(1);
    lex->add_token(tok_string_const, str_val);

    if (std::isalpha(lex->char_at())) {
      std::string_view modifier_val(lex->c_str(), 1);
      lex->skip_chars(1);
      lex->add_token(tok_string_modifier, modifier_val);
    }

    return true;
  }
};

// A string starting from """
// Used for multiline asm constructions. Can not have a postfix modifier.
struct ChunkMultilineString final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    const char* str_begin = lex->c_str();
    lex->skip_chars(3);
    while (!lex->is_eof()) {
      if (lex->char_at() == '"' && lex->char_at(1) == '"' && lex->char_at(2) == '"') {
        break;
      }
      lex->skip_chars(1);
    }
    if (lex->is_eof()) {
      lex->error("string extends past end of file");
    }

    std::string_view str_val(str_begin + 3, lex->c_str() - str_begin - 3);
    lex->skip_chars(3);
    lex->add_token(tok_string_const, str_val);
    return true;
  }
};

// A number, may be a hex one.
struct ChunkNumber final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    const char* str_begin = lex->c_str();
    bool hex = false;
    if (lex->char_at() == '0' && lex->char_at(1) == 'x') {
      lex->skip_chars(2);
      hex = true;
    }
    if (lex->is_eof()) {
      return false;
    }
    while (!lex->is_eof()) {
      char c = lex->char_at();
      if (c >= '0' && c <= '9') {
        lex->skip_chars(1);
        continue;
      }
      if (!hex) {
        break;
      }
      c |= 0x20;
      if (c < 'a' || c > 'f') {
        break;
      }
      lex->skip_chars(1);
    }

    std::string_view str_val(str_begin, lex->c_str() - str_begin);
    lex->add_token(tok_int_const, str_val);
    return true;
  }
};

// Anything starting from # is a compiler directive.
// Technically, #include and #pragma can be mapped as separate chunks,
// but storing such long strings in a trie increases its memory usage.
struct ChunkCompilerDirective final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    const char* str_begin = lex->c_str();

    lex->skip_chars(1);
    while (std::isalnum(lex->char_at())) {
      lex->skip_chars(1);
    }

    std::string_view str_val(str_begin, lex->c_str() - str_begin);
    if (str_val == "#include") {
      lex->add_token(tok_include, str_val);
      return true;
    }
    if (str_val == "#pragma") {
      lex->add_token(tok_pragma, str_val);
      return true;
    }

    lex->error("unknown compiler directive");
  }
};

// Tokens like !=, &, etc. emit just a simple TokenType.
// Since they are stored in trie, "parsing" them is just skipping len chars.
struct ChunkSimpleToken final : ChunkLexerBase {
  TokenType tp;
  int len;

  ChunkSimpleToken(TokenType tp, int len) : tp(tp), len(len) {}

  bool parse(Lexer* lex) const override {
    std::string_view str_val(lex->c_str(), len);
    lex->add_token(tp, str_val);
    lex->skip_chars(len);
    return true;
  }
};

// Spaces and other space-like symbols are just skipped.
struct ChunkSkipWhitespace final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    lex->skip_chars(1);
    lex->skip_spaces();
    return true;
  }
};

// Here we handle corner cases of grammar that are requested on demand.
// E.g., for 'pragma version >0.5.0', '0.5.0' should be parsed specially to emit tok_semver.
// See TolkLanguageGrammar::parse_next_chunk_special().
struct ChunkSpecialParsing {
  static bool parse_pragma_name(Lexer* lex) {
    const char* str_begin = lex->c_str();
    while (std::isalnum(lex->char_at()) || lex->char_at() == '-') {
      lex->skip_chars(1);
    }

    std::string_view str_val(str_begin, lex->c_str() - str_begin);
    if (str_val.empty()) {
      return false;
    }
    lex->add_token(tok_pragma_name, str_val);
    return true;
  }

  static bool parse_semver(Lexer* lex) {
    const char* str_begin = lex->c_str();
    while (std::isdigit(lex->char_at()) || lex->char_at() == '.') {
      lex->skip_chars(1);
    }

    std::string_view str_val(str_begin, lex->c_str() - str_begin);
    if (str_val.empty()) {
      return false;
    }
    lex->add_token(tok_semver, str_val);
    return true;
  }
};

// Anything starting from a valid identifier beginning symbol is parsed as an identifier.
// But if a resulting string is a keyword, a corresponding token is emitted instead of tok_identifier.
struct ChunkIdentifierOrKeyword final : ChunkLexerBase {
  // having parsed str up to the valid end, look up whether it's a valid keyword
  // in the future, this could be a bit more effective than just comparing strings (e.g. gperf),
  // but nevertheless, performance of the naive code below is reasonably good
  static TokenType maybe_keyword(std::string_view str) {
    switch (str.size()) {
      case 1:
        if (str == "~") return tok_bitwise_not;  // todo attention
        if (str == "_") return tok_underscore;  // todo attention
        break;
      case 2:
        if (str == "do") return tok_do;
        if (str == "if") return tok_if;
        break;
      case 3:
        if (str == "int") return tok_int;
        if (str == "var") return tok_var;
        if (str == "asm") return tok_asm;
        if (str == "get") return tok_get;
        if (str == "try") return tok_try;
        if (str == "nil") return tok_nil;
        break;
      case 4:
        if (str == "else") return tok_else;
        if (str == "true") return tok_true;
        if (str == "pure") return tok_pure;
        if (str == "then") return tok_then;
        if (str == "cell") return tok_cell;
        if (str == "cont") return tok_cont;
        break;
      case 5:
        if (str == "slice") return tok_slice;
        if (str == "tuple") return tok_tuple;
        if (str == "const") return tok_const;
        if (str == "false") return tok_false;
        if (str == "while") return tok_while;
        if (str == "until") return tok_until;
        if (str == "catch") return tok_catch;
        if (str == "ifnot") return tok_ifnot;
        break;
      case 6:
        if (str == "return") return tok_return;
        if (str == "repeat") return tok_repeat;
        if (str == "elseif") return tok_elseif;
        if (str == "forall") return tok_forall;
        if (str == "extern") return tok_extern;
        if (str == "global") return tok_global;
        if (str == "impure") return tok_impure;
        if (str == "inline") return tok_inline;
        break;
      case 7:
        if (str == "builder") return tok_builder;
        if (str == "builtin") return tok_builtin;
        break;
      case 8:
        if (str == "operator") return tok_operator;
        break;
      case 9:
        if (str == "elseifnot") return tok_elseifnot;
        if (str == "method_id") return tok_method_id;
        break;
      case 10:
        if (str == "inline_ref") return tok_inlineref;
        if (str == "auto_apply") return tok_autoapply;
        break;
      default:
        break;
    }
    return tok_empty;
  }

  bool parse(Lexer* lex) const override {
    const char* sym_begin = lex->c_str();
    lex->skip_chars(1);
    while (!lex->is_eof()) {
      char c = lex->char_at();
      // the pattern of valid identifier first symbol is provided in trie, here we test for identifier middle
      bool allowed_in_identifier = std::isalnum(c) || c == '_' || c == '$' || c == ':' || c == '?' || c == '!' || c == '\'';
      if (!allowed_in_identifier) {
        break;
      }
      lex->skip_chars(1);
    }

    std::string_view str_val(sym_begin, lex->c_str() - sym_begin);
    if (TokenType kw_tok = maybe_keyword(str_val)) {
      lex->add_token(kw_tok, str_val);
    } else {
      G.symbols.lookup_add(str_val);
      lex->add_token(tok_identifier, str_val);
    }
    return true;
  }
};

// Like in Kotlin, `backticks` can be used to wrap identifiers (both in declarations/usage, both for vars/functions).
// E.g.: function `do`() { var `with spaces` = 1; }
// This could be useful to use reserved names as identifiers (in a probable codegen from TL, for example).
struct ChunkIdentifierInBackticks final : ChunkLexerBase {
  bool parse(Lexer* lex) const override {
    const char* str_begin = lex->c_str();
    lex->skip_chars(1);
    while (!lex->is_eof() && lex->char_at() != '`' && lex->char_at() != '\n') {
      if (std::isspace(lex->char_at())) { // probably, I'll remove this restriction after rewriting symtable and cur_sym_idx
        lex->error("An identifier can't have a space in its name (even inside backticks)");
      }
      lex->skip_chars(1);
    }
    if (lex->char_at() != '`') {
      lex->error("Unclosed backtick `");
    }

    std::string_view str_val(str_begin + 1, lex->c_str() - str_begin - 1);
    lex->skip_chars(1);
    G.symbols.lookup_add(str_val);
    lex->add_token(tok_identifier, str_val);
    return true;
  }
};

//
// ----------------------------------------------------------------------
// Here we define a grammar of Tolk.
// All valid chunks prefixes are stored in trie.
//

struct TolkLanguageGrammar {
  static LexingTrie trie;

  static bool parse_next_chunk(Lexer* lex) {
    const ChunkLexerBase* best = trie.get_deepest(lex->c_str());
    return best && best->parse(lex);
  }

  static bool parse_next_chunk_special(Lexer* lex, TokenType parse_next_as) {
    switch (parse_next_as) {
      case tok_pragma_name:
        return ChunkSpecialParsing::parse_pragma_name(lex);
      case tok_semver:
        return ChunkSpecialParsing::parse_semver(lex);
      default:
        assert(false);
        return false;
    }
  }

  static void register_token(const char* str, int len, TokenType tp) {
    trie.add_prefix(str, new ChunkSimpleToken(tp, len));
  }

  static void init() {
    trie.add_prefix("//", singleton<ChunkInlineComment>());
    trie.add_prefix(";;", singleton<ChunkInlineComment>());
    trie.add_prefix("/*", singleton<ChunkMultilineComment>());
    trie.add_prefix("{-", singleton<ChunkMultilineComment>());
    trie.add_prefix(R"(")", singleton<ChunkString>());
    trie.add_prefix(R"(""")", singleton<ChunkMultilineString>());
    trie.add_prefix(" ", singleton<ChunkSkipWhitespace>());
    trie.add_prefix("\t", singleton<ChunkSkipWhitespace>());
    trie.add_prefix("\r", singleton<ChunkSkipWhitespace>());
    trie.add_prefix("\n", singleton<ChunkSkipWhitespace>());
    trie.add_prefix("#", singleton<ChunkCompilerDirective>());

    trie.add_pattern("[0-9]", singleton<ChunkNumber>());
    // todo think of . ~
    trie.add_pattern("[a-zA-Z_$.~]", singleton<ChunkIdentifierOrKeyword>());
    trie.add_prefix("`", singleton<ChunkIdentifierInBackticks>());

    register_token("+", 1, tok_plus);
    register_token("-", 1, tok_minus);
    register_token("*", 1, tok_mul);
    register_token("/", 1, tok_div);
    register_token("%", 1, tok_mod);
    register_token("?", 1, tok_question);
    register_token(":", 1, tok_colon);
    register_token(",", 1, tok_comma);
    register_token(";", 1, tok_semicolon);
    register_token("(", 1, tok_oppar);
    register_token(")", 1, tok_clpar);
    register_token("[", 1, tok_opbracket);
    register_token("]", 1, tok_clbracket);
    register_token("{", 1, tok_opbrace);
    register_token("}", 1, tok_clbrace);
    register_token("=", 1, tok_assign);
    register_token("<", 1, tok_lt);
    register_token(">", 1, tok_gt);
    register_token("&", 1, tok_bitwise_and);
    register_token("|", 1, tok_bitwise_or);
    register_token("^", 1, tok_bitwise_xor);
    register_token("==", 2, tok_eq);
    register_token("!=", 2, tok_neq);
    register_token("<=", 2, tok_leq);
    register_token(">=", 2, tok_geq);
    register_token("<<", 2, tok_lshift);
    register_token(">>", 2, tok_rshift);
    register_token("~/", 2, tok_divR);
    register_token("^/", 2, tok_divC);
    register_token("~%", 2, tok_modR);
    register_token("^%", 2, tok_modC);
    register_token("/%", 2, tok_divmod);
    register_token("+=", 2, tok_set_plus);
    register_token("-=", 2, tok_set_minus);
    register_token("*=", 2, tok_set_mul);
    register_token("/=", 2, tok_set_div);
    register_token("%=", 2, tok_set_mod);
    register_token("&=", 2, tok_set_bitwise_and);
    register_token("|=", 2, tok_set_bitwise_or);
    register_token("^=", 2, tok_set_bitwise_xor);
    register_token("->", 2, tok_mapsto);
    register_token("<=>", 3, tok_spaceship);
    register_token("~>>", 3, tok_rshiftR);
    register_token("^>>", 3, tok_rshiftC);
    register_token("~/=", 3, tok_set_divR);
    register_token("^/=", 3, tok_set_divC);
    register_token("~%=", 3, tok_set_modR);
    register_token("^%=", 3, tok_set_modC);
    register_token("<<=", 3, tok_set_lshift);
    register_token(">>=", 3, tok_set_rshift);
    register_token("~>>=", 4, tok_set_rshiftR);
    register_token("^>>=", 4, tok_set_rshiftC);
  }
};

LexingTrie TolkLanguageGrammar::trie;

//
// ----------------------------------------------------------------------
// The Lexer class is to be used outside (by parser, which constructs AST from tokens).
// It's streaming. It means, that `next()` parses a next token on demand
// (instead of parsing all file contents to vector<Token> and iterating over it).
// Parsing on demand uses effectively less memory.
// Note, that chunks, being parsed, call `add_token()`, and a chunk may add multiple tokens at once.
// That's why a small cirlular buffer for tokens is used.
// `last_token_idx` actually means a number of total tokens added.
// `cur_token_idx` is a number of returned by `next()`.
// It's assumed that an input file has already been loaded, its contents is present and won't be deleted
// (`start`, `cur` and `end`, as well as every Token str_val, points inside file->text).
//

Lexer::Lexer(const SrcFile* file)
  : file(file)
  , p_start(file->text.data())
  , p_end(p_start + file->text.size())
  , p_next(p_start)
  , location(file) {
  next();
}

void Lexer::next() {
  while (cur_token_idx == last_token_idx && !is_eof()) {
    update_location();
    if (!TolkLanguageGrammar::parse_next_chunk(this)) {
      error("Failed to parse");
    }
  }
  if (is_eof()) {
    add_token(tok_eof, file->text);
  }
  cur_token = tokens_circularbuf[++cur_token_idx & 7];
}

void Lexer::next_special(TokenType parse_next_as, const char* str_expected) {
  assert(cur_token_idx == last_token_idx);
  skip_spaces();
  update_location();
  if (!TolkLanguageGrammar::parse_next_chunk_special(this, parse_next_as)) {
    error(std::string(str_expected) + " expected");
  }
  cur_token = tokens_circularbuf[++cur_token_idx & 7];
}

void Lexer::error(const std::string& err_msg) const {
  throw ParseError(cur_location(), err_msg);
}

void Lexer::on_expect_call_failed(const char* str_expected) const {
  throw ParseError(cur_location(), std::string(str_expected) + " expected instead of `" + std::string(cur_str()) + "`");
}

void lexer_init() {
  TolkLanguageGrammar::init();
}

// todo #ifdef TOLK_PROFILING
// As told above, `next()` produces tokens on demand, while AST is being generated.
// Hence, it's difficult to measure Lexer performance separately.
// This function can be called just to tick Lexer performance, it just scans all input files.
// There is no sense to use it in production, but when refactoring and optimizing Lexer, it's useful.
void lexer_measure_performance(const AllSrcFiles& files_to_just_parse) {
  for (const SrcFile* file : files_to_just_parse) {
    Lexer lex(file);
    while (!lex.is_eof()) {
      lex.next();
    }
  }
}

}  // namespace tolk
