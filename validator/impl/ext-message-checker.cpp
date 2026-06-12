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
#include "ext-message-checker.hpp"

#include "block/block-auto.h"
#include "block/block-parse.h"
#include "block/transaction.h"
#include "td/utils/Timer.h"
#include "vm/dict.h"

#include "fabric.h"

namespace ton::validator {

td::actor::Task<ExtMessageChecker::CheckedExtMsg> ExtMessageChecker::check(td::BufferSlice data,
                                                                           block::SizeLimitsConfig::ExtMsgLimits limits,
                                                                           td::Ref<MasterchainState> mc_state) {
  CheckedExtMsg result;
  td::Timer timer;
  auto message = co_await create_ext_message(std::move(data), limits);
  result.message = message;
  result.timings.parse = timer.elapsed();

  WorkchainId wc = message->wc();
  StdSmcAddress addr = message->addr();
  // Note: the per-address rate limit is enforced by the pool at finalization (a mid-check
  // round-trip to the pool costs more than it saves: under load it adds milliseconds of
  // pool-mailbox latency per message and idles the workers).

  timer = td::Timer();
  auto state = co_await resolve_state(mc_state, message->shard());
  result.timings.fetch_state = timer.elapsed();

  timer = td::Timer();
  vm::AugmentedDictionary accounts_dict{state.accounts_root, 256, block::tlb::aug_ShardAccounts};
  auto shard_acc = accounts_dict.lookup(addr);
  result.timings.lookup = timer.elapsed();

  timer = td::Timer();
  bool special = wc == masterchainId && config_->is_special_smartcontract(addr);
  block::Account acc;
  if (!acc.unpack(std::move(shard_acc), state.utime, special)) {
    co_return td::Status::Error("Failed to unpack account state");
  }
  acc.block_lt = state.lt;

  // NOTE: exec_config stays valid below because nothing in between actually suspends (the
  // co_awaits unwrap ready td::Result values); only this worker's tasks mutate exec_configs_.
  auto &exec_config = exec_configs_[{wc, state.utime}];
  if (exec_config == nullptr) {
    exec_config = co_await ExtMessageQ::ExecutionConfig::create(*config_, wc, state.utime);
    if (exec_configs_.size() > 16) {
      std::erase_if(exec_configs_,
                    [&](const auto &kv) { return kv.second == nullptr || kv.first.second + 60 < state.utime; });
    }
  }

  const WalletMessageProcessor *wallet =
      acc.code.not_null() ? WalletMessageProcessor::get(acc.code->get_hash().bits()) : nullptr;
  if (wallet == nullptr) {
    co_await ExtMessageQ::run_message_on_account(wc, &acc, state.utime, state.lt + 1, message->root_cell(),
                                                 *exec_config);
    result.timings.vm = timer.elapsed();
    co_return result;
  }

  LOG(DEBUG) << "Checking external message to " << wc << ":" << addr.to_hex() << ", " << wallet->name();
  auto wallet_seqno = co_await wallet->get_wallet_seqno(acc.data);
  auto [msg_seqno, msg_valid_until] = co_await wallet->parse_message(message->root_cell());
  LOG(DEBUG) << "External message to " << wallet->name() << ": msg_seqno=" << msg_seqno
             << ", msg_ttl=" << msg_valid_until << ", wallet_seqno=" << wallet_seqno;
  if (msg_valid_until <= (UnixTime)td::Clocks::system()) {
    co_return td::Status::Error("valid_until is in the past");
  }
  if (msg_seqno < wallet_seqno) {
    co_return td::Status::Error(PSTRING() << "Too old seqno: msg_seqno=" << msg_seqno
                                          << ", wallet_seqno=" << wallet_seqno);
  }
  if (msg_seqno - wallet_seqno > MAX_WALLET_SEQNO_DIFF) {
    co_return td::Status::Error(PSTRING() << "Too new seqno: msg_seqno=" << msg_seqno
                                          << ", wallet_seqno=" << wallet_seqno);
  }
  // Note: the duplicate-seqno check against other in-flight messages is pool state; the pool
  // performs it at finalization (after this VM run instead of before it — same admission verdict).
  acc.data = co_await wallet->set_wallet_seqno(acc.data, msg_seqno);
  acc.storage_dict_hash = acc.orig_storage_dict_hash = {};
  co_await ExtMessageQ::run_message_on_account(wc, &acc, state.utime, state.lt + 1, message->root_cell(),
                                               *exec_config);
  result.timings.vm = timer.elapsed();
  result.is_wallet = true;
  result.msg_seqno = msg_seqno;
  result.msg_valid_until = msg_valid_until;
  result.wallet_seqno = wallet_seqno;
  result.state_utime = state.utime;
  LOG(DEBUG) << "Checked external message to " << wc << ":" << addr.to_hex() << ", " << wallet->name();
  co_return result;
}

td::actor::Task<ExtMessageChecker::ResolvedState> ExtMessageChecker::resolve_state(td::Ref<MasterchainState> mc_state,
                                                                                   AccountIdPrefixFull prefix) {
  // Refresh the per-mc-block config. Extracted once per masterchain block per worker instead of
  // once per message (the old LiteQuery-based fetch re-extracted the full config every message).
  if (config_ == nullptr || config_mc_block_id_ != mc_state->get_block_id()) {
    config_ = co_await block::ConfigInfo::extract_config(mc_state->root_cell(), mc_state->get_block_id(), 0xFFFF);
    config_mc_block_id_ = mc_state->get_block_id();
    exec_configs_.clear();
  }

  BlockIdExt block_id;
  if (prefix.workchain == masterchainId) {
    block_id = mc_state->get_block_id();
  } else {
    auto shard_hash = mc_state->get_shard_from_config(shard_prefix(prefix, 60), false);
    if (shard_hash.is_null()) {
      co_return td::Status::Error(ErrorCode::notready, PSTRING() << "no shard in masterchain state for account "
                                                                 << prefix.workchain << ":"
                                                                 << td::format::as_hex(prefix.account_id_prefix));
    }
    block_id = shard_hash->top_block_id();
  }

  auto make_resolved = [](const CachedState &entry) {
    return ResolvedState{entry.accounts_root, entry.utime, entry.lt};
  };
  {
    auto it = states_.find(block_id.shard_full());
    if (it != states_.end() && it->second.block_id == block_id) {
      co_return make_resolved(it->second);
    }
  }

  td::Ref<ShardState> state;
  if (prefix.workchain == masterchainId) {
    state = mc_state;
  } else {
    state = co_await td::actor::ask(manager_, &ValidatorManager::wait_block_state_short, block_id, (td::uint32)0,
                                    td::Timestamp::in(10.0), false);
  }
  // We may have suspended above: another in-flight check on this worker may have already
  // populated the cache entry in the meantime.
  auto &entry = states_[block_id.shard_full()];
  if (entry.block_id != block_id || entry.state.is_null()) {
    block::gen::ShardStateUnsplit::Record sstate;
    if (!tlb::unpack_cell(state->root_cell(), sstate)) {
      co_return td::Status::Error("cannot unpack shard state header");
    }
    entry = CachedState{block_id, std::move(state), vm::load_cell_slice_ref(sstate.accounts), sstate.gen_utime,
                        sstate.gen_lt};
    if (states_.size() > 64) {
      // Drop stale shards (after splits/merges); the hot entries repopulate on the next message.
      std::erase_if(states_, [&](const auto &kv) { return kv.second.block_id != block_id; });
    }
  }
  co_return make_resolved(entry);
}

}  // namespace ton::validator
