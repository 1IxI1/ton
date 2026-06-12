/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "block/block-parse.h"
#include "vm/boc.h"
#include "vm/cells/MerkleUpdate.h"
#include "vm/dict.h"

#include "block-auto.h"
#include "fabric.h"
#include "utils.h"

namespace ton::validator::consensus {

td::Result<double> get_candidate_gen_utime_exact(const BlockCandidate& candidate) {
  TRY_RESULT(cdata_roots, vm::std_boc_deserialize_multi(candidate.collated_data));
  for (const td::Ref<vm::Cell>& root : cdata_roots) {
    if (!block::gen::t_ConsensusExtraData.validate_ref(10000, root)) {
      continue;
    }
    block::gen::ConsensusExtraData::Record rec;
    CHECK(block::gen::unpack_cell(root, rec));
    return (double)rec.gen_utime_ms / 1000.0;
  }
  return td::Status::Error("no ConsensusExtraData in candidate");
}

td::Result<std::vector<td::Ref<ExtMessage>>> extract_accepted_externals(const CandidateRef& candidate) {
  if (candidate->is_empty()) {
    return std::vector<td::Ref<ExtMessage>>{};
  }
  const auto& block = std::get<BlockCandidate>(candidate->block);

  if (block.accepted_ext_messages) {
    std::vector<td::Ref<ExtMessage>> result;
    for (const auto& cell : *block.accepted_ext_messages) {
      auto ext_msg = create_ext_message(cell);
      if (ext_msg.is_error()) {
        return ext_msg.move_as_error_prefix("cannot reconstruct external message: ");
      }
      result.push_back(ext_msg.move_as_ok());
    }
    return result;
  }

  TRY_RESULT(block_root, vm::std_boc_deserialize(block.data));

  block::gen::Block::Record blk;
  block::gen::BlockExtra::Record extra;
  if (!(tlb::unpack_cell(block_root, blk) && tlb::unpack_cell(blk.extra, extra))) {
    return td::Status::Error("cannot unpack block");
  }

  vm::AugmentedDictionary in_msg_dict{vm::load_cell_slice_ref(extra.in_msg_descr), 256,
                                      block::tlb::aug_InMsgDescrDefault};

  std::vector<td::Ref<ExtMessage>> result;
  td::Status error;
  in_msg_dict.check_for_each_extra(
      [&](td::Ref<vm::CellSlice> value, td::Ref<vm::CellSlice>, td::ConstBitPtr, int key_len) {
        if (key_len != 256) {
          error = td::Status::Error("invalid InMsgDescr key length");
          return false;
        }
        int tag = block::gen::t_InMsg.get_tag(*value);
        if (tag != block::gen::InMsg::msg_import_ext) {
          return true;
        }
        vm::CellSlice cs{*value};
        td::Ref<vm::Cell> msg, transaction;
        if (!block::gen::t_InMsg.unpack_msg_import_ext(cs, msg, transaction)) {
          error = td::Status::Error("cannot unpack msg_import_ext");
          return false;
        }
        auto ext_msg = create_ext_message(std::move(msg));
        if (ext_msg.is_error()) {
          error = ext_msg.move_as_error_prefix("cannot reconstruct external message: ");
          return false;
        }
        result.push_back(ext_msg.move_as_ok());
        return true;
      });
  if (error.is_error()) {
    return std::move(error);
  }
  return result;
}

}  // namespace ton::validator::consensus
