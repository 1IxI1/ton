/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#pragma once

#include "validator/fabric.h"

namespace ton::validator::consensus {

class ManagerFacade : public td::actor::Actor {
 public:
  virtual td::actor::Task<GeneratedCandidate> collate_block(CollateParams params,
                                                            td::CancellationToken cancellation_token) = 0;

  virtual td::actor::Task<ValidateCandidateResult> validate_block_candidate(BlockCandidate candidate,
                                                                            ValidateParams params,
                                                                            td::Timestamp timeout) = 0;

  virtual td::actor::Task<> accept_block(BlockIdExt id, td::Ref<BlockData> data, size_t creator_idx,
                                         td::Ref<block::BlockSignatureSet> signatures, int send_broadcast_mode,
                                         bool apply) = 0;

  virtual td::actor::Task<td::Ref<vm::Cell>> wait_block_state_root(BlockIdExt block_id, td::Timestamp timeout) = 0;
  virtual td::actor::Task<td::Ref<BlockData>> wait_block_data(BlockIdExt block_id, td::Timestamp timeout) = 0;

  virtual void cache_block_candidate(BlockCandidate candidate) {
  }

  virtual void send_block_candidate_broadcast(BlockIdExt id, td::BufferSlice data, int mode) {
  }

  // External messages included in a candidate (ours or a peer's): the mempool should stop
  // offering them to collations while the candidate's fate is undecided.
  virtual void ext_messages_seen_in_candidate(td::Bits256 candidate_id, std::vector<td::Ref<ExtMessage>> messages) {
  }

  // A final certificate collapsed histories: candidates in `finalized` are now part of the chain
  // (their externals must be dropped for good), candidates in `rejected` are discarded (their
  // externals must be returned to the mempool).
  virtual void ext_messages_history_collapsed(std::vector<td::Bits256> finalized, std::vector<td::Bits256> rejected) {
  }
};

}  // namespace ton::validator::consensus
