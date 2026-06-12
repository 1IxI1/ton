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
#include "td/utils/Random.h"
#include "ton/ton-io.hpp"

#include "ext-message-pool.hpp"
#include "external-message.hpp"
#include "fabric.h"
#include "transaction.h"

namespace ton::validator {
td::actor::Task<ExtMessagePool::CheckResult> ExtMessagePool::check_add_external_message(td::BufferSlice data,
                                                                                        int priority,
                                                                                        bool add_to_mempool) {
  if (last_masterchain_state_.is_null()) {
    co_return td::Status::Error(ErrorCode::notready, "not ready");
  }
  auto message = co_await create_ext_message(std::move(data), last_masterchain_state_->get_ext_msg_limits());
  WorkchainId wc = message->wc();
  StdSmcAddress addr = message->addr();
  if (checked_ext_msg_counter_.get_msg_count(wc, addr) >= MAX_EXT_MSG_PER_ADDR) {
    co_return td::Status::Error(PSTRING() << "too many external messages to address " << wc << ":" << addr.to_hex());
  }
  td::optional<td::uint32> msg_seqno;
  auto result = co_await check_message(message, msg_seqno).wrap();
  ++(result.is_ok() ? total_check_ext_messages_ok_ : total_check_ext_messages_error_);
  if (result.is_error()) {
    co_return result.move_as_error();
  }
  if (checked_ext_msg_counter_.inc_msg_count(wc, addr) > MAX_EXT_MSG_PER_ADDR) {
    co_return td::Status::Error(PSTRING() << "too many external messages to address " << wc << ":" << addr.to_hex());
  }
  if (add_to_mempool) {
    add_message_to_mempool(message, priority, msg_seqno);
  }
  co_return result.move_as_ok();
}

void ExtMessagePool::install_collator_queue(ShardIdFull shard, std::unique_ptr<ExtMsgCallback> callback) {
  // Compute shard key range [lo, hi) for splitting
  td::uint64 lo_prefix = shard.shard & (shard.shard - 1);
  td::uint64 hi_prefix_plus1 = (shard.shard | (shard.shard - 1)) + 1;  // may overflow to 0
  MessageId shard_lo{AccountIdPrefixFull{shard.workchain, lo_prefix}, Bits256::zero()};
  MessageId shard_hi{AccountIdPrefixFull{hi_prefix_plus1 == 0 ? shard.workchain + 1 : shard.workchain, hi_prefix_plus1},
                     Bits256::zero()};

  // Take O(log n) shard slices from each priority level
  Snapshot snapshot;
  for (auto it = ext_msgs_.rbegin(); it != ext_msgs_.rend(); ++it) {
    auto [_, in_shard, __] = it->second.ext_messages_.split_range(shard_lo, shard_hi);
    if (!in_shard.empty()) {
      snapshot.emplace_back(it->first, std::move(in_shard));
    }
  }

  // Spawn a coroutine that drains the shard slices randomly into the queue
  push_existing_to_queue(callback->queue, callback->cancellation_token, shard, std::move(snapshot),
                         callback->sync_only)
      .start()
      .detach();

  if (!callback->sync_only) {
    alarm_timestamp().relax(callback->timeout);
    callbacks_.push_back(std::move(callback));
  }
}

td::actor::Task<> ExtMessagePool::push_existing_to_queue(ExtMsgQueue queue, td::CancellationToken token,
                                                         ShardIdFull shard, Snapshot snapshot, bool sync_only) {
  // Runs detached on the pool actor: `this` stays valid for the lifetime of the process and
  // accessing `held_externals_` after co_await is safe (we resume on the same actor).
  SCOPE_EXIT {
    if (sync_only) {
      queue.close();
    }
  };
  td::Timer t;
  size_t pushed = 0;
  size_t held = 0;
  for (auto &[priority, treap] : snapshot) {
    while (!treap.empty()) {
      if (token.check().is_error()) {
        co_return {};
      }
      size_t idx = td::Random::fast_uint32() % treap.size();
      auto [key, msg] = treap.at(idx);
      treap = treap.erase_at(idx);  // local snapshot only
      if (msg->expired() || !msg->is_active()) {
        continue;
      }
      if (is_held(key.hash)) {
        // Already included in an in-flight candidate; don't offer it to this collation.
        ++held;
        continue;
      }
      bool ok = co_await queue.push(std::make_pair(msg->message, priority));
      if (!ok) {
        co_return {};
      }
      ++pushed;
    }
  }
  LOG(WARNING) << "install_collator_queue: pushed " << pushed << " existing messages (" << held
               << " held by in-flight candidates) to shard " << shard << " in " << t.elapsed() << "s";
  co_return {};
}

void ExtMessagePool::cleanup_external_messages(ShardIdFull shard) {
  // Clean up expired messages
  for (auto &[priority, msgs] : ext_msgs_) {
    std::vector<MessageId> to_erase;
    for (size_t i = 0; i < msgs.ext_messages_.size(); i++) {
      auto [key, msg] = msgs.ext_messages_.at(i);
      if (shard_contains(shard, key.dst) && msg->expired()) {
        to_erase.push_back(key);
      }
    }
    for (auto &id : to_erase) {
      erase_message(priority, id);
    }
  }
}

void ExtMessagePool::complete_external_messages(std::vector<ExtMessage::Hash> to_delay,
                                                std::vector<ExtMessage::Hash> to_delete) {
  for (auto &hash : to_delete) {
    auto it = ext_messages_hashes_.find(hash);
    if (it != ext_messages_hashes_.end()) {
      erase_message(it->second.first, it->second.second);
    }
  }
  for (auto &hash : to_delay) {
    auto it = ext_messages_hashes_.find(hash);
    if (it != ext_messages_hashes_.end()) {
      int priority = it->second.first;
      auto msg_id = it->second.second;
      auto &msgs = ext_msgs_[priority];
      auto msg_opt = msgs.ext_messages_.find(msg_id);
      if (msg_opt && msgs.ext_messages_.size() < SOFT_MEMPOOL_LIMIT && msg_opt.value()->can_postpone()) {
        msg_opt.value()->postpone();
      } else {
        erase_message(priority, msg_id);
      }
    }
  }
}

void ExtMessagePool::erase_external_messages(std::vector<ExtMessage::Hash> to_delete) {
  applied_ext_msgs_delete_requests_ += to_delete.size();
  for (auto &hash : to_delete) {
    auto it = ext_messages_hashes_norm_.find(hash);
    if (it != ext_messages_hashes_norm_.end()) {
      auto ids = it->second;
      for (const auto &message_id : ids) {
        if (erase_message(message_id.priority, message_id.id)) {
          ++applied_ext_msgs_deleted_;
        }
      }
    }
  }
}

bool ExtMessagePool::erase_message(int priority, const MessageId &id) {
  auto it_priority = ext_msgs_.find(priority);
  if (it_priority == ext_msgs_.end()) {
    return false;
  }
  auto &msgs = it_priority->second;
  auto msg_opt = msgs.ext_messages_.find(id);
  if (!msg_opt) {
    return false;
  }

  auto address = msg_opt.value()->address();
  auto hash_norm = msg_opt.value()->hash_norm;
  msgs.ext_addr_messages_[address].erase(id.hash);
  msgs.ext_messages_ = msgs.ext_messages_.erase(id);
  ext_messages_hashes_.erase(id.hash);

  auto it_norm = ext_messages_hashes_norm_.find(hash_norm);
  if (it_norm != ext_messages_hashes_norm_.end()) {
    it_norm->second.erase(NormalizedMessageId{priority, id});
    if (it_norm->second.empty()) {
      ext_messages_hashes_norm_.erase(it_norm);
    }
  }
  return true;
}

void ExtMessagePool::candidate_externals_seen(td::Bits256 candidate_id, std::vector<td::Ref<ExtMessage>> messages) {
  if (messages.empty() || candidate_externals_.contains(candidate_id)) {
    return;
  }
  while (candidate_externals_.size() >= MAX_TRACKED_CANDIDATES) {
    // Bound memory: drop the record closest to expiry (unholds its externals).
    auto victim = candidate_externals_.begin();
    for (auto it = candidate_externals_.begin(); it != candidate_externals_.end(); ++it) {
      if (it->second.expires_at < victim->second.expires_at) {
        victim = it;
      }
    }
    drop_candidate_externals(victim);
  }
  for (const auto &message : messages) {
    ++held_externals_[message->hash()];
  }
  ++total_candidates_seen_;
  total_ext_msgs_held_ += messages.size();
  LOG(INFO) << "ext pool: holding " << messages.size() << " externals included in candidate "
            << candidate_id.to_hex().substr(0, 16);
  auto expires_at = td::Timestamp::in(CANDIDATE_EXTERNALS_TTL);
  candidate_externals_.emplace(candidate_id, CandidateExternals{std::move(messages), expires_at});
  alarm_timestamp().relax(expires_at);
}

void ExtMessagePool::history_collapsed(std::vector<td::Bits256> finalized, std::vector<td::Bits256> rejected) {
  // Process the finalized chain first so that re-adding from rejected candidates can skip
  // messages that also made it into the finalized chain.
  size_t finalized_candidates = 0, dropped = 0;
  for (const auto &candidate_id : finalized) {
    auto it = candidate_externals_.find(candidate_id);
    if (it == candidate_externals_.end()) {
      continue;
    }
    ++finalized_candidates;
    for (const auto &message : it->second.messages) {
      auto hash = message->hash();
      remember_finalized(hash);
      remember_finalized(message->hash_norm());
      unhold(hash);
      // Erase the message and all its normalized duplicates from the mempool for good.
      auto it_norm = ext_messages_hashes_norm_.find(message->hash_norm());
      if (it_norm != ext_messages_hashes_norm_.end()) {
        auto ids = it_norm->second;
        for (const auto &message_id : ids) {
          dropped += erase_message(message_id.priority, message_id.id);
        }
      }
    }
    total_ext_msgs_finalized_ += it->second.messages.size();
    candidate_externals_.erase(it);
  }
  total_candidates_finalized_ += finalized_candidates;

  size_t rejected_candidates = 0, readded = 0, skipped = 0;
  for (const auto &candidate_id : rejected) {
    auto it = candidate_externals_.find(candidate_id);
    if (it == candidate_externals_.end()) {
      continue;
    }
    ++rejected_candidates;
    for (const auto &message : it->second.messages) {
      auto hash = message->hash();
      unhold(hash);
      if (was_recently_finalized(hash) || was_recently_finalized(message->hash_norm()) || is_held(hash)) {
        ++skipped;
        continue;
      }
      if (readd_message(message)) {
        ++readded;
      }
    }
    candidate_externals_.erase(it);
  }
  total_candidates_rejected_ += rejected_candidates;
  total_ext_msgs_readded_ += readded;

  if (rejected_candidates != 0 || finalized_candidates != 0) {
    LOG(WARNING) << "ext pool: readded " << readded << " externals from " << rejected_candidates
                 << " rejected candidates (skipped " << skipped << " finalized-or-held); dropped " << dropped
                 << " externals included in " << finalized_candidates << " finalized candidates";
  }
}

void ExtMessagePool::unhold(const ExtMessage::Hash &hash) {
  auto it = held_externals_.find(hash);
  if (it == held_externals_.end()) {
    return;
  }
  if (--it->second == 0) {
    held_externals_.erase(it);
  }
}

void ExtMessagePool::drop_candidate_externals(std::map<td::Bits256, CandidateExternals>::iterator it) {
  // Candidate fate unknown (TTL or eviction): just make its externals available again. If it was
  // actually finalized, the messages will be rejected as stale by the next collation and expire.
  for (const auto &message : it->second.messages) {
    unhold(message->hash());
  }
  candidate_externals_.erase(it);
}

bool ExtMessagePool::readd_message(const td::Ref<ExtMessage> &message) {
  auto it = ext_messages_hashes_.find(message->hash());
  if (it != ext_messages_hashes_.end()) {
    // Still in the mempool (it was merely held); reactivate it in case completion postponed it
    // and offer it to running collations right away.
    auto [priority, id] = it->second;
    auto msg_opt = ext_msgs_[priority].ext_messages_.find(id);
    CHECK(msg_opt);
    if (msg_opt.value()->expired()) {
      return false;
    }
    msg_opt.value()->active = true;
    msg_opt.value()->reactivate_at = {};
    offer_to_callbacks(message, priority);
    return true;
  }
  // Not in the mempool (e.g. only seen in a peer's candidate, or already erased as stale):
  // insert it back. Note: this does not touch the per-address admission counter.
  size_t before = ext_messages_hashes_.size();
  add_message_to_mempool(message, 0, {});
  return ext_messages_hashes_.size() != before;
}

void ExtMessagePool::offer_to_callbacks(const td::Ref<ExtMessage> &message, int priority) {
  std::erase_if(callbacks_, [&](const std::unique_ptr<ExtMsgCallback> &callback) -> bool {
    if (callback->cancellation_token.check().is_error()) {
      return true;
    }
    if (shard_contains(callback->shard, message->shard())) {
      callback->queue.try_push(std::make_pair(message, priority)).detach();
    }
    return false;
  });
}

std::vector<std::pair<std::string, std::string>> ExtMessagePool::prepare_stats() {
  std::vector<std::pair<std::string, std::string>> vec;
  vec.emplace_back("total.ext_msg_check",
                   PSTRING() << "ok:" << total_check_ext_messages_ok_ << " error:" << total_check_ext_messages_error_);
  vec.emplace_back("total.ext_msg_applied_cleanup", PSTRING() << "requested:" << applied_ext_msgs_delete_requests_
                                                              << " deleted:" << applied_ext_msgs_deleted_);
  vec.emplace_back("total.ext_msg_candidates", PSTRING() << "seen:" << total_candidates_seen_
                                                         << " finalized:" << total_candidates_finalized_
                                                         << " rejected:" << total_candidates_rejected_);
  vec.emplace_back("total.ext_msg_simplex", PSTRING() << "held:" << total_ext_msgs_held_
                                                      << " finalized:" << total_ext_msgs_finalized_
                                                      << " readded:" << total_ext_msgs_readded_);
  return vec;
}

void ExtMessagePool::alarm() {
  if (cleanup_mempool_at_.is_in_past()) {
    cleanup_external_messages(ShardIdFull{masterchainId, shardIdAll});
    cleanup_external_messages(ShardIdFull{basechainId, shardIdAll});
    cleanup_mempool_at_ = td::Timestamp::in(250.0);
  }
  alarm_timestamp().relax(cleanup_mempool_at_);
  for (auto it = candidate_externals_.begin(); it != candidate_externals_.end();) {
    auto cur = it++;
    if (cur->second.expires_at.is_in_past()) {
      LOG(INFO) << "ext pool: candidate " << cur->first.to_hex().substr(0, 16) << " with "
                << cur->second.messages.size() << " externals expired without a finalization verdict";
      drop_candidate_externals(cur);
    } else {
      alarm_timestamp().relax(cur->second.expires_at);
    }
  }
  if (rotate_finalized_externals_at_.is_in_past()) {
    finalized_externals_prev_ = std::move(finalized_externals_cur_);
    finalized_externals_cur_.clear();
    rotate_finalized_externals_at_ = td::Timestamp::in(CANDIDATE_EXTERNALS_TTL);
  }
  alarm_timestamp().relax(rotate_finalized_externals_at_);
  std::erase_if(callbacks_, [&](const std::unique_ptr<ExtMsgCallback> &callback) -> bool {
    if (callback->timeout && callback->timeout.is_in_past()) {
      return true;
    }
    alarm_timestamp().relax(callback->timeout);
    return false;
  });
}

void ExtMessagePool::add_message_to_mempool(td::Ref<ExtMessage> message, int priority,
                                            td::optional<td::uint32> msg_seqno) {
  WorkchainId wc = message->wc();
  StdSmcAddress addr = message->addr();
  auto &msgs = ext_msgs_[priority];
  if (msgs.ext_messages_.size() > opts_->max_mempool_num()) {
    LOG(INFO) << "cannot add message addr=" << wc << ":" << addr.to_hex() << " prio=" << priority
              << " to mempool: mempool is full (limit=" << opts_->max_mempool_num() << ")";
    return;
  }
  auto msg = std::make_shared<MempoolMsg>(message);
  msg->msg_seqno = msg_seqno;
  MessageId id{message->shard(), message->hash()};
  auto address = msg->address();
  auto it = msgs.ext_addr_messages_.find(address);
  if (it != msgs.ext_addr_messages_.end() && it->second.size() >= PER_ADDRESS_LIMIT) {
    LOG(INFO) << "cannot add message addr=" << wc << ":" << addr.to_hex() << " prio=" << priority
              << " to mempool: per address limit reached (limit=" << PER_ADDRESS_LIMIT << ")";
    return;
  }
  auto it2 = ext_messages_hashes_.find(id.hash);
  if (it2 != ext_messages_hashes_.end()) {
    int old_priority = it2->second.first;
    if (old_priority >= priority) {
      LOG(INFO) << "cannot add message addr=" << wc << ":" << addr.to_hex() << " prio=" << priority
                << " to mempool: already exists";
      return;
    }
    erase_message(old_priority, id);
  }
  auto hash_norm = msg->hash_norm;
  msgs.ext_messages_ = msgs.ext_messages_.insert(id, std::move(msg));
  msgs.ext_addr_messages_[address].emplace(id.hash, id);
  ext_messages_hashes_[id.hash] = {priority, id};
  ext_messages_hashes_norm_[hash_norm].insert(NormalizedMessageId{priority, id});
  LOG(INFO) << "adding message addr=" << wc << ":" << addr.to_hex() << " prio=" << priority << " to mempool";
  if (!is_held(id.hash)) {
    offer_to_callbacks(message, priority);
  }
}

td::actor::Task<ExtMessagePool::CheckResult> ExtMessagePool::check_message(td::Ref<ExtMessage> message,
                                                                           td::optional<td::uint32> &msg_seqno) {
  WorkchainId wc = message->wc();
  StdSmcAddress addr = message->addr();
  auto [shard_acc, utime, lt, config] = co_await run_fetch_account_state(wc, addr, manager_);
  bool special = wc == masterchainId && config->is_special_smartcontract(addr);
  block::Account acc;
  if (!acc.unpack(shard_acc, utime, special)) {
    co_return td::Status::Error(PSLICE() << "Failed to unpack account state");
  }
  acc.block_lt = lt;

  auto [wait_allow_broadcast, allow_broadcast_promise] = td::actor::StartedTask<>::make_bridge();
  CheckResult check_result{.message = message, .wait_allow_broadcast = std::move(wait_allow_broadcast)};

  const WalletMessageProcessor *wallet =
      acc.code.not_null() ? WalletMessageProcessor::get(acc.code->get_hash().bits()) : nullptr;
  if (wallet != nullptr) {
    msg_seqno = co_await check_message_to_wallet(message, wallet, std::move(acc), utime, lt, std::move(config),
                                                 std::move(allow_broadcast_promise));
    co_return check_result;
  }
  wallets_.erase({wc, addr});
  co_await ExtMessageQ::run_message_on_account(wc, &acc, utime, lt + 1, message->root_cell(), std::move(config));
  allow_broadcast_promise.set_value(td::Unit{});
  co_return check_result;
}

td::Result<td::uint32> ExtMessagePool::check_message_to_wallet(td::Ref<ExtMessage> message,
                                                               const WalletMessageProcessor *wallet, block::Account acc,
                                                               UnixTime utime, LogicalTime lt,
                                                               std::unique_ptr<block::ConfigInfo> config,
                                                               td::Promise<td::Unit> allow_broadcast_promise) {
  WorkchainId wc = message->wc();
  StdSmcAddress addr = message->addr();
  LOG(DEBUG) << "Checking external message to " << wc << ":" << addr.to_hex() << ", " << wallet->name();
  TRY_RESULT(wallet_seqno, wallet->get_wallet_seqno(acc.data));
  auto &wallet_info = wallets_[{wc, addr}];
  SCOPE_EXIT {
    if (wallet_info.messages.empty()) {
      wallets_.erase({wc, addr});
    }
  };
  wallet_info.process_messages(wallet_seqno, utime);
  TRY_RESULT(parsed_message, wallet->parse_message(message->root_cell()));
  auto [msg_seqno, msg_valid_until] = parsed_message;
  LOG(DEBUG) << "External message to " << wallet->name() << ": msg_seqno=" << msg_seqno
             << ", msg_ttl=" << msg_valid_until << ", wallet_seqno=" << wallet_seqno;
  if (msg_valid_until <= (UnixTime)td::Clocks::system()) {
    return td::Status::Error("valid_until is in the past");
  }
  if (msg_seqno < wallet_seqno) {
    return td::Status::Error(PSTRING() << "Too old seqno: msg_seqno=" << msg_seqno
                                       << ", wallet_seqno=" << wallet_seqno);
  }
  if (msg_seqno - wallet_seqno > MAX_WALLET_SEQNO_DIFF) {
    return td::Status::Error(PSTRING() << "Too new seqno: msg_seqno=" << msg_seqno
                                       << ", wallet_seqno=" << wallet_seqno);
  }
  if (wallet_info.messages.contains(msg_seqno)) {
    return td::Status::Error(PSTRING() << "Duplicate msg_seqno " << msg_seqno);
  }
  TRY_RESULT_ASSIGN(acc.data, wallet->set_wallet_seqno(acc.data, msg_seqno));
  acc.storage_dict_hash = acc.orig_storage_dict_hash = {};
  TRY_STATUS(ExtMessageQ::run_message_on_account(wc, &acc, utime, lt + 1, message->root_cell(), std::move(config)));
  wallet_info.messages[msg_seqno] =
      WalletMessageInfo{.valid_until = msg_valid_until, .allow_broadcast_promise = std::move(allow_broadcast_promise)};
  wallet_info.process_messages(wallet_seqno, utime);
  LOG(DEBUG) << "Checked external message to " << wc << ":" << addr.to_hex() << ", " << wallet->name();
  return msg_seqno;
}

void ExtMessagePool::WalletInfo::process_messages(td::uint32 wallet_seqno, UnixTime utime) {
  for (auto it = messages.begin(); it != messages.end();) {
    auto &[seqno, message] = *it;
    if (seqno < wallet_seqno) {
      if (message.allow_broadcast_promise) {
        message.allow_broadcast_promise.set_error(
            td::Status::Error(PSTRING() << "Too old seqno: msg_seqno=" << seqno << ", wallet_seqno=" << wallet_seqno));
      }
      it = messages.erase(it);
      continue;
    }
    if (message.valid_until <= utime) {
      if (message.allow_broadcast_promise) {
        message.allow_broadcast_promise.set_error(td::Status::Error("valid_until is in the past"));
      }
      it = messages.erase(it);
      continue;
    }
    ++it;
  }
  for (td::uint32 seqno = wallet_seqno;; ++seqno) {
    auto it = messages.find(seqno);
    if (it == messages.end()) {
      break;
    }
    if (it->second.allow_broadcast_promise) {
      it->second.allow_broadcast_promise.set_value(td::Unit{});
    }
  }
}

size_t ExtMessagePool::CheckedExtMsgCounter::get_msg_count(WorkchainId wc, StdSmcAddress addr) {
  before_query();
  auto it1 = counter_cur_.find({wc, addr});
  auto it2 = counter_prev_.find({wc, addr});
  return (it1 == counter_cur_.end() ? 0 : it1->second) + (it2 == counter_prev_.end() ? 0 : it2->second);
}

size_t ExtMessagePool::CheckedExtMsgCounter::inc_msg_count(WorkchainId wc, StdSmcAddress addr) {
  before_query();
  auto it2 = counter_prev_.find({wc, addr});
  return (it2 == counter_prev_.end() ? 0 : it2->second) + ++counter_cur_[{wc, addr}];
}

void ExtMessagePool::CheckedExtMsgCounter::before_query() {
  while (cleanup_at_.is_in_past()) {
    counter_prev_ = std::move(counter_cur_);
    counter_cur_.clear();
    if (counter_prev_.empty()) {
      cleanup_at_ = td::Timestamp::in(MAX_EXT_MSG_PER_ADDR_TIME_WINDOW / 2.0);
      break;
    }
    cleanup_at_ += MAX_EXT_MSG_PER_ADDR_TIME_WINDOW / 2.0;
  }
}

}  // namespace ton::validator
