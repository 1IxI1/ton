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
#pragma once

#include <deque>
#include <set>

#include "interfaces/validator-manager.h"
#include "td/actor/coro_utils.h"
#include "td/utils/PersistentTreap.h"

#include "external-message.hpp"
#include "ext-message-checker.hpp"

namespace ton::validator {

class ExtMessagePool : public td::actor::Actor {
 public:
  ExtMessagePool(td::Ref<ValidatorManagerOptions> opts, td::actor::ActorId<ValidatorManager> manager)
      : opts_(opts), manager_(manager) {
  }

  struct CheckResult {
    td::Ref<ExtMessage> message;
    td::actor::StartedTask<> wait_allow_broadcast;
  };
  td::actor::Task<CheckResult> check_add_external_message(td::BufferSlice data, int priority, bool add_to_mempool);
  void install_collator_queue(ShardIdFull shard, std::unique_ptr<ExtMsgCallback> callback);
  void cleanup_external_messages(ShardIdFull shard);
  void complete_external_messages(std::vector<ExtMessage::Hash> to_delay, std::vector<ExtMessage::Hash> to_delete);
  void erase_external_messages(std::vector<ExtMessage::Hash> to_delete);

  // Simplex awareness: a candidate (ours or a peer's) includes these externals. Hold them so
  // that the next collation does not double-include them, and remember them by candidate so they
  // can be returned to the mempool if the candidate's history is later rejected.
  void candidate_externals_seen(td::Bits256 candidate_id, std::vector<td::Ref<ExtMessage>> messages);
  // A final certificate collapsed histories: externals of `finalized` candidates are dropped for
  // good, externals of `rejected` candidates are re-added to the available set (unless they are
  // also part of the finalized chain).
  void history_collapsed(std::vector<td::Bits256> finalized, std::vector<td::Bits256> rejected);

  void update_last_masterchain_state(td::Ref<MasterchainState> state) {
    last_masterchain_state_ = std::move(state);
  }
  void update_options(td::Ref<ValidatorManagerOptions> opts) {
    opts_ = std::move(opts);
  }
  std::vector<std::pair<std::string, std::string>> prepare_stats();

  void alarm() override;
  void start_up() override {
    alarm_timestamp().relax(admission_stats_at_);
  }

 private:
  struct MessageId {
    AccountIdPrefixFull dst;
    ExtMessage::Hash hash;

    bool operator<(const MessageId &msg) const {
      if (dst < msg.dst) {
        return true;
      }
      if (msg.dst < dst) {
        return false;
      }
      return hash < msg.hash;
    }
    bool operator==(const MessageId &msg) const {
      return !(*this < msg) && !(msg < *this);
    }
  };
  struct MempoolMsg {
    td::Ref<ExtMessage> message;
    ExtMessage::Hash hash_norm;
    td::uint32 generation = 0;
    bool active = true;
    td::Timestamp reactivate_at;
    td::Timestamp delete_at;
    td::optional<td::uint32> msg_seqno;

    auto address() const {
      return std::make_pair(message->wc(), message->addr());
    }
    bool is_active() {
      if (!active) {
        if (reactivate_at.is_in_past()) {
          active = true;
          generation++;
        }
      }
      return active;
    }
    bool can_postpone() const {
      return generation <= 2;
    }
    void postpone() {
      if (!active) {
        return;
      }
      active = false;
      reactivate_at = td::Timestamp::in(generation * 5.0);
    }
    bool expired() const {
      return delete_at.is_in_past();
    }
    explicit MempoolMsg(td::Ref<ExtMessage> msg) : message(std::move(msg)), hash_norm(message->hash_norm()) {
      delete_at = td::Timestamp::in(600);
    }
  };

  td::Ref<ValidatorManagerOptions> opts_;
  td::actor::ActorId<ValidatorManager> manager_;
  td::Ref<MasterchainState> last_masterchain_state_;

  struct ExtMessages {
    td::PersistentTreap<MessageId, std::shared_ptr<MempoolMsg>> ext_messages_;
    std::map<std::pair<WorkchainId, StdSmcAddress>, std::map<ExtMessage::Hash, MessageId>> ext_addr_messages_;
  };
  struct NormalizedMessageId {
    int priority;
    MessageId id;

    bool operator<(const NormalizedMessageId &msg) const {
      if (priority != msg.priority) {
        return priority < msg.priority;
      }
      return id < msg.id;
    }
  };
  std::map<int, ExtMessages> ext_msgs_;                                        // priority -> messages
  std::map<ExtMessage::Hash, std::pair<int, MessageId>> ext_messages_hashes_;  // raw hash -> priority
  std::map<ExtMessage::Hash, std::set<NormalizedMessageId>> ext_messages_hashes_norm_;

  struct CheckedExtMsgCounter {
    std::map<std::pair<WorkchainId, StdSmcAddress>, size_t> counter_cur_, counter_prev_;
    td::Timestamp cleanup_at_ = td::Timestamp::now();

    size_t get_msg_count(WorkchainId wc, StdSmcAddress addr);
    size_t inc_msg_count(WorkchainId wc, StdSmcAddress addr);
    void before_query();
  } checked_ext_msg_counter_;
  td::uint64 total_check_ext_messages_ok_{0}, total_check_ext_messages_error_{0};
  td::uint64 applied_ext_msgs_delete_requests_{0}, applied_ext_msgs_deleted_{0};

  td::Timestamp cleanup_mempool_at_ = td::Timestamp::now();

  // ===== Simplex awareness =====
  struct CandidateExternals {
    std::vector<td::Ref<ExtMessage>> messages;
    td::Timestamp expires_at;
  };
  std::map<td::Bits256, CandidateExternals> candidate_externals_;  // candidate id hash -> externals
  std::map<ExtMessage::Hash, size_t> held_externals_;              // raw hash -> #in-flight candidates with it
  // Raw and normalized hashes of externals recently seen in finalized candidates; two-generation
  // rotation with a window covering CANDIDATE_EXTERNALS_TTL.
  std::set<ExtMessage::Hash> finalized_externals_cur_, finalized_externals_prev_;
  td::Timestamp rotate_finalized_externals_at_ = td::Timestamp::in(CANDIDATE_EXTERNALS_TTL);

  td::uint64 total_candidates_seen_{0}, total_candidates_finalized_{0}, total_candidates_rejected_{0};
  td::uint64 total_ext_msgs_held_{0}, total_ext_msgs_finalized_{0}, total_ext_msgs_readded_{0};

  bool is_held(const ExtMessage::Hash &hash) const {
    return held_externals_.contains(hash);
  }
  bool was_recently_finalized(const ExtMessage::Hash &hash) const {
    return finalized_externals_cur_.contains(hash) || finalized_externals_prev_.contains(hash);
  }
  void remember_finalized(const ExtMessage::Hash &hash) {
    finalized_externals_cur_.insert(hash);
  }
  void unhold(const ExtMessage::Hash &hash);
  void drop_candidate_externals(std::map<td::Bits256, CandidateExternals>::iterator it);
  // Returns true if the message is available in the mempool afterwards.
  bool readd_message(const td::Ref<ExtMessage> &message);
  void offer_to_callbacks(const td::Ref<ExtMessage> &message, int priority);

  using Snapshot = std::vector<std::pair<int, td::PersistentTreap<MessageId, std::shared_ptr<MempoolMsg>>>>;
  td::actor::Task<> push_existing_to_queue(ExtMsgQueue queue, td::CancellationToken token, ShardIdFull shard,
                                           Snapshot snapshot, bool sync_only);

  void add_message_to_mempool(td::Ref<ExtMessage> message, int priority, td::optional<td::uint32> msg_seqno);
  bool erase_message(int priority, const MessageId &id);

  struct WalletMessageInfo {
    td::uint32 valid_until;
    td::Promise<td::Unit> allow_broadcast_promise;
  };
  struct WalletInfo {
    std::map<td::uint32, WalletMessageInfo> messages;
    ~WalletInfo() {
      for (auto &[_, message] : messages) {
        if (message.allow_broadcast_promise) {
          message.allow_broadcast_promise.set_error(td::Status::Error("wallet is no longer valid"));
        }
      }
    }
    void process_messages(td::uint32 wallet_seqno, UnixTime utime);
  };
  std::map<std::pair<WorkchainId, StdSmcAddress>, WalletInfo> wallets_;

  // ===== Parallel admission =====
  // The expensive per-message stages (parse, account state fetch, VM check) run on these worker
  // actors; the pool only dispatches and finalizes. Created lazily on the first check.
  std::vector<td::actor::ActorOwn<ExtMessageChecker>> checkers_;
  std::vector<size_t> checker_inflight_;
  size_t next_checker_{0};
  void init_checkers();
  // Admission backpressure: only MAX_INFLIGHT_CHECKS checks run concurrently; the rest wait in
  // FIFO order (bounded — beyond that requests fail fast instead of queueing into a congestion
  // collapse that would starve the whole node).
  size_t inflight_checks_{0};
  std::deque<td::actor::StartedTask<>::ExternalPromise> admission_waiters_;
  void release_check_slot();
  // Atomic (non-suspending) pool-side completion of a checked wallet message: prune/dedup the
  // wallet seqno window and register the allow-broadcast promise.
  td::Result<td::actor::StartedTask<>> finalize_wallet_check(const td::Ref<ExtMessage> &message,
                                                             const ExtMessageChecker::CheckedExtMsg &checked);

  // Rolling window for the periodic "ext admission" INFO stat.
  struct AdmissionWindowStats {
    td::uint64 in{0}, admitted{0}, rejected{0}, checked{0};
    double check_time{0};
    ExtMessageChecker::StageTimings timings;
    td::Timestamp window_start = td::Timestamp::now();
  };
  AdmissionWindowStats admission_window_;
  td::Timestamp admission_stats_at_ = td::Timestamp::in(ADMISSION_STATS_PERIOD);
  void log_admission_stats();

  std::vector<std::unique_ptr<ExtMsgCallback>> callbacks_;

  static constexpr double CANDIDATE_EXTERNALS_TTL = 60.0;
  static constexpr size_t MAX_TRACKED_CANDIDATES = 256;
  static constexpr double MAX_EXT_MSG_PER_ADDR_TIME_WINDOW = 10.0;
  static constexpr size_t MAX_EXT_MSG_PER_ADDR = 3 * 10;
  static constexpr size_t PER_ADDRESS_LIMIT = 256;
  static constexpr size_t SOFT_MEMPOOL_LIMIT = 1024;
  static constexpr size_t NUM_CHECKERS = 24;
  static constexpr size_t MAX_INFLIGHT_CHECKS = 8 * NUM_CHECKERS;
  // Sized so that worst-case queueing latency stays well under client/liteserver timeouts
  // (~10s): beyond this the requests would time out anyway, so fail them fast instead.
  static constexpr size_t MAX_ADMISSION_WAITERS = 30000;
  static constexpr double ADMISSION_STATS_PERIOD = 5.0;
};

}  // namespace ton::validator
