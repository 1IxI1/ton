/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include "consensus/utils.h"

#include "bus.h"

namespace ton::validator::consensus::simplex {

namespace {

// Tracks which external messages are referenced by in-flight candidates and tells the mempool
// when a final certificate collapses histories: externals of candidates on the finalized chain
// are gone for good, externals of competing (rejected) candidates must be returned to the
// mempool so that they are not lost (see ManagerFacade::ext_messages_* hooks).
class ExternalsTrackerImpl : public td::actor::SpawnsWith<Bus>, public td::actor::ConnectsTo<Bus> {
 public:
  TON_RUNTIME_DEFINE_EVENT_HANDLER();

  static bool should_be_spawned(const Bus& bus) {
    return bus.is_validator();
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const StopRequested>) {
    stop();
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const CandidateReceived> event) {
    const CandidateRef& candidate = event->candidate;
    if (last_collapsed_slot_.has_value() && candidate->id.slot <= *last_collapsed_slot_) {
      // This slot's history has already been collapsed; a late candidate here is irrelevant.
      return;
    }
    auto [it, inserted] = candidates_.try_emplace(candidate->id, Record{candidate->parent_id});
    if (!inserted) {
      return;
    }
    if (candidates_.size() > MAX_TRACKED_CANDIDATES) {
      // Bound memory if finalization stalls for a long time; the mempool unholds dropped
      // candidates' externals by TTL on its own.
      candidates_.erase(candidates_.begin());
    }
    if (candidate->is_empty()) {
      return;
    }
    auto externals = extract_accepted_externals(candidate);
    if (externals.is_error()) {
      LOG(WARNING) << "Cannot extract externals from candidate " << candidate->id << " : " << externals.error();
      return;
    }
    if (externals.ok().empty()) {
      return;
    }
    LOG(INFO) << "ext pool: holding " << externals.ok().size() << " externals seen in candidate " << candidate->id;
    td::actor::send_closure(owning_bus()->manager, &ManagerFacade::ext_messages_seen_in_candidate, candidate->id.hash,
                            externals.move_as_ok());
  }

  template <>
  void handle(BusHandle, std::shared_ptr<const FinalizationObserved> event) {
    // Collect the finalized chain: the certified candidate and all its ancestors we know about.
    std::set<CandidateId> finalized_chain;
    std::vector<td::Bits256> finalized;
    ParentId cur = event->id;
    while (cur.has_value() && finalized_chain.insert(*cur).second) {
      finalized.push_back(cur->hash);
      auto it = candidates_.find(*cur);
      if (it == candidates_.end()) {
        break;
      }
      cur = it->second.parent;
    }

    // Everything else at slots <= the finalized slot lost the race.
    std::vector<td::Bits256> rejected;
    for (auto it = candidates_.begin(); it != candidates_.end() && it->first.slot <= event->id.slot;) {
      if (!finalized_chain.contains(it->first)) {
        rejected.push_back(it->first.hash);
      }
      it = candidates_.erase(it);
    }

    last_collapsed_slot_ = std::max(last_collapsed_slot_.value_or(0), event->id.slot);
    td::actor::send_closure(owning_bus()->manager, &ManagerFacade::ext_messages_history_collapsed, std::move(finalized),
                            std::move(rejected));
  }

 private:
  static constexpr size_t MAX_TRACKED_CANDIDATES = 4096;

  struct Record {
    ParentId parent;
  };

  // Ordered by (slot, hash): erasing all slots <= finalized slot is a prefix erase.
  std::map<CandidateId, Record> candidates_;
  std::optional<td::uint32> last_collapsed_slot_;
};

}  // namespace

void ExternalsTracker::register_in(td::actor::Runtime& runtime) {
  runtime.register_actor<ExternalsTrackerImpl>("ExternalsTracker");
}

}  // namespace ton::validator::consensus::simplex
