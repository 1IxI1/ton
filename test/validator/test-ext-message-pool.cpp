/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#include <set>

#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"

#include "impl/ext-message-pool.hpp"

namespace ton::validator {
namespace {

using Hash = ExtMessage::Hash;

// Stub ExtMessage for testing. Carries a synthetic hash and shard prefix; other methods are unused.
class StubExtMessage : public ExtMessage {
 public:
  StubExtMessage(AccountIdPrefixFull prefix, Hash hash) : prefix_(prefix), hash_(hash) {
  }

  AccountIdPrefixFull shard() const override {
    return prefix_;
  }
  td::BufferSlice serialize() const override {
    return {};
  }
  td::Ref<vm::Cell> root_cell() const override {
    return {};
  }
  Hash hash() const override {
    return hash_;
  }
  Hash hash_norm() const override {
    return hash_;
  }
  ton::WorkchainId wc() const override {
    return prefix_.workchain;
  }
  ton::StdSmcAddress addr() const override {
    return {};
  }

 private:
  AccountIdPrefixFull prefix_;
  Hash hash_;
};

td::Ref<ExtMessage> make_msg(td::uint8 id) {
  Hash h = Hash::zero();
  h.as_array()[0] = id;
  return td::make_ref<StubExtMessage>(AccountIdPrefixFull{0, 0x4000000000000000ULL}, h);
}

td::Bits256 candidate_id(td::uint8 id) {
  td::Bits256 h = td::Bits256::zero();
  h.as_array()[31] = id;
  return h;
}

// Drains a sync-only collator queue snapshot from the pool and returns the ids of the messages
// the pool considers available.
td::actor::Task<std::multiset<td::uint8>> available_messages(td::actor::TestScheduler& ts,
                                                             td::actor::ActorId<ExtMessagePool> pool) {
  auto callback = std::make_unique<ExtMsgCallback>();
  callback->shard = ShardIdFull{0};
  callback->queue = ExtMsgQueue("test_queue", 100);
  callback->sync_only = true;
  auto queue = callback->queue;
  td::actor::send_closure(pool, &ExtMessagePool::install_collator_queue, ShardIdFull{0}, std::move(callback));
  co_await ts.wait_sync_work();

  std::multiset<td::uint8> ids;
  while (true) {
    auto maybe = co_await queue.pop().wrap();
    if (maybe.is_error()) {
      break;
    }
    ids.insert(maybe.ok().first->hash().as_array()[0]);
  }
  co_return ids;
}

TEST(ExtMessagePool, SimplexHoldReaddAndFinalize) {
  td::actor::TestScheduler ts;
  ts.run([&]() -> td::actor::Task<td::Unit> {
    auto opts = ValidatorManagerOptions::create(BlockIdExt{}, BlockIdExt{});
    auto pool = td::actor::create_actor<ExtMessagePool>("extmessages", opts, td::actor::ActorId<ValidatorManager>{});

    auto m1 = make_msg(1);
    auto m2 = make_msg(2);

    // Candidate A includes m1 and m2 (e.g. a peer's candidate; the messages were never admitted
    // into our mempool). Nothing is available while A is in flight.
    td::actor::send_closure(pool, &ExtMessagePool::candidate_externals_seen, candidate_id(0xA),
                            std::vector<td::Ref<ExtMessage>>{m1, m2});
    co_await ts.wait_sync_work();
    auto ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(0u, ids.size());

    // A's history is rejected: both messages must be re-added to the available set.
    td::actor::send_closure(pool, &ExtMessagePool::history_collapsed, std::vector<td::Bits256>{},
                            std::vector<td::Bits256>{candidate_id(0xA)});
    co_await ts.wait_sync_work();
    ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(2u, ids.size());
    EXPECT_EQ(1u, ids.count(1));
    EXPECT_EQ(1u, ids.count(2));

    // Candidate B includes m1: it is held (not offered to collations), m2 stays available.
    td::actor::send_closure(pool, &ExtMessagePool::candidate_externals_seen, candidate_id(0xB),
                            std::vector<td::Ref<ExtMessage>>{m1});
    co_await ts.wait_sync_work();
    ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(1u, ids.size());
    EXPECT_EQ(1u, ids.count(2));

    // B is finalized: m1 is gone for good.
    td::actor::send_closure(pool, &ExtMessagePool::history_collapsed,
                            std::vector<td::Bits256>{candidate_id(0xB)}, std::vector<td::Bits256>{});
    co_await ts.wait_sync_work();
    ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(1u, ids.size());
    EXPECT_EQ(1u, ids.count(2));

    // Candidate C (a competing fork) also included m1 and is rejected: m1 must NOT be re-added
    // because it is already part of the finalized chain.
    td::actor::send_closure(pool, &ExtMessagePool::candidate_externals_seen, candidate_id(0xC),
                            std::vector<td::Ref<ExtMessage>>{m1});
    co_await ts.wait_sync_work();
    td::actor::send_closure(pool, &ExtMessagePool::history_collapsed, std::vector<td::Bits256>{},
                            std::vector<td::Bits256>{candidate_id(0xC)});
    co_await ts.wait_sync_work();
    ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(1u, ids.size());
    EXPECT_EQ(1u, ids.count(2));

    pool.reset();
    co_await ts.wait_sync_work();
    co_return {};
  });
}

TEST(ExtMessagePool, HeldByMultipleCandidates) {
  td::actor::TestScheduler ts;
  ts.run([&]() -> td::actor::Task<td::Unit> {
    auto opts = ValidatorManagerOptions::create(BlockIdExt{}, BlockIdExt{});
    auto pool = td::actor::create_actor<ExtMessagePool>("extmessages", opts, td::actor::ActorId<ValidatorManager>{});

    auto m1 = make_msg(1);

    // Two competing candidates include the same message.
    td::actor::send_closure(pool, &ExtMessagePool::candidate_externals_seen, candidate_id(0xA),
                            std::vector<td::Ref<ExtMessage>>{m1});
    td::actor::send_closure(pool, &ExtMessagePool::candidate_externals_seen, candidate_id(0xB),
                            std::vector<td::Ref<ExtMessage>>{m1});
    co_await ts.wait_sync_work();

    // A is rejected, but B still holds m1: not available yet.
    td::actor::send_closure(pool, &ExtMessagePool::history_collapsed, std::vector<td::Bits256>{},
                            std::vector<td::Bits256>{candidate_id(0xA)});
    co_await ts.wait_sync_work();
    auto ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(0u, ids.size());

    // B is rejected as well: now m1 comes back.
    td::actor::send_closure(pool, &ExtMessagePool::history_collapsed, std::vector<td::Bits256>{},
                            std::vector<td::Bits256>{candidate_id(0xB)});
    co_await ts.wait_sync_work();
    ids = co_await available_messages(ts, pool.get());
    EXPECT_EQ(1u, ids.size());
    EXPECT_EQ(1u, ids.count(1));

    pool.reset();
    co_await ts.wait_sync_work();
    co_return {};
  });
}

}  // namespace
}  // namespace ton::validator
