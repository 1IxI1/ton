/*
 * Copyright (c) 2025-2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#pragma once

#include "interfaces/block.h"
#include "interfaces/external-message.h"
#include "td/actor/common.h"
#include "td/actor/coro_task.h"
#include "td/utils/Status.h"
#include "ton/ton-types.h"

#include "types.h"

namespace ton::validator::consensus {

td::Result<double> get_candidate_gen_utime_exact(const BlockCandidate& candidate);

// Recovers the external messages accepted into a (non-empty) candidate: either from the locally
// collected `accepted_ext_messages` (cheap, available for own candidates) or by parsing the
// block's InMsgDescr (peer candidates).
td::Result<std::vector<td::Ref<ExtMessage>>> extract_accepted_externals(const CandidateRef& candidate);

}  // namespace ton::validator::consensus
