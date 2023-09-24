#pragma once

#include "mds/MDSContext.h"
#include "mds/MDSRank.h"
#include <functional>

using JournalLogSegmentExpirationHook = std::function<void(LogSegment&, MDSRankBase*, MDSGatherBuilder&, int)>;
extern JournalLogSegmentExpirationHook journal_log_segment_expiration_hook;
