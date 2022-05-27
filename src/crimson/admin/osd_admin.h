// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <memory>

namespace crimson::admin {

class AdminSocketHook;

class AssertAlwaysHook;
class DumpMetricsHook;
class DumpPGStateHistory;
class DumpPerfCountersHook;
class FlushPgStatsHook;
class InjectDataErrorHook;
class InjectMDataErrorHook;
class OsdStatusHook;
class SendBeaconHook;
class DumpInFlightOpsHook;
class DumpHistoricOpsHook;
class DumpSlowestHistoricOpsHook;

template<class Hook, class... Args>
std::unique_ptr<AdminSocketHook> make_asok_hook(Args&&... args);

}  // namespace crimson::admin
