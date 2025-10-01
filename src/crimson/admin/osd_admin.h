// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
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
class RunOSDBenchHook;
class DumpInFlightOpsHook;
class DumpHistoricOpsHook;
class DumpSlowestHistoricOpsHook;
class DumpRecoveryReservationsHook;

template<class Hook, class... Args>
std::unique_ptr<AdminSocketHook> make_asok_hook(Args&&... args);

}  // namespace crimson::admin
