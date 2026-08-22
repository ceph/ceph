"""
Show PG Rebuild Stats for each PG
"""

import json
from datetime import datetime

from .cli import RebuildStatsCLICommand

from mgr_module import MgrModule


class RebuildStats(MgrModule):
    CLICommand = RebuildStatsCLICommand
    COMMANDS = [
        {
            "cmd": "rebuild-stats ",
            "desc": "Report of active/past PG rebuild times",
            "perm": "r"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(RebuildStats, self).__init__(*args, **kwargs)
        # Caches the historical record of PG rebuild times (upto 10 per PG)
        self.completed_history = {}
        # Tracks clean timestamps and incident flags prior to failures.
        self.healthy_baselines = {}
        # Latches the exact time when an active failure occurs
        self.active_recovery_starts = {}
        # Captures current cumulative objects recovered and use it as a baseline
        self.recovery_base_counters = {}

    def _parse_ceph_timestamp(self, ts_val):
        """
        Converts variable string and number timestamp payloads returned by Ceph
        subsystems into native Python datetime structures while preserving the
        precision and timezone information.

        Parameters:
          - ts_val (str/float/int): Raw chronological metric streamed from
            OSD map dumps.

        Returns:
          - datetime: Valid datetime object if parsing succeeded.
          - None: If the input timestamp represents an uninitialized or
            zero-state value ("0.000000").
        """
        if not ts_val or ts_val == "0.000000" or ts_val == 0:
            return None
        try:
            ts_str = str(ts_val).strip().rstrip('Z')
            if '+' in ts_str:
                ts_base, offset = ts_str.split('+', 1)
                if len(offset) == 4 and ':' not in offset:
                    offset = f"{offset[:2]}:{offset[2:]}"
                ts_str = f"{ts_base}+{offset}"
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None

    def _process_live_stats(self):
        """
        Scans the active cluster-wide PG map in memory to statefully capture
        transitions. This function handles baseline capture, latching the
        failure timestamps, and recovery counters verification to safely
        calculate rebuild times.

        Workflow:
          1. Evaluates live redundancy loss by scanning PG `state` transitions.
          2. On failure: Freezes the healthy `last_clean` baseline and logs the
             initial counter state.
          3. On recovery completion: Pops the latched timestamps, executes a
             calculation against cumulative rolling recovery metrics, and logs
             verified entries to history.
          4. De-duplicates records within a rolling 2-second timestamp delta.
        """
        pg_stats = self.get("pg_stats")
        for pg in pg_stats.get('pg_stats', []):
            pgid = pg['pgid']
            state_str = pg.get('state', "")

            stat_sum_dict = pg.get('stat_sum', {})
            misplaced = stat_sum_dict.get('num_objects_misplaced', 0)
            degraded = stat_sum_dict.get('num_objects_degraded', 0)
            cumulative_recovered = stat_sum_dict.get('num_objects_recovered', 0)

            is_vulnerable = "degraded" in state_str or "undersized" in state_str or misplaced > 0 or degraded > 0
            clean_str = str(pg.get('last_clean', ""))

            # The PG is undergoing a recovery event
            if is_vulnerable:
                raw_enter_time = str(pg.get('last_change', ""))

                # If we are not already tracking this PG, create an entry.
                if pgid not in self.active_recovery_starts and raw_enter_time:
                    dt_enter = self._parse_ceph_timestamp(raw_enter_time)
                    # Record & latch `last_clean` if this is a new entry
                    baseline_clean = self.healthy_baselines.get(pgid, clean_str)
                    dt_baseline = self._parse_ceph_timestamp(baseline_clean)

                    should_latch = False
                    if dt_enter and dt_baseline and dt_enter > dt_baseline:
                        should_latch = True
                    elif not dt_baseline:
                        should_latch = True

                    if should_latch:
                        self.active_recovery_starts[pgid] = raw_enter_time
                        self.recovery_base_counters[pgid] = cumulative_recovered

                        # Track if this a genuine recovery event with non-zero
                        # degraded or misplaced objects.
                        had_redundancy_loss = (degraded > 0 or misplaced > 0)
                        self.healthy_baselines[pgid] = (clean_str, had_redundancy_loss)

                        self.log.info(f"[rebuild-stats] Latched failure start for PG {pgid} at {raw_enter_time}.")

                # Initialize healthy baseline state tracking if empty
                if pgid not in self.healthy_baselines and clean_str and not clean_str.startswith("0.000000"):
                    self.healthy_baselines[pgid] = (clean_str, False)
            else:
                # PG is healthy. Process history if an active latch exists
                if pgid in self.active_recovery_starts:
                    event_start = self.active_recovery_starts.pop(pgid)
                    base_recovered = self.recovery_base_counters.pop(pgid, 0)

                    # Unpack baseline clean tuple data safely
                    baseline_clean = None
                    had_redundancy_loss = False
                    if pgid in self.healthy_baselines:
                        baseline_clean, had_redundancy_loss = self.healthy_baselines.pop(pgid)

                    dt_start = self._parse_ceph_timestamp(event_start)
                    dt_end = self._parse_ceph_timestamp(clean_str)

                    if dt_start and dt_end:
                        is_valid_recovery = True
                        if baseline_clean:
                            dt_baseline = self._parse_ceph_timestamp(baseline_clean)
                            if dt_baseline and dt_end <= dt_baseline:
                                is_valid_recovery = False

                        if is_valid_recovery and dt_end > dt_start:
                            duration_seconds = int((dt_end - dt_start).total_seconds())
                            delta_recovered = cumulative_recovered - base_recovered

                            # A record is valid if duration > 0 AND (it repaired objects OR it had
                            # misplaced/degraded objects when it failed)
                            if duration_seconds > 0 and (delta_recovered > 0 or had_redundancy_loss):
                                # Don't consider the PG if the time drift
                                # between the start and end times is less than
                                # a threshold of 2 secs. This could happen when
                                # multiple notifications are received resulting
                                # in `last_clean` shifting by fractions of
                                # seconds across the notifications.
                                if pgid in self.completed_history:
                                    is_duplicate = False
                                    for r in self.completed_history[pgid]:
                                        existing_end = self._parse_ceph_timestamp(r["end"])
                                        existing_start = self._parse_ceph_timestamp(r["start"])

                                        if existing_end and existing_start:
                                            end_drift = abs((dt_end - existing_end).total_seconds())
                                            start_drift = abs((dt_start - existing_start).total_seconds())
                                            if end_drift <= 2.0 or start_drift <= 2.0:
                                                is_duplicate = True
                                                break
                                    if is_duplicate:
                                        continue

                                record = {
                                    "start": event_start,
                                    "end": clean_str,
                                    "duration": duration_seconds
                                }

                                if pgid not in self.completed_history:
                                    self.completed_history[pgid] = []
                                self.completed_history[pgid].append(record)
                                if len(self.completed_history[pgid]) > 10:
                                    self.completed_history[pgid].pop(0)

                # Maintain continuous updates while healthy
                if clean_str and not clean_str.startswith("0.000000"):
                    self.healthy_baselines[pgid] = (clean_str, False)

    def handle_command(self, _, cmd):
        """
        Handles the user executed command routed from the Ceph CLI engine.
        Forces an immediate processing of the stats and formatting rebuild data
        and historical timelines into a JSON payload.

        Parameters:
          - inbuf (str): Input buffer (unused for standard read command routing).
          - cmd (dict): Structured array containing the CLI prefix validation tokens.

        Returns:
          - tuple (int, str, str): Returns standard Ceph return code (0 for success),
            the formatted JSON report body string, and any applicable execution errors.
        """
        if cmd['prefix'] != 'rebuild-stats':
            raise NotImplementedError(cmd['prefix'])

        self._process_live_stats()

        total_records = sum(len(v) for v in self.completed_history.values())
        report = {
            "summary": {
                "total_historical_records_captured": total_records
            },
            "active_rebuilds": [],
            "historical_rebuilds": []
        }

        pg_stats = self.get("pg_stats")
        for pg in pg_stats.get('pg_stats', []):
            pgid = pg['pgid']
            state_str = pg.get('state', "")

            stat_sum_dict = pg.get('stat_sum', {})
            misplaced = stat_sum_dict.get('num_objects_misplaced', 0)
            degraded = stat_sum_dict.get('num_objects_degraded', 0)

            if "degraded" in state_str or "undersized" in state_str or misplaced > 0 or degraded > 0:
                latched_start = self.active_recovery_starts.get(pgid, str(pg.get('last_change', "")))
                report["active_rebuilds"].append({
                    "pgid": pgid,
                    "rebuild_start": latched_start,
                    "objects_degraded": degraded,
                    "objects_misplaced": misplaced
                })

        for pgid, records in self.completed_history.items():
            for rec in records:
                report["historical_rebuilds"].append({
                    "pgid": pgid,
                    "rebuild_start": rec["start"],
                    "rebuild_end": rec["end"],
                    "duration_seconds": rec["duration"]
                })

        return 0, json.dumps(report, indent=2), ""

    def notify(self, notify_type, notify_id):
        """
        Pure push-based callback interface executed by the core C++ engine.
        Listens exclusively for pg_stats notification events and streams the
        data payload directly into a handler to capture dynamic transitions.

        Parameters:
        - notify_type (str): The subsystem type sending the event broadcast.
        - notify_id (str): Specific entity ID reference (Unused).
        """
        if notify_type != "pg_stats":
            return
        self._process_live_stats()

