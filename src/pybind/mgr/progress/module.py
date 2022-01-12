try:
    from typing import List, Dict, Union, Any, Optional
    from typing import TYPE_CHECKING
except ImportError:
    TYPE_CHECKING = False

from mgr_module import MgrModule, OSDMap, Option
from mgr_util import to_pretty_timedelta
from datetime import timedelta
import os
import threading
import datetime
import uuid
import time
import logging
import json


ENCODING_VERSION = 2

# keep a global reference to the module so we can use it from Event methods
_module = None  # type: Optional["Module"]


class Event(object):
    """
    A generic "event" that has a start time, completion percentage,
    and a list of "refs" that are (type, id) tuples describing which
    objects (osds, pools) this relates to.
    """
    def __init__(self, id: str,
                 message: str,
                 refs: List[str],
                 add_to_ceph_s: bool,
                 started_at: Optional[float] = None):
        self._message = message
        self._refs = refs
        self.started_at = started_at if started_at else time.time()
        self.id = id
        self._add_to_ceph_s = add_to_ceph_s

    def _refresh(self):
        global _module
        assert _module
        _module.log.debug('refreshing mgr for %s (%s) at %f' % (self.id, self._message,
                                                                self.progress))
        _module.update_progress_event(
            self.id, self.twoline_progress(6), self.progress, self._add_to_ceph_s)

    @property
    def message(self):
        # type: () -> str
        return self._message

    @property
    def refs(self):
        # type: () -> List[str]
        return self._refs

    @property
    def add_to_ceph_s(self):
        # type: () -> bool
        return self._add_to_ceph_s

    @property
    def progress(self):
        # type: () -> float
        raise NotImplementedError()

    @property
    def duration_str(self):
        duration = time.time() - self.started_at
        return "(%s)" % (
            to_pretty_timedelta(timedelta(seconds=duration)))

    @property
    def failed(self):
        return False

    @property
    def failure_message(self):
        return None

    def summary(self):
        # type: () -> str
        return "{0} {1} {2}".format(self.progress, self.message,
                                    self.duration_str)

    def _progress_str(self, width):
        inner_width = width - 2
        out = "["
        done_chars = int(self.progress * inner_width)
        out += done_chars * '='
        out += (inner_width - done_chars) * '.'
        out += "]"

        return out

    def twoline_progress(self, indent=4):
        """
        e.g.

        - Eating my delicious strudel (since: 30s)
            [===============..............] (remaining: 04m)

        """
        time_remaining = self.estimated_time_remaining()
        if time_remaining:
            remaining = "(remaining: %s)" % (
                to_pretty_timedelta(timedelta(seconds=time_remaining)))
        else:
            remaining = ''
        return "{0} {1}\n{2}{3} {4}".format(self._message,
                                            self.duration_str,
                                            " " * indent,
                                            self._progress_str(30),
                                            remaining)

    def to_json(self):
        # type: () -> Dict[str, Any]
        return {
            "id": self.id,
            "message": self.message,
            "duration": self.duration_str,
            "refs": self._refs,
            "progress": self.progress,
            "started_at": self.started_at,
            "time_remaining": self.estimated_time_remaining()
        }

    def estimated_time_remaining(self):
        elapsed = time.time() - self.started_at
        progress = self.progress
        if progress == 0.0:
            return None
        return int(elapsed * (1 - progress) / progress)


class GhostEvent(Event):
    """
    The ghost of a completed event: these are the fields that we persist
    after the event is complete.
    """

    def __init__(self, my_id, message, refs, add_to_ceph_s, started_at, finished_at=None,
                 failed=False, failure_message=None):
        super().__init__(my_id, message, refs, add_to_ceph_s, started_at)
        self.finished_at = finished_at if finished_at else time.time()

        if failed:
            self._failed = True
            self._failure_message = failure_message
        else:
            self._failed = False

    @property
    def progress(self):
        return 1.0

    @property
    def failed(self):
        return self._failed

    @property
    def failure_message(self):
        return self._failure_message if self._failed else None

    def to_json(self):
        d = {
            "id": self.id,
            "message": self.message,
            "refs": self._refs,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "add_to_ceph_s:": self.add_to_ceph_s
        }
        if self._failed:
            d["failed"] = True
            d["failure_message"] = self._failure_message
        return d


class GlobalRecoveryEvent(Event):
    """
    An event whoese completion is determined by active+clean/total_pg_num
    """

    def __init__(self, message, refs, add_to_ceph_s, start_epoch, active_clean_num):
        # type: (str, List[Any], bool, int, int) -> None
        super().__init__(str(uuid.uuid4()), message, refs, add_to_ceph_s)
        self._add_to_ceph_s = add_to_ceph_s
        self._progress = 0.0
        self._start_epoch = start_epoch
        self._active_clean_num = active_clean_num
        self._refresh()

    def global_event_update_progress(self, log):
        # type: (logging.Logger) -> None
        "Update progress of Global Recovery Event"
        global _module
        assert _module
        skipped_pgs = 0
        active_clean_pgs = _module.get("active_clean_pgs")
        total_pg_num = active_clean_pgs["total_num_pgs"]
        new_active_clean_pgs = active_clean_pgs["pg_stats"]
        new_active_clean_num = len(new_active_clean_pgs)
        for pg in new_active_clean_pgs:
            # Disregard PGs that are not being reported
            # if the states are active+clean. Since it is
            # possible that some pgs might not have any movement
            # even before the start of the event.
            if pg['reported_epoch'] < self._start_epoch:
                log.debug("Skipping pg {0} since reported_epoch {1} < start_epoch {2}"
                             .format(pg['pgid'], pg['reported_epoch'], self._start_epoch))
                skipped_pgs += 1
                continue

        if self._active_clean_num != new_active_clean_num:
            # Have this case to know when need to update
            # the progress
            try:
                # Might be that total_pg_num is 0
                self._progress = float(new_active_clean_num) / (total_pg_num - skipped_pgs)
            except ZeroDivisionError:
                self._progress = 0.0
        else:
            # No need to update since there is no change
            return

        log.debug("Updated progress to %s", self.summary())
        self._refresh()

    @property
    def progress(self):
        return self._progress


class RemoteEvent(Event):
    """
    An event that was published by another module: we know nothing about
    this, rely on the other module to continuously update us with
    progress information as it emerges.
    """

    def __init__(self, my_id, message, refs, add_to_ceph_s):
        # type: (str, str, List[str], bool) -> None
        super().__init__(my_id, message, refs, add_to_ceph_s)
        self._progress = 0.0
        self._failed = False
        self._refresh()

    def set_progress(self, progress):
        # type: (float) -> None
        self._progress = progress
        self._refresh()

    def set_failed(self, message):
        self._progress = 1.0
        self._failed = True
        self._failure_message = message
        self._refresh()

    def set_message(self, message):
        self._message = message
        self._refresh()

    @property
    def progress(self):
        return self._progress

    @property
    def failed(self):
        return self._failed

    @property
    def failure_message(self):
        return self._failure_message if self._failed else None


class PgRecoveryEvent(Event):
    """
    An event whose completion is determined by the recovery of a set of
    PGs to a healthy state.

    Always call update() immediately after construction.
    """

    def __init__(self, message, refs, which_pgs, which_osds, start_epoch, add_to_ceph_s):
        # type: (str, List[Any], List[PgId], List[str], int, bool) -> None
        super().__init__(str(uuid.uuid4()), message, refs, add_to_ceph_s)
        self._pgs = which_pgs
        self._which_osds = which_osds
        self._original_pg_count = len(self._pgs)
        self._original_bytes_recovered = None  # type: Optional[Dict[PgId, float]]
        self._progress = 0.0

        self._start_epoch = start_epoch
        self._refresh()

    @property
    def which_osds(self):
        return self. _which_osds

    def pg_update(self, pg_progress: Dict, log: Any) -> None:
        # FIXME: O(pg_num) in python
        # Sanity check to see if there are any missing PGs and to assign
        # empty array and dictionary if there hasn't been any recovery
        pg_to_state: Dict[str, Any] = pg_progress["pgs"]
        pg_ready: bool = pg_progress["pg_ready"]

        if self._original_bytes_recovered is None:
            self._original_bytes_recovered = {}
            missing_pgs = []
            for pg in self._pgs:
                pg_str = str(pg)
                if pg_str in pg_to_state:
                    self._original_bytes_recovered[pg] = \
                        pg_to_state[pg_str]['num_bytes_recovered']
                else:
                    missing_pgs.append(pg)
            if pg_ready:
                for pg in missing_pgs:
                    self._pgs.remove(pg)

        complete_accumulate = 0.0

        # Calculating progress as the number of PGs recovered divided by the
        # original where partially completed PGs count for something
        # between 0.0-1.0. This is perhaps less faithful than looking at the
        # total number of bytes recovered, but it does a better job of
        # representing the work still to do if there are a number of very
        # few-bytes PGs that still need the housekeeping of their recovery
        # to be done. This is subjective...

        complete = set()
        for pg in self._pgs:
            pg_str = str(pg)
            try:
                info = pg_to_state[pg_str]
            except KeyError:
                # The PG is gone!  Probably a pool was deleted. Drop it.
                complete.add(pg)
                continue
            # Only checks the state of each PGs when it's epoch >= the OSDMap's epoch
            if info['reported_epoch'] < self._start_epoch:
                continue

            state = info['state']

            states = state.split("+")

            if "active" in states and "clean" in states:
                complete.add(pg)
            else:
                if info['num_bytes'] == 0:
                    # Empty PGs are considered 0% done until they are
                    # in the correct state.
                    pass
                else:
                    recovered = info['num_bytes_recovered']
                    total_bytes = info['num_bytes']
                    if total_bytes > 0:
                        ratio = float(recovered -
                                      self._original_bytes_recovered[pg]) / \
                            total_bytes
                        # Since the recovered bytes (over time) could perhaps
                        # exceed the contents of the PG (moment in time), we
                        # must clamp this
                        ratio = min(ratio, 1.0)
                        ratio = max(ratio, 0.0)

                    else:
                        # Dataless PGs (e.g. containing only OMAPs) count
                        # as half done.
                        ratio = 0.5
                    complete_accumulate += ratio

        self._pgs = list(set(self._pgs) ^ complete)
        completed_pgs = self._original_pg_count - len(self._pgs)
        completed_pgs = max(completed_pgs, 0)
        try:
            prog = (completed_pgs + complete_accumulate)\
                / self._original_pg_count
        except ZeroDivisionError:
            prog = 0.0

        self._progress = min(max(prog, 0.0), 1.0)

        self._refresh()
        log.info("Updated progress to %s", self.summary())

    @property
    def progress(self):
        # type: () -> float
        return self._progress


class PgId(object):
    def __init__(self, pool_id, ps):
        # type: (str, int) -> None
        self.pool_id = pool_id
        self.ps = ps

    def __cmp__(self, other):
        return (self.pool_id, self.ps) == (other.pool_id, other.ps)

    def __lt__(self, other):
        return (self.pool_id, self.ps) < (other.pool_id, other.ps)

    def __str__(self):
        return "{0}.{1:x}".format(self.pool_id, self.ps)


class Module(MgrModule):
    COMMANDS = [
        {"cmd": "progress",
         "desc": "Show progress of recovery operations",
         "perm": "r"},
        {"cmd": "progress json",
         "desc": "Show machine readable progress information",
         "perm": "r"},
        {"cmd": "progress clear",
         "desc": "Reset progress tracking",
         "perm": "rw"},
        {"cmd": "progress on",
         "desc": "Enable progress tracking",
         "perm": "rw"},
        {"cmd": "progress off",
         "desc": "Disable progress tracking",
         "perm": "rw"}

    ]

    MODULE_OPTIONS = [
        Option(
            'max_completed_events',
            default=50,
            type='int',
            desc='number of past completed events to remember',
            runtime=True
        ),
        Option(
            'sleep_interval',
            default=5,
            type='secs',
            desc='how long the module is going to sleep',
            runtime=True
        ),
        Option(
            'enabled',
            default=True,
            type='bool',
        )
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self._events = {}  # type: Dict[str, Union[RemoteEvent, PgRecoveryEvent, GlobalRecoveryEvent]]
        self._completed_events = []  # type: List[GhostEvent]

        self._old_osd_map = None  # type: Optional[OSDMap]

        self._ready = threading.Event()
        self._shutdown = threading.Event()

        self._latest_osdmap = None  # type: Optional[OSDMap]

        self._dirty = False

        global _module
        _module = self

        # only for mypy
        if TYPE_CHECKING:
            self.max_completed_events = 0
            self.sleep_interval = 0
            self.enabled = True

    def config_notify(self):
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' %s = %s', opt['name'], getattr(self, opt['name']))

    def _osd_in_out(self, old_map, old_dump, new_map, osd_id, marked):
        # type: (OSDMap, Dict, OSDMap, str, str) -> None
        # A function that will create or complete an event when an
        # OSD is marked in or out according to the affected PGs
        affected_pgs = []
        for pool in old_dump['pools']:
            pool_id = pool['pool']  # type: str
            for ps in range(0, pool['pg_num']):

                # Was this OSD affected by the OSD coming in/out?
                # Compare old and new osds using
                # data from the json dump
                old_up_acting = old_map.pg_to_up_acting_osds(pool['pool'], ps)
                old_osds = set(old_up_acting['acting'])
                new_up_acting = new_map.pg_to_up_acting_osds(pool['pool'], ps)
                new_osds = set(new_up_acting['acting'])

                # Check the osd_id being in the acting set for both old
                # and new maps to cover both out and in cases
                was_on_out_or_in_osd = osd_id in old_osds or osd_id in new_osds
                if not was_on_out_or_in_osd:
                    continue

                self.log.debug("pool_id, ps = {0}, {1}".format(
                    pool_id, ps
                ))

                self.log.debug(
                    "old_up_acting: {0}".format(json.dumps(old_up_acting, indent=4, sort_keys=True)))

                # Has this OSD been assigned a new location?
                # (it might not be if there is no suitable place to move
                #  after an OSD is marked in/out)
                
                is_relocated = old_osds != new_osds
                
                self.log.debug(
                    "new_up_acting: {0}".format(json.dumps(new_up_acting,
                                                           indent=4,
                                                           sort_keys=True)))

                if was_on_out_or_in_osd and is_relocated:
                    # This PG is now in motion, track its progress
                    affected_pgs.append(PgId(pool_id, ps))

        # In the case that we ignored some PGs, log the reason why (we may
        # not end up creating a progress event)

        self.log.warning("{0} PGs affected by osd.{1} being marked {2}".format(
            len(affected_pgs), osd_id, marked))

        # In the case of the osd coming back in, we might need to cancel
        # previous recovery event for that osd
        if marked == "in":
            for ev_id in list(self._events):
                try:
                    ev = self._events[ev_id]
                    if isinstance(ev, PgRecoveryEvent) and osd_id in ev.which_osds:
                        self.log.info("osd.{0} came back in, cancelling event".format(
                            osd_id
                        ))
                        self._complete(ev)
                except KeyError:
                    self.log.warning("_osd_in_out: ev {0} does not exist".format(ev_id))

        if len(affected_pgs) > 0:
            r_ev = PgRecoveryEvent(
                    "Rebalancing after osd.{0} marked {1}".format(osd_id, marked),
                    refs=[("osd", osd_id)],
                    which_pgs=affected_pgs,
                    which_osds=[osd_id],
                    start_epoch=self.get_osdmap().get_epoch(),
                    add_to_ceph_s=False
                    )
            r_ev.pg_update(self.get("pg_progress"), self.log)
            self._events[r_ev.id] = r_ev

    def _osdmap_changed(self, old_osdmap, new_osdmap):
        # type: (OSDMap, OSDMap) -> None
        old_dump = old_osdmap.dump()
        new_dump = new_osdmap.dump()

        old_osds = dict([(o['osd'], o) for o in old_dump['osds']])

        for osd in new_dump['osds']:
            osd_id = osd['osd']
            new_weight = osd['in']
            if osd_id in old_osds:
                old_weight = old_osds[osd_id]['in']

                if new_weight == 0.0 and old_weight > new_weight:
                    self.log.warning("osd.{0} marked out".format(osd_id))
                    self._osd_in_out(old_osdmap, old_dump, new_osdmap, osd_id, "out")
                elif new_weight >= 1.0 and old_weight == 0.0:
                    # Only consider weight>=1.0 as "in" to avoid spawning
                    # individual recovery events on every adjustment
                    # in a gradual weight-in
                    self.log.warning("osd.{0} marked in".format(osd_id))
                    self._osd_in_out(old_osdmap, old_dump, new_osdmap, osd_id, "in")

    def _pg_state_changed(self):

        # This function both constructs and updates
        # the global recovery event if one of the
        # PGs is not at active+clean state
        active_clean_pgs = self.get("active_clean_pgs")
        total_pg_num = active_clean_pgs["total_num_pgs"]
        active_clean_num = len(active_clean_pgs["pg_stats"])
        try:
            # There might be a case where there is no pg_num
            progress = float(active_clean_num) / total_pg_num
        except ZeroDivisionError:
            return
        if progress < 1.0:
            self.log.warning(("Starting Global Recovery Event,"
                              "%d pgs not in active + clean state"),
                              total_pg_num - active_clean_num)
            ev = GlobalRecoveryEvent("Global Recovery Event",
                    refs=[("global", "")],
                    add_to_ceph_s=True,
                    start_epoch=self.get_osdmap().get_epoch(),
                    active_clean_num=active_clean_num)
            ev.global_event_update_progress(self.log)
            self._events[ev.id] = ev

    def _process_osdmap(self):
        old_osdmap = self._latest_osdmap
        self._latest_osdmap = self.get_osdmap()
        assert old_osdmap
        assert self._latest_osdmap
        self.log.info(("Processing OSDMap change %d..%d"),
            old_osdmap.get_epoch(), self._latest_osdmap.get_epoch())

        self._osdmap_changed(old_osdmap, self._latest_osdmap)

    def _process_pg_summary(self):
        # if there are no events we will skip this here to avoid
        # expensive get calls
        if len(self._events) == 0:
            return

        global_event = False
        data = self.get("pg_progress")
        for ev_id in list(self._events):
            try:
                ev = self._events[ev_id]
                # Check for types of events
                # we have to update
                if isinstance(ev, PgRecoveryEvent):
                    ev.pg_update(data, self.log)
                    self.maybe_complete(ev)
                elif isinstance(ev, GlobalRecoveryEvent):
                    global_event = True
                    ev.global_event_update_progress(self.log)
                    self.maybe_complete(ev)
            except KeyError:
                self.log.warning("_process_pg_summary: ev {0} does not exist".format(ev_id))
                continue

        if not global_event:
            # If there is no global event
            # we create one
            self._pg_state_changed()

    def maybe_complete(self, event):
        # type: (Event) -> None
        if event.progress >= 1.0:
            self._complete(event)

    def _save(self):
        self.log.info("Writing back {0} completed events".format(
            len(self._completed_events)
        ))
        # TODO: bound the number we store.
        encoded = json.dumps({
            "events": [ev.to_json() for ev in self._completed_events],
            "version": ENCODING_VERSION,
            "compat_version": ENCODING_VERSION
        })
        self.set_store("completed", encoded)

    def _load(self):
        stored = self.get_store("completed")

        if stored is None:
            self.log.info("No stored events to load")
            return

        decoded = json.loads(stored)
        if decoded['compat_version'] > ENCODING_VERSION:
            raise RuntimeError("Cannot decode version {0}".format(
                               decoded['compat_version']))

        if decoded['compat_version'] < ENCODING_VERSION:
            # we need to add the "started_at" and "finished_at" attributes to the events
            for ev in decoded['events']:
                ev['started_at'] = None
                ev['finished_at'] = None

        for ev in decoded['events']:
            self._completed_events.append(GhostEvent(ev['id'], ev['message'],
                                                     ev['refs'], ev['started_at'],
                                                     ev['finished_at'],
                                                     ev.get('failed', False),
                                                     ev.get('failure_message')))

        self._prune_completed_events()

    def _prune_completed_events(self):
        length = len(self._completed_events)
        if length > self.max_completed_events:
            self._completed_events = self._completed_events[length - self.max_completed_events : length]
            self._dirty = True

    def serve(self):
        self.config_notify()
        self.clear_all_progress_events()
        self.log.info("Loading...")

        self._load()
        self.log.info("Loaded {0} historic events".format(self._completed_events))

        self._latest_osdmap = self.get_osdmap()
        self.log.info("Loaded OSDMap, ready.")

        self._ready.set()

        while not self._shutdown.is_set():
            # Lazy periodic write back of completed events
            if self._dirty:
                self._save()
                self._dirty = False

            if self.enabled:
                self._process_osdmap()
                self._process_pg_summary()

            self._shutdown.wait(timeout=self.sleep_interval)

        self._shutdown.wait()

    def shutdown(self):
        self._shutdown.set()
        self.clear_all_progress_events()

    def update(self, ev_id, ev_msg, ev_progress, refs=None, add_to_ceph_s=False):
        # type: (str, str, float, Optional[list], bool) -> None
        """
        For calling from other mgr modules
        """
        if not self.enabled:
            return

        if refs is None:
            refs = []
        try:
            ev = self._events[ev_id]
            assert isinstance(ev, RemoteEvent)
        except KeyError:
            # if key doesn't exist we create an event
            ev = RemoteEvent(ev_id, ev_msg, refs, add_to_ceph_s)
            self._events[ev_id] = ev
            self.log.info("update: starting ev {0} ({1})".format(
                ev_id, ev_msg))
        else:
            self.log.debug("update: {0} on {1}".format(
                ev_progress, ev_msg))

        ev.set_progress(ev_progress)
        ev.set_message(ev_msg)

    def _complete(self, ev):
        # type: (Event) -> None
        duration = (time.time() - ev.started_at)
        self.log.info("Completed event {0} ({1}) in {2} seconds".format(
            ev.id, ev.message, int(round(duration))
        ))
        self.complete_progress_event(ev.id)

        self._completed_events.append(
            GhostEvent(ev.id, ev.message, ev.refs, ev.add_to_ceph_s, ev.started_at,
                       failed=ev.failed, failure_message=ev.failure_message))
        assert ev.id
        del self._events[ev.id]
        self._prune_completed_events()
        self._dirty = True

    def complete(self, ev_id):
        """
        For calling from other mgr modules
        """
        if not self.enabled:
            return
        try:
            ev = self._events[ev_id]
            assert isinstance(ev, RemoteEvent)
            ev.set_progress(1.0)
            self.log.info("complete: finished ev {0} ({1})".format(ev_id,
                                                                   ev.message))
            self._complete(ev)
        except KeyError:
            self.log.warning("complete: ev {0} does not exist".format(ev_id))
            pass

    def fail(self, ev_id, message):
        """
        For calling from other mgr modules to mark an event as failed (and
        complete)
        """
        try:
            ev = self._events[ev_id]
            assert isinstance(ev, RemoteEvent)
            ev.set_failed(message)
            self.log.info("fail: finished ev {0} ({1}): {2}".format(ev_id,
                                                                    ev.message,
                                                                    message))
            self._complete(ev)
        except KeyError:
            self.log.warning("fail: ev {0} does not exist".format(ev_id))

    def on(self):
        self.set_module_option('enabled', "true")

    def off(self):
        self.set_module_option('enabled', "false")

    def _handle_ls(self):
        if len(self._events) or len(self._completed_events):
            out = ""
            chrono_order = sorted(self._events.values(),
                                  key=lambda x: x.started_at, reverse=True)
            for ev in chrono_order:
                out += ev.twoline_progress()
                out += "\n"

            if len(self._completed_events):
                # TODO: limit number of completed events to show
                out += "\n"
                for ghost_ev in self._completed_events:
                    out += "[{0}]: {1}\n".format("Complete" if not ghost_ev.failed else "Failed",
                                                 ghost_ev.twoline_progress())

            return 0, out, ""
        else:
            return 0, "", "Nothing in progress"

    def _json(self):
        return {
            'events': [ev.to_json() for ev in self._events.values()],
            'completed': [ev.to_json() for ev in self._completed_events]
        }

    def clear(self):
        self._events = {}
        self._completed_events = []
        self._dirty = True
        self._save()
        self.clear_all_progress_events()

    def _handle_clear(self):
        self.clear()
        return 0, "", ""

    def handle_command(self, _, cmd):
        if cmd['prefix'] == "progress":
            return self._handle_ls()
        elif cmd['prefix'] == "progress clear":
            # The clear command isn't usually needed - it's to enable
            # the admin to "kick" this module if it seems to have done
            # something wrong (e.g. we have a bug causing a progress event
            # that never finishes)
            return self._handle_clear()
        elif cmd['prefix'] == "progress json":
            return 0, json.dumps(self._json(), indent=4, sort_keys=True), ""
        elif cmd['prefix'] == "progress on":
            if self.enabled:
                return 0, "", "progress already enabled!"
            self.on()
            return 0, "", "progress enabled"
        elif cmd['prefix'] == "progress off":
            if not self.enabled:
                return 0, "", "progress already disabled!"
            self.off()
            self.clear()
            return 0, "", "progress disabled"
        else:
            raise NotImplementedError(cmd['prefix'])
