try:
    from typing import List, Dict, Union, Any, Optional
    from typing import TYPE_CHECKING
except ImportError:
    TYPE_CHECKING = False

from mgr_module import MgrModule, OSDMap
from mgr_util import to_pretty_timedelta
from datetime import timedelta
import os
import threading
import datetime
import uuid
import time

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

    def __init__(self, message, refs, started_at=None):
        # type: (str, List[str], Optional[float]) -> None
        self._message = message
        self._refs = refs
        self.started_at = started_at if started_at else time.time()
        self.id = None  # type: Optional[str]

    def _refresh(self):
        global _module
        assert _module
        _module.log.debug('refreshing mgr for %s (%s) at %f' % (self.id, self._message,
                                                                self.progress))
        _module.update_progress_event(
            self.id, self.twoline_progress(6), self.progress)

    @property
    def message(self):
        # type: () -> str
        return self._message

    @property
    def refs(self):
        # type: () -> List[str]
        return self._refs

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

    def __init__(self, my_id, message, refs, started_at, finished_at=None,
                 failed=False, failure_message=None):
        super(GhostEvent, self).__init__(message, refs, started_at)
        self.finished_at = finished_at if finished_at else time.time()
        self.id = my_id

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
            "finished_at": self.finished_at
        }
        if self._failed:
            d["failed"] = True
            d["failure_message"] = self._failure_message
        return d


class RemoteEvent(Event):
    """
    An event that was published by another module: we know nothing about
    this, rely on the other module to continuously update us with
    progress information as it emerges.
    """

    def __init__(self, my_id, message, refs):
        # type: (str, str, List[str]) -> None
        super(RemoteEvent, self).__init__(message, refs)
        self.id = my_id
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

    def __init__(self, message, refs, which_pgs, which_osds, start_epoch):
        # type: (str, List[Any], List[PgId], List[str], int) -> None
        super(PgRecoveryEvent, self).__init__(message, refs)

        self._pgs = which_pgs

        self._which_osds = which_osds

        self._original_pg_count = len(self._pgs)

        self._original_bytes_recovered = None  # type: Optional[Dict[PgId, float]]

        self._progress = 0.0

        # self._start_epoch = _module.get_osdmap().get_epoch()
        self._start_epoch = start_epoch

        self.id = str(uuid.uuid4())  # type: str
        self._refresh()

    @property
    def which_osds(self):
        return self. _which_osds

    def pg_update(self, raw_pg_stats, pg_ready, log):
        # type: (Dict, bool, Any) -> None
        # FIXME: O(pg_num) in python
        # FIXME: far more fields getting pythonized than we really care about
        # Sanity check to see if there are any missing PGs and to assign
        # empty array and dictionary if there hasn't been any recovery
        pg_to_state = dict([(p['pgid'], p) for p in raw_pg_stats['pg_stats']]) # type: Dict[str, Any]
        if self._original_bytes_recovered is None:
            self._original_bytes_recovered = {}
            missing_pgs = []
            for pg in self._pgs:
                pg_str = str(pg)
                if pg_str in pg_to_state:
                    self._original_bytes_recovered[pg] = \
                        pg_to_state[pg_str]['stat_sum']['num_bytes_recovered']
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
            if int(info['reported_epoch']) < int(self._start_epoch):
                continue

            state = info['state']

            states = state.split("+")

            if "active" in states and "clean" in states:
                complete.add(pg)
            else:
                if info['stat_sum']['num_bytes'] == 0:
                    # Empty PGs are considered 0% done until they are
                    # in the correct state.
                    pass
                else:
                    recovered = info['stat_sum']['num_bytes_recovered']
                    total_bytes = info['stat_sum']['num_bytes']
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
        self._progress = (completed_pgs + complete_accumulate)\
            / self._original_pg_count

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
         "perm": "rw"}
    ]

    MODULE_OPTIONS = [
        {
            'name': 'max_completed_events',
            'default': 50,
            'type': 'int',
            'desc': 'number of past completed events to remember',
            'runtime': True,
        },
        {
            'name': 'persist_interval',
            'default': 5,
            'type': 'secs',
            'desc': 'how frequently to persist completed events',
            'runtime': True,
        },
    ]  # type: List[Dict[str, Any]]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        self._events = {}  # type: Dict[str, Union[RemoteEvent, PgRecoveryEvent]]
        self._completed_events = [] # type: List[GhostEvent]

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
            self.persist_interval = 0

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
        unmoved_pgs = []
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
                if marked == "in":
                    is_relocated = len(old_osds - new_osds) > 0
                else:
                    is_relocated = len(new_osds - old_osds) > 0

                self.log.debug(
                    "new_up_acting: {0}".format(json.dumps(new_up_acting,
                                                           indent=4,
                                                           sort_keys=True)))

                if was_on_out_or_in_osd and is_relocated:
                    # This PG is now in motion, track its progress
                    affected_pgs.append(PgId(pool_id, ps))
                elif not is_relocated:
                    # This PG didn't get a new location, we'll log it
                    unmoved_pgs.append(PgId(pool_id, ps))

        # In the case that we ignored some PGs, log the reason why (we may
        # not end up creating a progress event)
        if len(unmoved_pgs):
            self.log.warning("{0} PGs were on osd.{1}, but didn't get new locations".format(
                len(unmoved_pgs), osd_id))

        self.log.warning("{0} PGs affected by osd.{1} being marked {2}".format(
            len(affected_pgs), osd_id, marked))


        # In the case of the osd coming back in, we might need to cancel
        # previous recovery event for that osd
        if marked == "in":
            for ev_id in list(self._events):
                ev = self._events[ev_id]
                if isinstance(ev, PgRecoveryEvent) and osd_id in ev.which_osds:
                    self.log.info("osd.{0} came back in, cancelling event".format(
                        osd_id
                    ))
                    self._complete(ev)

        if len(affected_pgs) > 0:
            r_ev = PgRecoveryEvent(
                    "Rebalancing after osd.{0} marked {1}".format(osd_id, marked),
                    refs=[("osd", osd_id)],
                    which_pgs=affected_pgs,
                    which_osds=[osd_id],
                    start_epoch=self.get_osdmap().get_epoch()
                    )
            r_ev.pg_update(self.get("pg_stats"), self.get("pg_ready"), self.log)
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

    def notify(self, notify_type, notify_data):
        self._ready.wait()

        if notify_type == "osd_map":
            old_osdmap = self._latest_osdmap
            self._latest_osdmap = self.get_osdmap()
            assert old_osdmap
            assert self._latest_osdmap

            self.log.info("Processing OSDMap change {0}..{1}".format(
                old_osdmap.get_epoch(), self._latest_osdmap.get_epoch()
            ))
            self._osdmap_changed(old_osdmap, self._latest_osdmap)
        elif notify_type == "pg_summary":
            # if there are no events we will skip this here to avoid 
            # expensive get calls
            if len(self._events) == 0:
                return
            data = self.get("pg_stats")
            ready = self.get("pg_ready")
            for ev_id in list(self._events):
                ev = self._events[ev_id]
                if isinstance(ev, PgRecoveryEvent):
                    ev.pg_update(data, ready, self.log)
                    self.maybe_complete(ev)

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

            self._shutdown.wait(timeout=self.persist_interval)

        self._shutdown.wait()

    def shutdown(self):
        self._shutdown.set()
        self.clear_all_progress_events()

    def update(self, ev_id, ev_msg, ev_progress, refs=None):
        # type: (str, str, float, Optional[list]) -> None
        """
        For calling from other mgr modules
        """
        if refs is None:
            refs = []
        try:

            ev = self._events[ev_id]
            assert isinstance(ev, RemoteEvent)
        except KeyError:
            ev = RemoteEvent(ev_id, ev_msg, refs)
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
            GhostEvent(ev.id, ev.message, ev.refs, ev.started_at,
                       failed=ev.failed, failure_message=ev.failure_message))
        assert ev.id
        del self._events[ev.id]
        self._prune_completed_events()
        self._dirty = True

    def complete(self, ev_id):
        """
        For calling from other mgr modules
        """
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

    def _handle_clear(self):
        self._events = {}
        self._completed_events = []
        self._dirty = True
        self._save()
        self.clear_all_progress_events()

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
        else:
            raise NotImplementedError(cmd['prefix'])
