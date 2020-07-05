import json
from collections import defaultdict
import datetime

# freq to write cached state to disk
PERSIST_PERIOD = datetime.timedelta(seconds = 10)
# on disk key prefix
HEALTH_HISTORY_KEY_PREFIX = "health_history/"
# apply on offset to "now": used for testing
NOW_OFFSET = None

class HealthEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

class HealthCheckAccumulator(object):
    """
    Deuplicated storage of health checks.
    """
    def __init__(self, init_checks = None):
        # check : severity : { summary, detail }
        # summary and detail are deduplicated
        self._checks = defaultdict(lambda:
            defaultdict(lambda: {
                "summary": set(),
                "detail": set()
            }))

        if init_checks:
            self._update(init_checks)

    def __str__(self):
        return "check count {}".format(len(self._checks))

    def add(self, checks):
        """
        Add health checks to the current state

        Returns:
            bool: True if the state changed, False otherwise.
        """
        changed = False

        for check, info in checks.items():

            # only keep the icky stuff
            severity = info["severity"]
            if severity == "HEALTH_OK":
                continue

            summary = info["summary"]["message"]
            details = map(lambda d: d["message"], info["detail"])

            if self._add_check(check, severity, [summary], details):
                changed = True

        return changed

    def checks(self):
        return self._checks

    def merge(self, other):
        assert isinstance(other, HealthCheckAccumulator)
        self._update(other._checks)

    def _update(self, checks):
        """Merge checks with same structure. Does not set dirty bit"""
        for check in checks:
            for severity in checks[check]:
                summaries = set(checks[check][severity]["summary"])
                details = set(checks[check][severity]["detail"])
                self._add_check(check, severity, summaries, details)

    def _add_check(self, check, severity, summaries, details):
        changed = False

        for summary in summaries:
            if summary not in self._checks[check][severity]["summary"]:
                changed = True
                self._checks[check][severity]["summary"].add(summary)

        for detail in details:
            if detail not in self._checks[check][severity]["detail"]:
                changed = True
                self._checks[check][severity]["detail"].add(detail)

        return changed

class HealthHistorySlot(object):
    """
    Manage the life cycle of a health history time slot.

    A time slot is a fixed slice of wall clock time (e.g. every hours, from :00
    to :59), and all health updates that occur during this time are deduplicated
    together. A slot is initially in a clean state, and becomes dirty when a new
    health check is observed. The state of a slot should be persisted when
    need_flush returns true. Once the state has been flushed, reset the dirty
    bit by calling mark_flushed.
    """
    def __init__(self, init_health = dict()):
        self._checks = HealthCheckAccumulator(init_health.get("checks"))
        self._slot = self._curr_slot()
        self._next_flush = None

    def __str__(self):
        return "key {} next flush {} checks {}".format(
            self.key(), self._next_flush, self._checks)

    def health(self):
        return dict(checks = self._checks.checks())

    def key(self):
        """Identifier in the persist store"""
        return self._key(self._slot)

    def expired(self):
        """True if this slot is the current slot, False otherwise"""
        return self._slot != self._curr_slot()

    def need_flush(self):
        """True if this slot needs to be flushed, False otherwise"""
        now = HealthHistorySlot._now()
        if self._next_flush is not None:
            if self._next_flush <= now or self.expired():
                return True
        return False

    def mark_flushed(self):
        """Reset the dirty bit. Caller persists state"""
        assert self._next_flush
        self._next_flush = None

    def add(self, health):
        """
        Add health to the underlying health accumulator. When the slot
        transitions from clean to dirty a target flush time is computed.
        """
        changed = self._checks.add(health["checks"])
        if changed and not self._next_flush:
            self._next_flush = HealthHistorySlot._now() + PERSIST_PERIOD
        return changed

    def merge(self, other):
        assert isinstance(other, HealthHistorySlot)
        self._checks.merge(other._checks)

    @staticmethod
    def key_range(hours):
        """Return the time slot keys for the past N hours"""
        def inner(curr, hours):
            slot = curr - datetime.timedelta(hours = hours)
            return HealthHistorySlot._key(slot)
        curr = HealthHistorySlot._curr_slot()
        return map(lambda i: inner(curr, i), range(hours))

    @staticmethod
    def curr_key():
        """Key for the current UTC time slot"""
        return HealthHistorySlot._key(HealthHistorySlot._curr_slot())

    @staticmethod
    def key_to_time(key):
        """Return key converted into datetime"""
        timestr = key[len(HEALTH_HISTORY_KEY_PREFIX):]
        return datetime.datetime.strptime(timestr, "%Y-%m-%d_%H")

    @staticmethod
    def _key(dt):
        """Key format. Example: health_2018_11_05_00"""
        return HEALTH_HISTORY_KEY_PREFIX + dt.strftime("%Y-%m-%d_%H")

    @staticmethod
    def _now():
        """Control now time for easier testing"""
        now = datetime.datetime.utcnow()
        if NOW_OFFSET is not None:
            now = now + NOW_OFFSET
        return now

    @staticmethod
    def _curr_slot():
        """Slot for the current UTC time"""
        dt = HealthHistorySlot._now()
        return datetime.datetime(
            year  = dt.year,
            month = dt.month,
            day   = dt.day,
            hour  = dt.hour)
