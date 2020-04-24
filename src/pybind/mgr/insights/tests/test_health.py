import unittest
from tests import mock
from ..health import *

class HealthChecksTest(unittest.TestCase):
    def test_check_accum_empty(self):
        # health checks accum initially empty reports empty
        h = HealthCheckAccumulator()
        self.assertEqual(h.checks(), {})

        h = HealthCheckAccumulator({})
        self.assertEqual(h.checks(), {})

    def _get_init_checks(self):
        return HealthCheckAccumulator({
            "C0": {
                "S0": {
                    "summary": ["s0", "s1"],
                    "detail": ("d0", "d1")
                }
            }
        })

    def test_check_init(self):
        # initialization with lists and tuples is OK
        h = self._get_init_checks()
        self.assertEqual(h.checks(), {
            "C0": {
                "S0": {
                    "summary": set(["s0", "s1"]),
                    "detail": set(["d0", "d1"])
                }
            }
        })

    def _get_merged_checks(self):
        h = self._get_init_checks()
        h.merge(HealthCheckAccumulator({
            "C0": {
                "S0": {
                    "summary": ["s0", "s1", "s2"],
                    "detail": ("d2",)
                },
                "S1": {
                    "summary": ["s0", "s1", "s2"],
                    "detail": ()
                }
            },
            "C1": {
                "S0": {
                    "summary": [],
                    "detail": ("d0", "d1", "d2")
                }
            }
        }))
        return h

    def test_check_merge(self):
        # merging combines and de-duplicates
        h = self._get_merged_checks()
        self.assertEqual(h.checks(), {
            "C0": {
                "S0": {
                    "summary": set(["s0", "s1", "s2"]),
                    "detail": set(["d0", "d1", "d2"])
                },
                "S1": {
                    "summary": set(["s0", "s1", "s2"]),
                    "detail": set([])
                }
            },
            "C1": {
                "S0": {
                    "summary": set([]),
                    "detail": set(["d0", "d1", "d2"])
                }
            }
        })

    def test_check_add_no_change(self):
        # returns false when nothing changes
        h = self._get_merged_checks()

        self.assertFalse(h.add({}))

        self.assertFalse(h.add({
            "C0": {
                "severity": "S0",
                "summary": { "message": "s0" },
                "detail": []
            }
        }))

        self.assertFalse(h.add({
            "C0": {
                "severity": "S0",
                "summary": { "message": "s1" },
                "detail": [{ "message": "d1" }]
            }
        }))

        self.assertFalse(h.add({
            "C0": {
                "severity": "S0",
                "summary": { "message": "s0" },
                "detail": [{ "message": "d1" }, { "message": "d2" }]
            }
        }))

    def test_check_add_changed(self):
        # new checks report change
        h = self._get_merged_checks()

        self.assertTrue(h.add({
            "C0": {
                "severity": "S0",
                "summary": { "message": "s3" },
                "detail": []
            }
        }))

        self.assertTrue(h.add({
            "C0": {
                "severity": "S0",
                "summary": { "message": "s1" },
                "detail": [{ "message": "d4" }]
            }
        }))

        self.assertTrue(h.add({
            "C0": {
                "severity": "S2",
                "summary": { "message": "s0" },
                "detail": [{ "message": "d0" }]
            }
        }))

        self.assertTrue(h.add({
            "C2": {
                "severity": "S0",
                "summary": { "message": "s0" },
                "detail": [{ "message": "d0" }, { "message": "d1" }]
            }
        }))

        self.assertEqual(h.checks(), {
            "C0": {
                "S0": {
                    "summary": set(["s0", "s1", "s2", "s3"]),
                    "detail": set(["d0", "d1", "d2", "d4"])
                },
                "S1": {
                    "summary": set(["s0", "s1", "s2"]),
                    "detail": set([])
                },
                "S2": {
                    "summary": set(["s0"]),
                    "detail": set(["d0"])
                }
            },
            "C1": {
                "S0": {
                    "summary": set([]),
                    "detail": set(["d0", "d1", "d2"])
                }
            },
            "C2": {
                "S0": {
                    "summary": set(["s0"]),
                    "detail": set(["d0", "d1"])
                }
            }
        })

class HealthHistoryTest(unittest.TestCase):
    def _now(self):
        # return some time truncated at 30 minutes past the hour. this lets us
        # fiddle with time offsets without worrying about accidentally landing
        # on exactly the top of the hour which is the edge of a time slot for
        # tracking health history.
        dt = datetime.datetime.utcnow()
        return datetime.datetime(
            year   = dt.year,
            month  = dt.month,
            day    = dt.day,
            hour   = dt.hour,
            minute = 30)

    def test_empty_slot(self):
        now = self._now()

        HealthHistorySlot._now = mock.Mock(return_value=now)
        h = HealthHistorySlot()

        # reports no historical checks
        self.assertEqual(h.health(), { "checks": {} })

        # an empty slot doesn't need to be flushed
        self.assertFalse(h.need_flush())

    def test_expires(self):
        now = self._now()

        HealthHistorySlot._now = mock.Mock(return_value=now)
        h = HealthHistorySlot()
        self.assertFalse(h.expired())

        # an hour from now it would be expired
        future = now + datetime.timedelta(hours = 1)
        HealthHistorySlot._now = mock.Mock(return_value=future)
        self.assertTrue(h.expired())

    def test_need_flush(self):
        now = self._now()

        HealthHistorySlot._now = mock.Mock(return_value=now)
        h = HealthHistorySlot()
        self.assertFalse(h.need_flush())

        self.assertTrue(h.add(dict(checks = {
            "C0": {
                "severity": "S0",
                "summary": { "message": "s0" },
                "detail": [{ "message": "d0" }]
            }
        })))
        # no flush needed, yet...
        self.assertFalse(h.need_flush())

        # after persist period time elapses, a flush is needed
        future = now + PERSIST_PERIOD
        HealthHistorySlot._now = mock.Mock(return_value=future)
        self.assertTrue(h.need_flush())

        # mark flush resets
        h.mark_flushed()
        self.assertFalse(h.need_flush())

    def test_need_flush_edge(self):
        # test needs flush is true because it has expired, not because it has
        # been dirty for the persistence period
        dt = datetime.datetime.utcnow()
        now = datetime.datetime(
            year   = dt.year,
            month  = dt.month,
            day    = dt.day,
            hour   = dt.hour,
            minute = 59,
            second = 59)
        HealthHistorySlot._now = mock.Mock(return_value=now)
        h = HealthHistorySlot()
        self.assertFalse(h.expired())
        self.assertFalse(h.need_flush())

        # now it is dirty, but it doesn't need a flush
        self.assertTrue(h.add(dict(checks = {
            "C0": {
                "severity": "S0",
                "summary": { "message": "s0" },
                "detail": [{ "message": "d0" }]
            }
        })))
        self.assertFalse(h.expired())
        self.assertFalse(h.need_flush())

        # advance time past the hour so it expires, but not past the persistence
        # period deadline for the last event that set the dirty bit
        self.assertTrue(PERSIST_PERIOD.total_seconds() > 5)
        future = now + datetime.timedelta(seconds = 5)
        HealthHistorySlot._now = mock.Mock(return_value=future)

        self.assertTrue(h.expired())
        self.assertTrue(h.need_flush())
