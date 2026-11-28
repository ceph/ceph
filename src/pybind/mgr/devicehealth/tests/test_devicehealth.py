import sqlite3
from unittest import mock

import pytest

from devicehealth.module import Module
from mgr_module import MAX_DBCLEANUP_RETRIES


@pytest.fixture()
def module():
    m = Module('devicehealth', 0, 0)
    with mock.patch.object(m, 'event'), \
            mock.patch.object(m, 'config_notify'), \
            mock.patch.object(m, 'db_ready', return_value=True), \
            mock.patch.object(m, 'check_legacy_pool', return_value=True), \
            mock.patch.object(m, 'get_kv', return_value=None), \
            mock.patch.object(m, 'set_kv'), \
            mock.patch.object(m, 'scrape_all'), \
            mock.patch.object(m, 'predict_all_devices'), \
            mock.patch.object(m, 'close_db'), \
            mock.patch.object(m, 'open_db'):
        yield m


def serve(module, passes, fails):
    """
    Run serve() for `passes` loop passes; the database is lost (sqlite
    "disk I/O error", as libcephsqlite reports a lost lock) on every pass
    for which fails(n) is true. Returns the number of passes made.
    """
    n = 0

    def check_health():
        nonlocal n
        n += 1
        if n >= passes:
            module.run = False
        if fails(n):
            raise sqlite3.OperationalError('disk I/O error')

    with mock.patch.object(module, 'check_health', side_effect=check_health):
        module.serve()
    return n


def test_serve_survives_repeated_db_loss(module):
    # the database is reopened after each loss; losses spread over the
    # module's lifetime must not add up to a fatal error
    losses = MAX_DBCLEANUP_RETRIES + 2
    serve(module, passes=3 * losses + 1, fails=lambda n: n % 3 == 0)
    assert module.open_db.call_count == losses


def test_serve_fails_on_persistent_db_loss(module):
    # a database that stays lost still fails the module after
    # MAX_DBCLEANUP_RETRIES reopens instead of retrying forever
    failed = []
    with pytest.raises(sqlite3.OperationalError):
        serve(module, passes=100, fails=lambda n: failed.append(n) or True)
    assert len(failed) == MAX_DBCLEANUP_RETRIES + 1
    assert module.open_db.call_count == MAX_DBCLEANUP_RETRIES
