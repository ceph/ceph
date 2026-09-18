"""pytest configuration for the cephadm test suite.

"""
# Pre-import tracemalloc so that pytest's unraisable-exception hook
# (_pytest.unraisableexception.unraisable_hook -> tracemalloc_message) does not
# lazily import it from inside a garbage-collection pass.
#
# Some tests (e.g. TestBootstrap) leave NamedTemporaryFile objects created on
# the pyfakefs fake filesystem to be garbage collected after the fake fs has
# been torn down; their __del__ raises, pytest's hook runs during GC and, the
# first time, imports tracemalloc.  If that GC happens while pyfakefs'
# Patcher._find_modules() is iterating sys.modules, the `fs` fixture setup
# fails with "RuntimeError: dictionary changed size during iteration" and,
# because Patcher is a ref-counted singleton, every later fs-based test errors
# with "'NoneType' object has no attribute 'add_real_directory'".
import tracemalloc  # noqa: F401

import sys
import shutil


# FreeBSD-specific workarounds for pyfakefs 5.3.5 / CPython 3.12
# interactions. Neither of these reflects a real bug in cephadm; both
# are pinned-dependency quirks that only surface on FreeBSD.
def pytest_configure(config):
    if not sys.platform.startswith('freebsd'):
        return
    # pyfakefs 5.3.5's TemporaryFileCloser patching for Python 3.12
    # doesn't always match CPython's GC finalization order: __del__
    # can fire after pyfakefs has already unpatched for that test,
    # hitting an already-closed backing BytesIO. Harmless -- the
    # actual test outcome is unaffected -- but noisy. Silenced rather
    # than "fixed", since a real fix means patching pyfakefs's
    # tempfile finalizer internals.
    #
    # Registered via pytest's own filterwarnings ini mechanism rather
    # than a plain warnings.filterwarnings() call: pytest wraps every
    # test in warnings.catch_warnings() + simplefilter("always"),
    # which discards any filter added at plain import/conftest time
    # before the first test even runs. addinivalue_line() re-applies
    # the filter for every test item instead.
    config.addinivalue_line(
        'filterwarnings',
        'ignore:Exception ignored in.*_TemporaryFileCloser.*:'
        'pytest.PytestUnraisableExceptionWarning',
    )

