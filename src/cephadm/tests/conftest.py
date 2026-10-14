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
