import os
import pytest
import sys

skipif_teuthology_process = pytest.mark.skipif(
    os.path.basename(sys.argv[0]) == "teuthology",
    reason="Skipped because this test cannot pass when run in a teuthology " \
        "process (as opposed to py.test)"
)