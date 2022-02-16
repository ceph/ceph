import pytest
import shutil
import subprocess


def promtool_available() -> bool:
    return shutil.which('promtool') is not None


def call(cmd):
    completion = subprocess.run(cmd.split(), stdout=subprocess.PIPE)
    return completion
