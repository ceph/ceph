#!/usr/bin/python3
"""Build cephadm from one or more files into a standalone executable.
"""
# TODO: If cephadm is being built and packaged within a format such as RPM
# do we have to do anything special wrt passing in the version
# of python to build with? Even with the intermediate cmake layer?

import argparse
import logging
import os
import pathlib
import shutil
import subprocess
import tempfile
import sys

HAS_ZIPAPP = False
try:
    import zipapp

    HAS_ZIPAPP = True
except ImportError:
    pass


log = logging.getLogger(__name__)


def _reexec(python):
    """Switch to the selected version of python by exec'ing into the desired
    python path.
    Sets the _BUILD_PYTHON_SET env variable as a sentinel to indicate exec has
    been performed.
    """
    env = os.environ.copy()
    env["_BUILD_PYTHON_SET"] = python
    os.execvpe(python, [python, __file__] + sys.argv[1:], env)


def _did_rexec():
    """Returns true if the process has already exec'ed into the desired python
    version.
    """
    return bool(os.environ.get("_BUILD_PYTHON_SET", ""))


def _build(dest, src):
    """Build the binary."""
    os.chdir(src)
    tempdir = pathlib.Path(tempfile.mkdtemp(suffix=".cephadm.build"))
    log.debug("working in %s", tempdir)
    try:
        if os.path.isfile("requirements.txt"):
            _install_deps(tempdir)
        log.info("Copying contents")
        # TODO: currently the only file relevant to a compiled cephadm is the
        # cephadm.py file. Once cephadm is broken up into multiple py files
        # (and possibly other libs from python-common, etc) we'll want some
        # sort organized structure to track what gets copied into the
        # dir to be zipped. For now we just have a simple call to copy
        # (and rename) the one file we care about.
        shutil.copy("cephadm.py", tempdir / "__main__.py")
        _compile(dest, tempdir)
    finally:
        shutil.rmtree(tempdir)


def _compile(dest, tempdir):
    """Compile the zipapp."""
    # TODO we could explicitly pass a python version here
    log.info("Constructing the zipapp file")
    try:
        zipapp.create_archive(
            source=tempdir,
            target=dest,
            interpreter=sys.executable,
            compressed=True,
        )
        log.info("Zipapp created with compression")
    except TypeError:
        # automatically fall back to uncompressed
        zipapp.create_archive(
            source=tempdir,
            target=dest,
            interpreter=sys.executable,
        )
        log.info("Zipapp created without compression")


def _install_deps(tempdir):
    """Install dependencies with pip."""
    # TODO we could explicitly pass a python version here
    log.info("Installing dependencies")
    # apparently pip doesn't have an API, just a cli.
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--requirement",
            "requirements.txt",
            "--target",
            tempdir,
        ]
    )


def main():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("cephadm/build.py: %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)

    log.debug("argv: %r", sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "dest", help="Destination path name for new cephadm binary"
    )
    parser.add_argument(
        "--source", help="Directory containing cephadm sources"
    )
    parser.add_argument(
        "--python", help="The path to the desired version of python"
    )
    args = parser.parse_args()

    if not _did_rexec() and args.python:
        _reexec(args.python)

    log.info(
        "Python Version: {v.major}.{v.minor}.{v.micro}".format(
            v=sys.version_info
        )
    )
    log.info("Args: %s", vars(args))
    if not HAS_ZIPAPP:
        # Unconditionally display an error that the version of python
        # lacks zipapp (probably too old).
        print("error: zipapp module not found", file=sys.stderr)
        print(
            "(zipapp is available in Python 3.5 or later."
            " are you using a new enough version?)",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.source:
        source = pathlib.Path(args.source).absolute()
    else:
        source = pathlib.Path(__file__).absolute().parent
    dest = pathlib.Path(args.dest).absolute()
    log.info("Source Dir: %s", source)
    log.info("Destination Path: %s", dest)
    _build(dest, source)


if __name__ == "__main__":
    main()
