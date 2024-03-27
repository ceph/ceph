#!/usr/bin/python
"""Patch Ceph Container images with development (work-in-progress) build
artifacts and sources.

"""

from functools import lru_cache
import argparse
import enum
import logging
import logging.config
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time

log = logging.getLogger()


BASE_IMAGE = "quay.ceph.io/ceph-ci/ceph:main"
DEST_IMAGE = "ceph/ceph:wip"

# Set CLEANUP to false to skip cleaning up temporary files for debugging.
CLEANUP = True
CATCH_ERRORS = True


class Component(enum.Enum):
    """Component to be copied into the new container image."""

    PY_MGR = ("mgr", "python", False)
    PY_COMMON = ("common", "python", False)
    CEPHADM = ("cephadm", "python", False)
    PYBIND = ("bindings", "python", True)
    DASHBOARD = ("dashboard", "dashboard", True)
    CORE = ("core", "core", True)
    CEPHFS = ("cephfs", "core", True)
    RBD = ("rbd", "core", True)
    RGW = ("rgw", "rgw", True)

    def __init__(self, name, group, needs_compilation=True):
        self.component_name = name
        self.component_group = group
        self.needs_compilation = needs_compilation

    def __str__(self):
        return f"{self.component_group}/{self.component_name}"


def _exclude_dirs(dirs, excludes):
    """Exclude directories from a fs walk."""
    for exclude in excludes:
        try:
            del dirs[dirs.index(exclude)]
        except ValueError:
            pass


def _run(cmd, *args, **kwargs):
    """Wrapper for subprocess.run with additional logging"""
    log.debug("running command: %r", cmd)
    result = subprocess.run(cmd, *args, **kwargs)
    log.debug("command exited with returncode=%d", result.returncode)
    return result


def check_build_dir(cli_ctx):
    """Raise an error if the build dir is not populated by a build."""
    log.debug("checking if build dir is OK")
    bdir = cli_ctx.build_dir
    makefile = bdir / "Makefile"
    ninja = bdir / "build.ninja"
    if not (makefile.is_file() or ninja.is_file()):
        log.error(f"neither {makefile} nor {ninja} found")
        raise ValueError("invalid build dir")
    return


def pull_base_image(cli_ctx):
    """Pull the base image."""
    cmd = [cli_ctx.engine, "pull", cli_ctx.base_image]
    if cli_ctx.root_build:
        cmd.insert(0, "sudo")
    _run(cmd).check_returncode()


def push_image(cli_ctx):
    """Push the target image. Assumes login already performed."""
    cmd = [cli_ctx.engine, "push", cli_ctx.target]
    if cli_ctx.root_build:
        cmd.insert(0, "sudo")
    _run(cmd).check_returncode()


class ChangeDir:
    """Context manager for temporarily changing directory."""

    def __init__(self, path):
        self.path = path
        self.orig = None

    def __enter__(self):
        self.orig = pathlib.Path.cwd().absolute()
        os.chdir(self.path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        os.chdir(self.orig)


class AddComponentsAction(argparse.Action):
    """Helper for adding a component to the CLI."""

    def __init__(
        self,
        option_strings,
        dest,
        const=None,
        default=False,
        required=False,
        help=None,
    ):
        super().__init__(
            option_strings=option_strings,
            dest=dest,
            const=const,
            nargs=0,
            default=default,
            required=required,
            help=help,
        )

    def __call__(self, parser, namespace, values, option_string):
        destination = getattr(namespace, self.dest, None) or []
        try:
            comps = list(self.const)
        except TypeError:
            comps = [self.const]
        for comp in comps:
            if comp not in destination:
                destination.append(comp)
        setattr(namespace, self.dest, destination)

    @classmethod
    def add_argument(cls, parser, long_opt, components, hint):
        parser.add_argument(
            long_opt,
            dest="components",
            action=cls,
            const=components,
            help=f"Includes: {hint}",
        )


class CLIContext:
    """Manages state related to the CLI."""

    def __init__(self, cli_ns):
        self._cli = cli_ns

    @property
    def needs_build_dir(self):
        return any(c.needs_compilation for c in self.build_components())

    @property
    @lru_cache()
    def engine(self):
        for ctr_eng in ["podman", "docker"]:
            if shutil.which(ctr_eng):
                break
        else:
            raise ValueError("no container engine found")
        log.debug("found container engine: %r", ctr_eng)
        return pathlib.Path(ctr_eng)

    @property
    def base_image(self):
        return self._cli.base

    @property
    def target(self):
        return self._cli.target

    @property
    def root_build(self):
        return self._cli.root_build

    @property
    def build_dir(self):
        if self._cli.build_dir:
            return pathlib.Path(self._cli.build_dir).absolute()
        return pathlib.Path(".").absolute()

    @property
    def source_dir(self):
        if self._cli.source_dir:
            return pathlib.Path(self._cli.source_dir).absolute()
        return (self.build_dir / "..").absolute()

    @property
    def push(self):
        return self._cli.push

    @property
    def pull(self):
        return self._cli.pull

    @property
    def strip_binaries(self):
        return self._cli.strip

    @property
    def components_selected(self):
        return bool(self._cli.components)

    @property
    def cephadm_build_args(self):
        return list(self._cli.cephadm_build_arg or [])

    @property
    def run_before_commands(self):
        return list(self._cli.run_before or [])

    def build_components(self):
        if self._cli.components:
            return self._cli.components
        # because everything is nothing.
        return list(Component)

    def setup_logging(self):
        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": True,
                "formatters": {
                    "default": {
                        "class": "logging.Formatter",
                        "format": "%(asctime)s %(levelname)s: %(message)s",
                    },
                },
                "handlers": {
                    "stderr": {
                        "formatter": "default",
                        "level": "DEBUG",
                        "class": "logging.StreamHandler",
                        "stream": sys.stderr,
                    }
                },
                "root": {
                    "level": self._cli.log_level,
                    "handlers": ["stderr"],
                },
            }
        )

    @classmethod
    def parse(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--base",
            default=BASE_IMAGE,
            help=f"base container image [default: {BASE_IMAGE}]",
        )
        parser.add_argument(
            "--target",
            default=DEST_IMAGE,
            help=f"target image to create [default: {DEST_IMAGE}]",
        )
        parser.add_argument(
            "--push", action="store_true", help="push when done"
        )
        parser.add_argument(
            "--pull",
            action="store_true",
            help=(
                "pull base image before building (useful to prevent building"
                " with a stale base image)"
            ),
        )
        parser.add_argument(
            "--build-dir",
            "-b",
            help="path to the build dir (defaults to current workding dir)",
        )
        parser.add_argument(
            "--source-dir",
            help="path to the source dir (defaults to parent of build dir)",
        )
        parser.add_argument(
            "--strip", action="store_true", help="strip binaries"
        )
        parser.add_argument(
            "--root-build", action="store_true", help="build image as root"
        )
        parser.add_argument("--container-engine")
        parser.set_defaults(log_level=logging.INFO)
        parser.add_argument(
            "--debug",
            action="store_const",
            dest="log_level",
            const=logging.DEBUG,
            help="Enable debug level logging",
        )
        parser.add_argument(
            "--quiet",
            "-q",
            action="store_const",
            dest="log_level",
            const=logging.WARNING,
            help="Only print errors and warnings",
        )
        parser.add_argument(
            "--cephadm-build-arg",
            "-A",
            action="append",
            help="Pass additional arguments to cephadm build script.",
        )
        parser.add_argument(
            "--run-before",
            action="append",
            help="Add a RUN command before other actions"
        )
        # selectors
        component_selections = [
            # aggregated components:
            (
                # aggregates all python components
                "--py",
                "python",
                [
                    Component.CEPHADM,
                    Component.PY_MGR,
                    Component.PY_COMMON,
                    Component.PYBIND,
                ],
            ),
            (
                # aggregates python components that don't need to compile
                "--pure-py",
                "non-compiled python",
                [
                    Component.CEPHADM,
                    Component.PY_MGR,
                    Component.PY_COMMON,
                ],
            ),
            # currently no single component selectors for rbd & cephfs
            # this is the way it was in the shell script too.
            (
                "--core",
                "mon, mgr, osd, mds, bins and libraries",
                [
                    Component.CORE,
                    Component.RBD,
                    Component.CEPHFS,
                ],
            ),
            # single components:
            (
                "--python-common",
                "common python libs",
                [
                    Component.PY_COMMON,
                ],
            ),
            (
                "--python-bindings",
                "compiled python bindings",
                [
                    Component.PYBIND,
                ],
            ),
            (
                "--python-mgr-modules",
                "mgr modules written in python",
                [
                    Component.PY_MGR,
                ],
            ),
            (
                "--cephadm",
                "cephadm command",
                [
                    Component.CEPHADM,
                ],
            ),
            (
                "--dashboard",
                "dashboard",
                [
                    Component.DASHBOARD,
                ],
            ),
            (
                "--rgw",
                "radosgw, radosgw-admin",
                [
                    Component.RGW,
                ],
            ),
        ]
        for long_opt, hint, comps in component_selections:
            AddComponentsAction.add_argument(
                parser,
                long_opt,
                components=comps,
                hint=hint,
            )

        return cls(parser.parse_args())


class Builder:
    """Builds an image based on the components added."""

    def __init__(self, cli_ctx):
        self._ctx = cli_ctx
        self._jobs = []
        self._include_dashboard = False
        self._cached_py_site_packages = None
        self._workdir = pathlib.Path(
            tempfile.mkdtemp(suffix=".cpatch", dir=self._ctx.build_dir)
        )
        log.debug("cpatch working dir: %s", self._workdir)

    def add(self, component):
        """Request that the given component be added to the build."""
        log.info("Including: %s", component)
        dispatch = {
            Component.PY_COMMON: self._py_common_job,
            Component.CEPHADM: self._cephadm_job,
            Component.PY_MGR: self._py_mgr_job,
            Component.DASHBOARD: self._py_mgr_job,
            Component.PYBIND: self._pybind_job,
            Component.CORE: self._core_job,
            Component.CEPHFS: self._cephfs_job,
            Component.RBD: self._rbd_job,
            Component.RGW: self._rgw_job,
        }
        job = dispatch[component]
        if component == Component.DASHBOARD:
            self._include_dashboard = True
        if job not in {j for _, j in self._jobs}:
            self._jobs.append((component, job))

    def build(self):
        """Build the container image."""
        dlines = [f"FROM {self._ctx.base_image}"]
        for cmd in self._ctx.run_before_commands:
            dlines.append(f'RUN {cmd}')
        jcount = len(self._jobs)
        for idx, (component, job) in enumerate(self._jobs):
            num = idx + 1
            log.info(f"Executing job {num}/{jcount} for {component}")
            dresult = job(component)
            dlines.extend(dresult)

        with open(self._workdir / "Dockerfile", "w") as fout:
            for line in dlines:
                print(line, file=fout)
        self._container_build()

    def _container_build(self):
        log.info("Building container image")
        cmd = [self._ctx.engine, "build", "--tag", self._ctx.target, "."]
        cmd.append('--net=host')
        if self._ctx.root_build:
            cmd.insert(0, "sudo")
        log.debug("Container build command: %r", cmd)
        _run(cmd, cwd=self._workdir).check_returncode()

    def _build_tar(
        self,
        tar,
        start_path=".",
        exclude_dirs=None,
        exclude_file_suffixes=None,
    ):

        for cur, dirs, files in os.walk(start_path):
            cur = pathlib.Path(cur)
            # skip 'tests' directories in both tar and walk
            if exclude_dirs:
                _exclude_dirs(dirs, exclude_dirs)
            for sdir in dirs:
                ddir = cur / sdir
                tar.add(ddir, recursive=False)
            for sfile in files:
                if exclude_file_suffixes and sfile.endswith(
                    exclude_file_suffixes
                ):
                    continue
                dfile = cur / sfile
                tar.add(dfile, recursive=False)

    def _copy_binary(self, src_path, dst_path):
        log.debug("binary: %s -> %s", src_path, dst_path)
        if self._ctx.strip_binaries:
            log.debug("copy and strip: %s", dst_path)
            shutil.copy2(src_path, dst_path)
            _run(["strip", str(dst_path)]).check_returncode()
            return
        log.debug("hard linking: %s", dst_path)
        try:
            os.unlink(dst_path)
        except FileNotFoundError:
            pass
        os.link(src_path, dst_path)

    def _bins_and_libs(self, prefix, bin_patterns, lib_patterns):
        out = []

        bin_src = self._ctx.build_dir / "bin"
        bin_dst = self._workdir / f"{prefix}_bin"
        bin_dst.mkdir(parents=True, exist_ok=True)
        for path in bin_src.iterdir():
            if any(path.match(m) for m in bin_patterns):
                self._copy_binary(path, bin_dst / path.name)
        out.append(f"ADD {prefix}_bin /usr/bin")

        lib_src = self._ctx.build_dir / "lib"
        lib_dst = self._workdir / f"{prefix}_lib"
        lib_dst.mkdir(parents=True, exist_ok=True)
        for path in lib_src.iterdir():
            if any(path.match(m) for m in lib_patterns):
                self._copy_binary(path, lib_dst / path.name)
        out.append(f"ADD {prefix}_lib /usr/lib64")

        return out

    def _conditional_libs(self, src_dir, name, destination, lib_patterns):
        lib_src = self._ctx.build_dir / src_dir
        lib_dst = self._workdir / name
        lib_dst.mkdir(parents=True, exist_ok=True)
        try:
            for path in lib_src.iterdir():
                if any(path.match(m) for m in lib_patterns):
                    self._copy_binary(path, lib_dst / path.name)
        except FileNotFoundError as err:
            log.warning("skipping lib %s: %s", name, err)
        return f"ADD {name} {destination}"

    def _py_site_packages(self):
        """Return the correct python site packages dir for the image."""
        if self._cached_py_site_packages is not None:
            return self._cached_py_site_packages
        # use the container image to probe for the correct python site-packages dir
        valid_site_packages = [
            "/usr/lib/python3.8/site-packages",
            "/usr/lib/python3.6/site-packages",
        ]
        cmd = [
            self._ctx.engine,
            "run",
            "--rm",
            self._ctx.base_image,
            "ls",
            "-d",
        ]
        cmd += valid_site_packages
        if self._ctx.root_build:
            cmd.insert(0, "sudo")
        result = _run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        log.debug(f"container output: {result.stdout!r}")
        for line in result.stdout.decode("utf8").splitlines():
            pdir = line.strip()
            if line.strip() in valid_site_packages:
                log.debug(f"found site packages dir: {pdir}")
                self._cached_py_site_packages = pdir
                return self._cached_py_site_packages
        raise ValueError(
            "no valid python site-packages dir found in container"
        )

    def _py_mgr_job(self, component):
        name = "mgr_plugins.tar"
        if self._include_dashboard:
            log.debug("Including dashboard with mgr")
            exclude_dirs = ("tests",)
        else:
            log.debug("Excluding dashboard from mgr")
            exclude_dirs = ("tests", "node_modules")
        exclude_file_suffixes = (".pyc", ".pyo", ".tmp", "~")
        with tarfile.open(self._workdir / name, mode="w") as tar:
            with ChangeDir(self._ctx.source_dir / "src/pybind/mgr"):
                self._build_tar(
                    tar,
                    exclude_dirs=exclude_dirs,
                    exclude_file_suffixes=exclude_file_suffixes,
                )
        return [f"ADD {name} /usr/share/ceph/mgr"]

    def _py_common_job(self, component):
        name = "python_common.tar"
        exclude_dirs = ("tests", "node_modules")
        exclude_file_suffixes = (".pyc", ".pyo", ".tmp", "~")
        with tarfile.open(self._workdir / name, mode="w") as tar:
            with ChangeDir(self._ctx.source_dir / "src/python-common"):
                self._build_tar(
                    tar,
                    exclude_dirs=exclude_dirs,
                    exclude_file_suffixes=exclude_file_suffixes,
                )
        return [f"ADD {name} {self._py_site_packages()}"]

    def _cephadm_job(self, component):
        src_path = self._ctx.source_dir / "src/cephadm/cephadm"
        dst_path = self._workdir / "cephadm"
        if src_path.is_file():
            # traditional, uncompiled cephadm
            log.debug(f"found single-source file cephadm: {src_path}")
            os.link(src_path, dst_path)
        else:
            build_cephadm_path = self._ctx.source_dir / "src/cephadm/build.py"
            if not build_cephadm_path.is_file():
                raise ValueError("no cephadm build script found")
            log.debug("found cephadm compilation script: compiling cephadm")
            build_cmd = [build_cephadm_path] + self._ctx.cephadm_build_args
            build_cmd += [dst_path]
            _run(build_cmd).check_returncode()
        return ["ADD cephadm /usr/sbin/cephadm"]

    def _pybind_job(self, component):
        sodir = self._ctx.build_dir / "lib/cython_modules/lib.3"
        dst = self._workdir / "cythonlib"
        dst.mkdir(parents=True, exist_ok=True)
        for pth in sodir.iterdir():
            if pth.match("*.cpython-3*.so"):
                self._copy_binary(pth, dst / pth.name)
        return [f"ADD cythonlib {self._py_site_packages()}"]

    def _core_job(self, component):
        # [Quoth the original script]:
        # binaries are annoying because the ceph version is embedded all over
        # the place, so we have to include everything but the kitchen sink.
        out = []

        out.extend(
            self._bins_and_libs(
                prefix="core",
                bin_patterns=["ceph-mgr", "ceph-mon", "ceph-osd", "rados"],
                lib_patterns=["libceph-common.so*", "librados.so*"],
            )
        )

        out.append(
            self._conditional_libs(
                src_dir="lib",
                name="eclib",
                destination="/usr/lib64/ceph/erasure-code",
                lib_patterns=["libec_*.so*"],
            )
        )
        out.append(
            self._conditional_libs(
                src_dir="lib",
                name="clslib",
                destination="/usr/lib64/rados-classes",
                lib_patterns=["libcls_*.so*"],
            )
        )

        # [Quoth the original script]:
        # by default locally built binaries assume /usr/local
        out.append(
            "RUN rm -rf /usr/local/lib64 && ln -s /usr/lib64 /usr/local && ln -s /usr/share/ceph /usr/local/share"
        )

        return out

    def _rgw_job(self, component):
        return self._bins_and_libs(
            prefix="rgw",
            bin_patterns=["radosgw", "radosgw-admin"],
            lib_patterns=["libradosgw.so*"],
        )
        return out

    def _cephfs_job(self, component):
        return self._bins_and_libs(
            prefix="cephfs",
            bin_patterns=["ceph-mds"],
            lib_patterns=["libcephfs.so*"],
        )
        return out

    def _rbd_job(self, component):
        return self._bins_and_libs(
            prefix="rbd",
            bin_patterns=["rbd", "rbd-mirror"],
            lib_patterns=["librbd.so*"],
        )
        return out

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if CLEANUP:
            shutil.rmtree(self._workdir)
        return


def clean_errors(f):
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as err:
            if not CATCH_ERRORS:
                raise
            print("ERROR: {}".format(err))
            sys.exit(1)
        except subprocess.CalledProcessError:
            if not CATCH_ERRORS:
                raise
            print("ERROR: command failed: {}".format(err))
            sys.exit(1)

    return wrapped


@clean_errors
def main():
    cli_ctx = CLIContext.parse()
    cli_ctx.setup_logging()
    if not cli_ctx.components_selected:
        log.warning(
            "consider --py, --core, and/or --rgw for an abbreviated (faster) build."
        )
        time.sleep(2)

    if cli_ctx.needs_build_dir:
        check_build_dir(cli_ctx)

    if cli_ctx.pull:
        pull_base_image(cli_ctx)
    with Builder(cli_ctx) as builder:
        for component in cli_ctx.build_components():
            builder.add(component)
        builder.build()

    if cli_ctx.push:
        push_image(cli_ctx)


if __name__ == "__main__":
    main()
