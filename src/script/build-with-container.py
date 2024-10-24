#!/usr/bin/python3
"""build-with-container.py - Build Ceph in a Containerized environment.

build-with-container.py is a self-contained python script meant to assist
with building and testing the Ceph Project source in a (OCI) container.

Benefits of building ceph in a container:
* you do not need ceph dependencies installed on your personal system(s)
* you can build for other distributions than the on you are running
* you can cache the image and save time downloading dependencies
* you can build for multiple different distros on the same build hardware
* you can make experiemental changes to the build scripts, dependency
  packages, compliers, etc. and test them before submitting the changes
  to a real CI job
* it's cool!

This script requires python3 and either podman or docker.

Currently we assume our audience is developers wishing to build and test ceph
locally. As such there are a number of predefined execution steps that loosely
map to a number of development tasks. You can specify one or more execution
steps with the `--execution/-e <step>` option. Other commonly needed options
may include:
* --distro/-d <distro> - Abbreviated name for base distribution & container image
* --build-dir/-b <dir> - Relative path to output directory for builds

For example:
  build-with-container.py -d centos9 -e build
  # the same as running build-with-container.py without any arguments

  Selects CentOS (stream) 9 as the base image and will automatically (if
  needed) build a new container image with the ceph dependencies. It will then
  run a configure and build step to compile the ceph sources.

Example 2:
  build-with-container.py -d ubuntu22.04 -e test -d _mybuild

  Selects Ubuntu 22.04 as the base image and then automatically (if needed)
  builds a container image with the ceph dependencies. It then will configure
  and make the "make test" dependencies and run the "make test" suite.

Example 3:
  build-with-container.py -d centos9 -e rpm

  Again, will build a new container image with dependencies if needed.
  Then it will create a source rpm and then build binary rpms from
  that source RPM.
"""

import argparse
import enum
import glob
import logging
import os
import pathlib
import shutil
import shlex
import subprocess

log = logging.getLogger()


DISTROS = [
    "ubuntu22.04",
    "centos9",
    "centos8",
]


def _cmdstr(cmd):
    return " ".join(shlex.quote(c) for c in cmd)


def _run(cmd, *args, **kwargs):
    log.info("Executing command: %s", _cmdstr(cmd))
    return subprocess.run(cmd, *args, **kwargs)


def _container_cmd(ctx, args):
    rm_container = not ctx.cli.keep_container
    cmd = [
        ctx.container_engine,
        "run",
        "--name=ceph_build",
    ]
    if rm_container:
        cmd.append("--rm")
    if "podman" in ctx.container_engine:
        cmd.append("--pids-limit=-1")
    if ctx.map_user:
        cmd.append("--user=0")
    cwd = pathlib.Path(".").absolute()
    cmd += [
        f"--volume={cwd}:{ctx.cli.homedir}:Z",
        f"-eHOMEDIR={ctx.cli.homedir}",
    ]
    if ctx.cli.build_dir:
        cmd.append(f"-eBUILD_DIR={ctx.cli.build_dir}")
    for extra_arg in ctx.cli.extra or []:
        cmd.append(extra_arg)
    cmd.append(ctx.image_name)
    cmd.extend(args)
    return cmd


def _git_command(ctx, args):
    cmd = ["git"]
    cmd.extend(args)
    return cmd


def _git_current_branch(ctx):
    cmd = _git_command(ctx, ["rev-parse", "--abbrev-ref", "HEAD"])
    res = _run(cmd, check=True, capture_output=True)
    return res.stdout.decode("utf8").strip()


def _git_current_sha(ctx, short=True):
    args = ["rev-parse"]
    if short:
        args.append("--short")
    args.append("HEAD")
    cmd = _git_command(ctx, args)
    res = _run(cmd, check=True, capture_output=True)
    return res.stdout.decode("utf8").strip()


class Steps(enum.StrEnum):
    DNF_CACHE = "dnfcache"
    BUILD_CONTAINER = "build-container"
    CONTAINER = "container"
    CONFIGURE = "configure"
    BUILD = "build"
    BUILD_TESTS = "buildtests"
    TESTS = "tests"
    FREE_FORM = "free_form"
    SOURCE_RPM = "source-rpm"
    RPM = "rpm"
    DEBS = "debs"


class ImageSource(enum.StrEnum):
    CACHE = "cache"
    PULL = "pull"
    BUILD = "build"

    @classmethod
    def argument(cls, value):
        try:
            return {cls(v) for v in value.split(",")}
        except Exception:
            raise argparse.ArgumentTypeError(
                f"the argument must be one of {cls.hint()}"
                " or a comma delimited list of those values"
            )

    @classmethod
    def hint(cls):
        return ", ".join(s.value for s in cls)


class Context:
    """Command context."""

    def __init__(self, cli):
        self.cli = cli
        self._engine = None
        self.distro_cache_name = ""

    @property
    def container_engine(self):
        if self._engine is not None:
            return self._engine
        if self.cli.container_engine:
            return cli.container_engine

        for ctr_eng in ["podman", "docker"]:
            if shutil.which(ctr_eng):
                break
        else:
            raise RuntimeError("no container engine found")
        log.debug("found container engine: %r", ctr_eng)
        self._engine = ctr_eng
        return self._engine

    @property
    def image_name(self):
        base = self.cli.image_repo or "ceph-build"
        return f"{base}:{self.target_tag()}"

    def target_tag(self):
        if self.cli.tag:
            return self.cli.tag
        try:
            branch = _git_current_branch(self).replace("/", "-")
        except subprocess.CalledProcessError:
            branch = "UNKNOWN"
        return f"{branch}.{self.cli.distro}"

    @property
    def from_image(self):
        return {
            "centos9": "quay.io/centos/centos:stream9",
            "centos8": "quay.io/centos/centos:stream8",
            "ubuntu22.04": "docker.io/ubuntu:22.04",
        }[self.cli.distro]

    @property
    def dnf_cache_dir(self):
        if self.cli.dnf_cache_path and self.distro_cache_name:
            return (
                pathlib.Path(self.cli.dnf_cache_path) / self.distro_cache_name
            )
        return None

    @property
    def map_user(self):
        # TODO: detect if uid mapping is needed
        return os.getuid() != 0


class Builder:
    """Organize and manage the build steps."""

    _steps = {}

    def __init__(self):
        self._did_steps = set()

    def wants(self, step, ctx, *, force=False, top=False):
        log.info("want to execute build step: %s", step)
        if ctx.cli.no_prereqs and not top:
            log.info("Running prerequisite steps disabled")
            return
        if step in self._did_steps:
            log.info("step already done: %s", step)
            return
        self._steps[step](ctx)
        self._did_steps.add(step)
        log.info("step done: %s", step)

    def available_steps(self):
        return [str(k) for k in self._steps]

    @classmethod
    def set(self, step):
        def wrap(f):
            self._steps[step] = f
            f._for_step = step
            return f

        return wrap


@Builder.set(Steps.DNF_CACHE)
def dnf_cache_dir(ctx):
    if ctx.cli.distro not in ["centos9"]:
        return
    if not ctx.cli.dnf_cache_path:
        return

    ctx.distro_cache_name = f"_ceph_{ctx.cli.distro}"
    cache_dir = ctx.dnf_cache_dir
    (cache_dir / "lib").mkdir(parents=True, exist_ok=True)
    (cache_dir / "cache").mkdir(parents=True, exist_ok=True)
    (cache_dir / ".DNF_CACHE").touch(exist_ok=True)


@Builder.set(Steps.BUILD_CONTAINER)
def build_container(ctx):
    ctx.build.wants(Steps.DNF_CACHE, ctx)
    cmd = [
        ctx.container_engine,
        "build",
        "-t",
        ctx.image_name,
        f"--build-arg=JENKINS_HOME={ctx.cli.homedir}",
    ]
    if ctx.cli.distro:
        cmd.append(f"--build-arg=DISTRO={ctx.from_image}")
    if ctx.dnf_cache_dir:
        cmd += [
            f"--volume={ctx.dnf_cache_dir}/lib:/var/lib/dnf:Z",
            f"--volume={ctx.dnf_cache_dir}:/var/cache/dnf:Z",
            "--build-arg=CLEAN_DNF=no",
        ]
    if ctx.cli.homedir:
        cwd = pathlib.Path(".").absolute()
        cmd.append(f"--volume={cwd}:{ctx.cli.homedir}:Z")
    cmd += ["-f", ctx.cli.containerfile, ctx.cli.containerdir]
    _run(cmd, check=True)


@Builder.set(Steps.CONTAINER)
def get_container(ctx):
    """Acquire an image that we will build in."""
    inspect_cmd = [
        ctx.container_engine,
        "image",
        "inspect",
        ctx.image_name,
    ]
    pull_cmd = [
        ctx.container_engine,
        "pull",
        ctx.image_name,
    ]
    allowed = ctx.cli.image_sources or ImageSource
    if ImageSource.CACHE in allowed:
        res = _run(inspect_cmd, check=False, capture_output=True)
        if res.returncode == 0:
            log.info("Container image %s present", ctx.image_name)
            return
        log.info("Container image %s not present", ctx.image_name)
    if ImageSource.PULL in allowed:
        res = _run(pull_cmd, check=False, capture_output=True)
        if res.returncode == 0:
            log.info("Container image %s pulled successfully", ctx.image_name)
            return
    log.info("Container image %s needed", ctx.image_name)
    if ImageSource.BUILD in allowed:
        ctx.build.wants(Steps.BUILD_CONTAINER, ctx)
        return
    raise ValueError("no available image sources")


@Builder.set(Steps.CONFIGURE)
def bc_configure(ctx):
    """Configure the build"""
    ctx.build.wants(Steps.CONTAINER, ctx)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"cd {ctx.cli.homedir} && source ./src/script/run-make.sh && has_build_dir || configure",
        ],
    )
    _run(cmd, check=True)


@Builder.set(Steps.BUILD)
def bc_build(ctx):
    """Execute a standard build."""
    ctx.build.wants(Steps.CONFIGURE, ctx)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"cd {ctx.cli.homedir} && source ./src/script/run-make.sh && build vstart",
        ],
    )
    _run(cmd, check=True)


@Builder.set(Steps.BUILD_TESTS)
def bc_build_tests(ctx):
    """Build the tests."""
    ctx.build.wants(Steps.CONFIGURE, ctx)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"cd {ctx.cli.homedir} && source ./src/script/run-make.sh && build tests",
        ],
    )
    _run(cmd, check=True)


@Builder.set(Steps.TESTS)
def bc_run_tests(ctx):
    """Execute the tests."""
    ctx.build.wants(Steps.BUILD_TESTS, ctx)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"cd {ctx.cli.homedir} && source ./run-make-check.sh && build && run",
        ],
    )
    _run(cmd, check=True)


@Builder.set(Steps.SOURCE_RPM)
def bc_make_source_rpm(ctx):
    """Build SPRMs."""
    ctx.build.wants(Steps.CONTAINER, ctx)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"cd {ctx.cli.homedir} && ./make-srpm.sh",
        ],
    )
    _run(cmd, check=True)


@Builder.set(Steps.RPM)
def bc_build_rpm(ctx):
    """Build RPMs from SRPM."""
    srpm_glob = "ceph*.src.rpm"
    if ctx.cli.rpm_match_sha:
        head_sha = _git_current_sha(ctx)
        srpm_glob = f"ceph*.g{head_sha}.*.src.rpm"
    paths = glob.glob(srpm_glob)
    if len(paths) > 1:
        raise RuntimeError(
            "too many matching source rpms"
            f" (rename or remove unwanted files matching {srpm_glob} in the"
            " ceph dir and try again)"
        )
    if not paths:
        # no matches. build a new srpm
        ctx.build.wants(Steps.SOURCE_RPM, ctx)
        paths = glob.glob(srpm_glob)
        assert paths
    srpm_path = pathlib.Path(ctx.cli.homedir) / paths[0]
    topdir = pathlib.Path(ctx.cli.homedir) / "rpmbuild"
    if ctx.cli.build_dir:
        topdir = (
            pathlib.Path(ctx.cli.homedir) / ctx.cli.build_dir / "rpmbuild"
        )
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"set -x; mkdir -p {topdir} && rpmbuild --rebuild -D'_topdir {topdir}' {srpm_path}",
        ],
    )
    _run(cmd, check=True)


@Builder.set(Steps.DEBS)
def bc_make_debs(ctx):
    """Build debian/ubuntu packages."""
    ctx.build.wants(Steps.CONTAINER, ctx)
    basedir = pathlib.Path(ctx.cli.homedir) / "debs"
    if ctx.cli.build_dir:
        basedir = (
            pathlib.Path(ctx.cli.homedir) / ctx.cli.build_dir
        )
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"mkdir -p {basedir} && cd {ctx.cli.homedir} && ./make-debs.sh {basedir}",
        ],
    )
    _run(cmd, check=True)


def parse_cli(build_step_names):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Emit debugging level logging",
    )
    parser.add_argument(
        "--container-engine",
        help="Select container engine to use (eg. podman, docker)",
    )
    parser.add_argument(
        "--cwd",
        help="Change working directory before executing commands",
    )
    parser.add_argument(
        "--distro",
        "-d",
        choices=DISTROS,
        default="centos9",
        help="Specify a distro short name",
    )
    parser.add_argument(
        "--tag",
        "-t",
        help="Specify a container tag",
    )
    parser.add_argument(
        "--image-repo",
        help="Specify a container image repository",
    )
    parser.add_argument(
        "--image-sources",
        "-I",
        type=ImageSource.argument,
        help="Specify a set of valid image sources. "
        f"May be a comma separated list of {ImageSource.hint()}",
    )
    parser.add_argument(
        "--homedir",
        default="/ceph",
        help="Container image home/build dir",
    )
    parser.add_argument(
        "--dnf-cache-path",
        help="DNF caching using provided base dir",
    )
    parser.add_argument(
        "--build-dir",
        "-b",
        help=(
            "Specify a build directory relative to the home dir"
            " (the ceph source root)"
        ),
    )
    parser.add_argument(
        "--extra",
        "-x",
        action="append",
        help="Specify an extra argument to pass to container command",
    )
    parser.add_argument(
        "--keep-container",
        action="store_true",
        help="Skip removing container after executing command",
    )
    parser.add_argument(
        "--containerfile",
        default="Dockerfile.build",
        help="Path to container file",
    )
    parser.add_argument(
        "--containerdir",
        default=".",
        help="Path to other container files",
    )
    parser.add_argument(
        "--no-prereqs",
        "-P",
        action="store_true",
        help="Do not execute any prerequisite steps. Only execute specified steps",
    )
    parser.add_argument(
        "--rpm-no-match-sha",
        dest="rpm_match_sha",
        action="store_false",
        help=(
            "Do not try to build RPM packages that match the SHA of the current"
            " git checkout. Use any source RPM available."
        ),
    )
    parser.add_argument(
        "--execute",
        "-e",
        dest="steps",
        action="append",
        choices=build_step_names,
        help="Execute the target build step(s)",
    )
    cli, rest = parser.parse_known_args()
    cli.remaining_args = rest
    return cli


def _src_root():
    return pathlib.Path(__file__).parent.parent.parent.absolute()


def _setup_logging(cli):
    level = logging.DEBUG if cli.debug else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("{asctime}: {levelname}: {message}", style="{")
    )
    handler.setLevel(level)
    logger.addHandler(handler)


def main():
    builder = Builder()
    cli = parse_cli(builder.available_steps())
    _setup_logging(cli)

    os.chdir(cli.cwd or _src_root())
    ctx = Context(cli)
    ctx.build = builder
    for step in cli.steps or [Steps.BUILD]:
        ctx.build.wants(step, ctx, top=True)


if __name__ == "__main__":
    main()
