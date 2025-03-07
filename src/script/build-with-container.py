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
  build-with-container.py -d ubuntu22.04 -e test -b build.ub2204

  Selects Ubuntu 22.04 as the base image and then automatically (if needed)
  builds a container image with the ceph dependencies. It then will configure
  and make the "make test" dependencies and run the "make test" suite.

Example 3:
  build-with-container.py -d centos9 -e rpm

  Again, will build a new container image with dependencies if needed.
  Then it will create a source rpm and then build binary rpms from
  that source RPM.

Example 4:
  build-with-container.py -d ubuntu24.04 -e debs

  If needed, build a new container image with ubuntu24.04 and ceph dependencies.
  Then build ceph deb packages.

Example 5:
  $EDITOR ./my-cool-script.sh && chmod +x ./my-cool-script.sh
  build-with-container.py -d centos9 -b build.hacks -e build -e custom -- /ceph/my-cool-script.sh

  If needed, build a new container image with centos 9 stream and ceph dependencies.
  Then build ceph sources. Then run a custom script from the ceph source dir
  in the container.


The command comes with built-in help. Specify the --help option to print
general command help. Specify the --help-build-steps option to list all
the executable build steps with short descriptions of what they do.
"""

import argparse
import contextlib
import enum
import glob
import logging
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import sys

log = logging.getLogger()


try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, enum.Enum):
        def __str__(self):
            return self.value


class DistroKind(StrEnum):
    CENTOS10 = "centos10"
    CENTOS8 = "centos8"
    CENTOS9 = "centos9"
    FEDORA41 = "fedora41"
    UBUNTU2204 = "ubuntu22.04"
    UBUNTU2404 = "ubuntu24.04"

    @classmethod
    def uses_dnf(cls):
        return {cls.CENTOS8, cls.CENTOS9, cls.CENTOS10, cls.FEDORA41}

    @classmethod
    def uses_rpmbuild(cls):
        # right now this is the same as uses_dnf, but perhaps not always
        # let's be specific in our interface
        return cls.uses_dnf()  # but lazy in the implementation

    @classmethod
    def aliases(cls):
        return {
            str(cls.CENTOS10): cls.CENTOS10,
            "centos10stream": cls.CENTOS10,
            str(cls.CENTOS8): cls.CENTOS8,
            str(cls.CENTOS9): cls.CENTOS9,
            "centos9stream": cls.CENTOS9,
            str(cls.FEDORA41): cls.FEDORA41,
            "fc41": cls.FEDORA41,
            str(cls.UBUNTU2204): cls.UBUNTU2204,
            "ubuntu-jammy": cls.UBUNTU2204,
            "jammy": cls.UBUNTU2204,
            str(cls.UBUNTU2404): cls.UBUNTU2404,
            "ubuntu-noble": cls.UBUNTU2404,
            "noble": cls.UBUNTU2404,
        }

    @classmethod
    def from_alias(cls, value):
        return cls.aliases()[value]


class DefaultImage(StrEnum):
    CENTOS10 = "quay.io/centos/centos:stream10"
    CENTOS8 = "quay.io/centos/centos:stream8"
    CENTOS9 = "quay.io/centos/centos:stream9"
    FEDORA41 = "registry.fedoraproject.org/fedora:41"
    UBUNTU2204 = "docker.io/ubuntu:22.04"
    UBUNTU2404 = "docker.io/ubuntu:24.04"


class CommandFailed(Exception):
    pass


class DidNotExecute(Exception):
    pass


def _cmdstr(cmd):
    return " ".join(shlex.quote(c) for c in cmd)


def _run(cmd, *args, **kwargs):
    ctx = kwargs.pop("ctx", None)
    if ctx and ctx.dry_run:
        log.info("(dry-run) Not Executing command: %s", _cmdstr(cmd))
        # because we can not return a result (as we did nothing)
        # raise a specific exception to be caught by higher layer
        raise DidNotExecute(cmd)

    log.info("Executing command: %s", _cmdstr(cmd))
    return subprocess.run(cmd, *args, **kwargs)


def _container_cmd(ctx, args, *, workdir=None, interactive=False):
    rm_container = not ctx.cli.keep_container
    cmd = [
        ctx.container_engine,
        "run",
        "--name=ceph_build",
    ]
    if interactive:
        cmd.append("-it")
    if rm_container:
        cmd.append("--rm")
    if "podman" in ctx.container_engine:
        cmd.append("--pids-limit=-1")
    if ctx.map_user:
        cmd.append("--user=0")
    if ctx.cli.env_file:
        cmd.append(f"--env-file={ctx.cli.env_file.absolute()}")
    if workdir:
        cmd.append(f"--workdir={workdir}")
    cwd = pathlib.Path(".").absolute()
    overlay = ctx.overlay()
    if overlay and overlay.temporary:
        cmd.append(f"--volume={cwd}:{ctx.cli.homedir}:O")
    elif overlay:
        cmd.append(
            f"--volume={cwd}:{ctx.cli.homedir}:O,upperdir={overlay.upper},workdir={overlay.work}"
        )
    else:
        cmd.append(f"--volume={cwd}:{ctx.cli.homedir}:Z")
    cmd.append(f"-eHOMEDIR={ctx.cli.homedir}")
    if ctx.cli.build_dir:
        cmd.append(f"-eBUILD_DIR={ctx.cli.build_dir}")
    if ctx.cli.ccache_dir:
        ccdir = str(ctx.cli.ccache_dir).format(
            homedir=ctx.cli.homedir or "",
            build_dir=ctx.cli.build_dir or "",
            distro=ctx.cli.distro or "",
        )
        cmd.append(f"-eCCACHE_DIR={ccdir}")
        cmd.append(f"-eCCACHE_BASEDIR={ctx.cli.homedir}")
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


class Steps(StrEnum):
    DNF_CACHE = "dnfcache"
    BUILD_CONTAINER = "build-container"
    CONTAINER = "container"
    CONFIGURE = "configure"
    BUILD = "build"
    BUILD_TESTS = "buildtests"
    TESTS = "tests"
    CUSTOM = "custom"
    SOURCE_RPM = "source-rpm"
    RPM = "rpm"
    DEBS = "debs"
    PACKAGES = "packages"
    INTERACTIVE = "interactive"


class ImageSource(StrEnum):
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
            return self.cli.container_engine

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
        suffix = ""
        if self.cli.tag and self.cli.tag.startswith("+"):
            suffix = f".{self.cli.tag[1:]}"
        elif self.cli.tag:
            return self.cli.tag
        branch = self.cli.current_branch
        if not branch:
            try:
                branch = _git_current_branch(self).replace("/", "-")
            except subprocess.CalledProcessError:
                branch = "UNKNOWN"
        return f"{branch}.{self.cli.distro}{suffix}"

    def base_branch(self):
        # because git truly is the *stupid* content tracker there's not a
        # simple way to detect base branch. In BWC the base branch is really
        # only here for an optional 2nd level of customization in the build
        # container bootstrap we default to `main` even when that's not true.
        # One can explicltly set the base branch on the command line to invoke
        # customizations (that don't yet exist) or invalidate image caching.
        return self.cli.base_branch or "main"

    @property
    def from_image(self):
        if self.cli.base_image:
            return self.cli.base_image
        distro_images = {
            fld.value: getattr(DefaultImage, fld.name).value
            for fld in DistroKind
        }
        return distro_images[self.cli.distro]

    @property
    def dnf_cache_dir(self):
        if self.cli.dnf_cache_path and self.distro_cache_name:
            path = (
                pathlib.Path(self.cli.dnf_cache_path) / self.distro_cache_name
            )
            path = path.expanduser()
            return path.resolve()
        return None

    @property
    def map_user(self):
        # TODO: detect if uid mapping is needed
        return os.getuid() != 0

    @property
    def dry_run(self):
        return self.cli.dry_run

    @contextlib.contextmanager
    def user_command(self):
        """Handle subprocess execptions raised by commands we expect to be fallible.
        Helps hide traceback noise when just running commands.
        """
        try:
            yield
        except subprocess.SubprocessError as err:
            if self.cli.debug:
                raise
            raise CommandFailed() from err
        except DidNotExecute:
            pass

    def overlay(self):
        if not self.cli.overlay_dir:
            return None
        overlay = Overlay(temporary=self.cli.overlay_dir == "-")
        if not overlay.temporary:
            obase = pathlib.Path(self.cli.overlay_dir).resolve()
            # you can't nest the workdir inside the upperdir at least on the
            # version of podman I tried. But the workdir does need to be on the
            # same FS according to the docs.  So make the workdir and the upper
            # dir (content) siblings within the specified dir. podman doesn't
            # have the courtesy to manage the workdir automatically when
            # specifying upper dir.
            overlay.upper = obase / "content"
            overlay.work = obase / "work"
        return overlay


class Overlay:
    def __init__(self, temporary=True, upper=None, work=None):
        self.temporary = temporary
        self.upper = upper
        self.work = work


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
        if not self._did_steps:
            prepare_env_once(ctx)
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

    @classmethod
    def docs(cls):
        for step, func in cls._steps.items():
            yield str(step), getattr(func, "__doc__", "")


def prepare_env_once(ctx):
    overlay = ctx.overlay()
    if overlay and not overlay.temporary:
        log.info("Creating overlay dirs: %s, %s", overlay.upper, overlay.work)
        overlay.upper.mkdir(parents=True, exist_ok=True)
        overlay.work.mkdir(parents=True, exist_ok=True)


@Builder.set(Steps.DNF_CACHE)
def dnf_cache_dir(ctx):
    """Set up a DNF cache directory for reuse across container builds."""
    if ctx.cli.distro not in DistroKind.uses_dnf():
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
    """Generate a build environment container image."""
    ctx.build.wants(Steps.DNF_CACHE, ctx)
    cmd = [
        ctx.container_engine,
        "build",
        "--pull",
        "-t",
        ctx.image_name,
        f"--build-arg=JENKINS_HOME={ctx.cli.homedir}",
        f"--build-arg=CEPH_BASE_BRANCH={ctx.base_branch()}",
    ]
    if ctx.cli.distro:
        cmd.append(f"--build-arg=DISTRO={ctx.from_image}")
    if ctx.dnf_cache_dir and "docker" in ctx.container_engine:
        log.warning(
            "The --volume option is not supported by docker. Skipping dnf cache dir mounts"
        )
    elif ctx.dnf_cache_dir:
        cmd += [
            f"--volume={ctx.dnf_cache_dir}/lib:/var/lib/dnf:Z",
            f"--volume={ctx.dnf_cache_dir}:/var/cache/dnf:Z",
            "--build-arg=CLEAN_DNF=no",
        ]
    cmd += ["-f", ctx.cli.containerfile, ctx.cli.containerdir]
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.CONTAINER)
def get_container(ctx):
    """Build or fetch a container image that we will build in."""
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
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


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
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


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
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.TESTS)
def bc_run_tests(ctx):
    """Execute the tests."""
    ctx.build.wants(Steps.BUILD_TESTS, ctx)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"cd {ctx.cli.homedir} && source ./run-make-check.sh && build tests && run",
        ],
    )
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.SOURCE_RPM)
def bc_make_source_rpm(ctx):
    """Build SPRMs."""
    ctx.build.wants(Steps.CONTAINER, ctx)
    make_srpm_cmd = f"cd {ctx.cli.homedir} && ./make-srpm.sh"
    if ctx.cli.ceph_version:
        make_srpm_cmd = f"{make_srpm_cmd} {ctx.cli.ceph_version}"
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            make_srpm_cmd,
        ],
    )
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.RPM)
def bc_build_rpm(ctx):
    """Build RPMs from SRPM."""
    srpm_glob = "ceph*.src.rpm"
    if ctx.cli.rpm_match_sha:
        if not ctx.cli.ceph_version:
            head_sha = _git_current_sha(ctx)
            srpm_glob = f"ceph*.g{head_sha}.*.src.rpm"
        else:
            # Given a tarball with a name like
            #   ceph-19.3.0-7462-g565e5c65.tar.bz2
            # The SRPM name would be:
            #   ceph-19.3.0-7462.g565e5c65.el9.src.rpm
            # This regex replaces the second '-' with a '.'
            srpm_version = re.sub(
                r"(\d+\.\d+\.\d+-\d+)-(.*)",
                r"\1.\2",
                ctx.cli.ceph_version
            )
            srpm_glob = f"ceph-{srpm_version}.*.src.rpm"
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
    rpmbuild_args = [
        'rpmbuild',
        '--rebuild',
        f'-D_topdir {topdir}',
    ] + list(ctx.cli.rpmbuild_arg) + [str(srpm_path)]
    rpmbuild_cmd = ' '.join(shlex.quote(cmd) for cmd in rpmbuild_args)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"set -x; mkdir -p {topdir} && {rpmbuild_cmd}",
        ],
    )
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.DEBS)
def bc_make_debs(ctx):
    """Build debian/ubuntu packages."""
    ctx.build.wants(Steps.CONTAINER, ctx)
    basedir = pathlib.Path(ctx.cli.homedir) / "debs"
    if ctx.cli.build_dir:
        basedir = pathlib.Path(ctx.cli.homedir) / ctx.cli.build_dir
    make_debs_cmd = f"./make-debs.sh {basedir}"
    if ctx.cli.ceph_version:
        make_debs_cmd = f"{make_debs_cmd} {ctx.cli.ceph_version} {ctx.cli.distro}"
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            f"mkdir -p {basedir} && cd {ctx.cli.homedir} && {make_debs_cmd}",
        ],
    )
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.PACKAGES)
def bc_make_packages(ctx):
    """Build some sort of distro packages - chooses target based on distro."""
    if ctx.cli.distro in DistroKind.uses_rpmbuild():
        ctx.build.wants(Steps.RPM, ctx)
    else:
        ctx.build.wants(Steps.DEBS, ctx)


@Builder.set(Steps.CUSTOM)
def bc_custom(ctx):
    """Run a custom build command."""
    ctx.build.wants(Steps.CONTAINER, ctx)
    if not ctx.cli.remaining_args:
        raise RuntimeError(
            "no command line arguments provided:"
            " specify command after '--' on the command line"
        )
    cc = " ".join(ctx.cli.remaining_args)
    log.info("Custom command: %r", cc)
    cmd = _container_cmd(
        ctx,
        [
            "bash",
            "-c",
            cc,
        ],
        workdir=ctx.cli.homedir,
    )
    with ctx.user_command():
        _run(cmd, check=True, ctx=ctx)


@Builder.set(Steps.INTERACTIVE)
def bc_interactive(ctx):
    """Start an interactive shell in the build container."""
    ctx.build.wants(Steps.CONTAINER, ctx)
    cmd = _container_cmd(
        ctx,
        [],
        workdir=ctx.cli.homedir,
        interactive=True,
    )
    with ctx.user_command():
        _run(cmd, check=False, ctx=ctx)


class ArgumentParser(argparse.ArgumentParser):
    def parse_my_args(self, args=None, namespace=None):
        """Parse argument up to the '--' term and then stop parsing.
        Returns a tuple of the parsed args and then remaining args.
        """
        args = sys.argv[1:] if args is None else list(args)
        if "--" in args:
            idx = args.index("--")
            my_args, rest = args[:idx], args[idx + 1 :]
        else:
            my_args, rest = args, []
        return self.parse_args(my_args, namespace=namespace), rest


def parse_cli(build_step_names):
    parser = ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Emit debugging level logging and tracebacks",
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
        choices=DistroKind.aliases().keys(),
        type=DistroKind.from_alias,
        default=str(DistroKind.CENTOS9),
        help="Specify a distro short name",
    )
    parser.add_argument(
        "--tag",
        "-t",
        help="Specify a container tag. Append to the auto generated tag"
        " by prefixing the supplied value with the plus (+) character",
    )
    parser.add_argument(
        "--base-branch",
        help="Specify a base branch name",
    )
    parser.add_argument(
        "--current-branch",
        help="Manually specify the current branch name",
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
        "--base-image",
        help=(
            "Supply a custom base image to use instead of the default"
            " image for the source distro."
        ),
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
        "--overlay-dir",
        "-l",
        help=(
            "Mount the homedir as an overlay volume using the given dir"
            "to host the overlay content and working dir. Specify '-' to"
            "use a temporary overlay (discarding writes on container exit)"
        ),
    )
    parser.add_argument(
        "--ccache-dir",
        help=(
            "Specify a directory (within the container) to save ccache"
            " output"
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
        help="Specify the path to a (build) container file",
    )
    parser.add_argument(
        "--containerdir",
        default=".",
        help="Specify the path to container context dir",
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
        "--rpmbuild-arg",
        '-R',
        action="append",
        help="Pass this extra argument to rpmbuild",
    )
    parser.add_argument(
        "--ceph-version",
        help="Rather than infer the Ceph version, use this value",
    )
    parser.add_argument(
        "--execute",
        "-e",
        dest="steps",
        action="append",
        choices=build_step_names,
        help="Execute the target build step(s)",
    )
    parser.add_argument(
        "--env-file",
        type=pathlib.Path,
        help="Use this environment file when building",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not execute key commands, print and continue if possible",
    )
    parser.add_argument(
        "--help-build-steps",
        action="store_true",
        help="Print executable build steps and brief descriptions",
    )
    cli, rest = parser.parse_my_args()
    if cli.help_build_steps:
        print("Executable Build Steps")
        print("======================")
        print("")

        for step_name, doc in sorted(Builder().docs()):
            print(step_name)
            print(" " * 5, doc)
            print("")
        sys.exit(0)
    if rest and rest[0] == "--":
        rest[:] = rest[1:]
    cli.remaining_args = rest
    return cli


def _src_root():
    return pathlib.Path(__file__).parent.parent.parent.absolute()


class ColorFormatter(logging.Formatter):
    _yellow = "\x1b[33;20m"
    _red = "\x1b[31;20m"
    _reset = "\x1b[0m"

    def format(self, record):
        res = super().format(record)
        if record.levelno == logging.WARNING:
            res = self._yellow + res + self._reset
        if record.levelno == logging.ERROR:
            res = self._red + res + self._reset
        return res


def _setup_logging(cli):
    level = logging.DEBUG if cli.debug else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = logging.StreamHandler()
    fmt = "{asctime}: {levelname}: {message}"
    if sys.stdout.isatty() and sys.stderr.isatty():
        formatter = ColorFormatter(fmt, style="{")
    else:
        formatter = logging.Formatter(fmt, style="{")
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger.addHandler(handler)


def main():
    builder = Builder()
    cli = parse_cli(builder.available_steps())
    _setup_logging(cli)

    os.chdir(cli.cwd or _src_root())
    ctx = Context(cli)
    ctx.build = builder
    try:
        for step in cli.steps or [Steps.BUILD]:
            ctx.build.wants(step, ctx, top=True)
    except CommandFailed as err:
        err_cause = getattr(err, "__cause__", None)
        if err_cause:
            log.error("Command failed: %s", err_cause)
        else:
            log.error("Command failed!")
        log.warning(
            "🚧 the command may have faild due to circumstances"
            " beyond the influence of this build script. For example: a"
            " complier error caused by a source code change."
            " Pay careful attention to the output generated by the command"
            " before reporting this as a problem with the"
            " build-with-container.py script. 🚧"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
