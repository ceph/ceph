#!/usr/bin/env python3
"""build.py - Build, tag, and push Ceph container images from a Jinja2 template.

Renders Containerfile.j2 into a Containerfile, builds the container image,
tags it according to CI or release conventions, and optionally pushes it
to a container registry.

Environment variables (inputs):
  FROM_IMAGE              Base container image (default depends on BRANCH).
  CI_CONTAINER            'true' for CI builds; 'false' for release builds.
  FLAVOR                  OSD flavour: default | crimson-debug | crimson-release | debug.
  BRANCH                  Ceph branch name (default: current git branch).
  CEPH_SHA1               Ceph commit SHA1 (default: current git HEAD).
  ARCH                    Host architecture: x86_64 or arm64 (default: host arch).
  CEPH_GIT_REPO           Git repo URL (auto-detected from CI_CONTAINER).
  CONTAINER_REPO_HOSTNAME Registry hostname (default: quay.ceph.io).
  CONTAINER_REPO_ORGANIZATION  Registry organisation.
  CONTAINER_REPO          Registry repository name.
  CONTAINER_REPO_USERNAME Registry auth username.
  CONTAINER_REPO_PASSWORD Registry auth password.
  PRERELEASE_USERNAME     download.ceph.com credentials.
  PRERELEASE_PASSWORD     download.ceph.com credentials.
  REMOVE_LOCAL_IMAGES     Remove local images after push (default: 'true').
  NO_PUSH                 Skip push step when set to 'true'.
  VERSION                 Release version tag (for release builds; default: latest git tag).
  CONTAINER_ENGINE        'podman' or 'docker' (default: 'podman').
  CUSTOM_CEPH_REPO_URL    Optional custom ceph repo URL (overrides normal ceph repo setup).
  GANESHA_REPO_BASEURL    NFS-Ganesha repo baseurl for centos/almalinux images.
"""

import argparse
import datetime
import json
import logging
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

try:
    import jinja2
except ImportError:
    print(
        "Error: the 'jinja2' package is required.\n"
        "Install it with:  pip install jinja2",
        file=sys.stderr,
    )
    sys.exit(1)

log = logging.getLogger(__name__)

# Default NFS-Ganesha repo baseurl (contains DNF releasever/basearch variables).
_DEFAULT_GANESHA_REPO_BASEURL = (
    "https://buildlogs.centos.org/centos/"
    "$releasever-stream/storage/$basearch/nfsganesha-5/"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(name: str, default: str = "") -> str:
    """Return the value of an environment variable, or *default*."""
    return os.environ.get(name, default)


def _git(*args: str) -> str:
    """Run a git command and return stdout, or '' on error."""
    try:
        result = subprocess.run(
            ["git"] + list(args),
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return ""


def _run(cmd: list, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """Run *cmd*, logging the command first."""
    log.info("Running: %s", " ".join(str(c) for c in cmd))
    return subprocess.run(cmd, check=check, **kwargs)


# ---------------------------------------------------------------------------
# Distro detection
# ---------------------------------------------------------------------------

def detect_distro(from_image: str) -> dict:
    """Derive distro metadata from *from_image*.

    Returns a dict with keys:
      distro_id    - 'centos', 'rocky', or 'almalinux'
      distro_major - major version string, e.g. '9' or '10'
      el_ver       - e.g. 'el9' or 'el10'
      dist_path    - e.g. 'centos/9' or 'rocky/10'
    """
    # Strip registry prefix; take the last path component (image:tag)
    image_and_tag = from_image.split("/")[-1]
    if ":" in image_and_tag:
        image_name, tag = image_and_tag.rsplit(":", 1)
    else:
        image_name, tag = image_and_tag, "latest"

    name_lower = image_name.lower()
    if "centos" in name_lower:
        distro_id = "centos"
    elif "rocky" in name_lower:
        distro_id = "rocky"
    elif "alma" in name_lower:
        distro_id = "almalinux"
    else:
        log.warning(
            "Unknown distro in FROM_IMAGE=%s; defaulting to 'centos'", from_image
        )
        distro_id = "centos"

    # Extract the first run of digits from the tag (e.g. 'stream9' -> '9', '10' -> '10')
    m = re.search(r"(\d+)", tag)
    distro_major = m.group(1) if m else "9"

    return {
        "distro_id": distro_id,
        "distro_major": distro_major,
        "el_ver": f"el{distro_major}",
        "dist_path": f"{distro_id}/{distro_major}",
    }


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def render_template(template_path: str, context: dict) -> str:
    """Render the Jinja2 template at *template_path* with *context*."""
    tpl_dir = str(Path(template_path).parent)
    tpl_name = Path(template_path).name
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(tpl_dir),
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
    )
    return env.get_template(tpl_name).render(**context)


# ---------------------------------------------------------------------------
# Container build / inspect
# ---------------------------------------------------------------------------

def build_image(
    engine: str,
    containerfile: str,
    build_args: dict,
    secret_file: str,
    tag: str,
) -> None:
    """Build a container image."""
    cmd = [engine, "build"]
    if engine == "podman":
        cmd += ["--pull=newer", "--squash"]
    else:
        cmd += ["--pull"]
    cmd += ["-f", containerfile, "-t", tag]
    for key, value in build_args.items():
        if value:
            cmd += ["--build-arg", f"{key}={value}"]
    if secret_file:
        cmd += [f"--secret=id=prerelease_creds,src={secret_file}"]
    _run(cmd)


def get_image_id(engine: str, image_name: str) -> str:
    """Return the image ID for a locally built image."""
    if engine == "podman":
        fmt_name = f"localhost/{image_name}"
    else:
        fmt_name = image_name
    result = _run(
        [engine, "image", "ls", fmt_name, "--format", "{{.ID}}"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""


def inspect_image(engine: str, image_id: str) -> dict:
    """Return a metadata dict extracted from the built image.

    Keys include 'arch' (image architecture) and all env vars set by the
    Containerfile ENV instruction (e.g. 'FROM_IMAGE', 'CEPH_REF', etc.).
    """
    result = _run(
        [engine, "inspect", "--format", "{{json .}}", image_id],
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)
    metadata: dict = {"arch": data.get("Architecture", "")}
    for entry in data.get("Config", {}).get("Env", []):
        if "=" in entry:
            k, _, v = entry.partition("=")
            if k != "PATH":
                metadata[k] = v
    return metadata


# ---------------------------------------------------------------------------
# Tag computation
# ---------------------------------------------------------------------------

def compute_tags(
    repopath: str,
    branch: str,
    ceph_sha1: str,
    arch: str,
    flavor: str,
    from_image: str,
    ci_container: bool,
    version: str,
    builddate: str,
) -> list:
    """Return the list of remote tags to apply to the built image.

    CI build tag conventions:
      full_repo_tag   : <branch>-<fromtag>-<arch>-devel[-<flavor>]
      branch_repo_tag : <branch>[-<fromtag>][-<flavor>][-arm64]
      sha1_repo_tag   : <sha1>[-<fromtag>][-<flavor>][-arm64]

    For crimson-debug / crimson-release on x86_64, only a single
    sha1-flavour tag is produced (matching original build.sh behaviour).

    Release build tag convention:
      version_tag     : v<version>-<builddate>
    """
    # Derive fromtag from FROM_IMAGE, e.g. "rockylinux:10" -> "rockylinux-10"
    fromtag = from_image.split("/")[-1].replace(":", "-")

    if ci_container:
        # Determine the default fromtag for this branch.
        # reef / squid branches default to centos-stream9; everything else
        # (tentacle, main, …) defaults to rockylinux-10.
        if "reef" in branch or "squid" in branch:
            default_fromtag = "centos-stream9"
        else:
            default_fromtag = "rockylinux-10"

        # debug flavour gets a -debug suffix on all three tags.
        debug_suffix = f"-{flavor}" if flavor == "debug" else ""

        full_repo_tag = f"{repopath}:{branch}-{fromtag}-{arch}-devel{debug_suffix}"
        branch_repo_tag = f"{repopath}:{branch}{debug_suffix}"
        sha1_repo_tag = f"{repopath}:{ceph_sha1}{debug_suffix}"

        # When using a non-default fromtag, distinguish branch/sha1 tags.
        if fromtag != default_fromtag:
            branch_repo_tag = f"{repopath}:{branch}-{fromtag}{debug_suffix}"
            sha1_repo_tag = f"{repopath}:{ceph_sha1}-{fromtag}{debug_suffix}"

        # arm64 gets an extra suffix on branch/sha1 tags.
        if arch == "arm64":
            branch_repo_tag = f"{branch_repo_tag}-arm64"
            sha1_repo_tag = f"{sha1_repo_tag}-arm64"

        # For crimson flavours on x86_64, publish only the sha1-flavour tag.
        if flavor in ("crimson-debug", "crimson-release") and arch == "x86_64":
            sha1_flavor_tag = f"{repopath}:{ceph_sha1}-{flavor}"
            return [sha1_flavor_tag]

        return [full_repo_tag, branch_repo_tag, sha1_repo_tag]

    # Release build
    version_tag = f"{repopath}:v{version}-{builddate}"
    return [version_tag]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "template",
        nargs="?",
        default="Containerfile.j2",
        help="Path to Jinja2 template file (default: Containerfile.j2)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Only render the template; do not build the image",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    args = parse_args()

    # ------------------------------------------------------------------
    # Resolve environment variables
    # ------------------------------------------------------------------
    ci_container = _env("CI_CONTAINER", "false").lower() == "true"
    flavor = _env("FLAVOR", "default")
    branch = _env("BRANCH") or _git("rev-parse", "--abbrev-ref", "HEAD") or "main"
    ceph_sha1 = _env("CEPH_SHA1") or _git("rev-parse", "HEAD")
    arch = _env("ARCH") or platform.machine()
    if arch == "aarch64":
        arch = "arm64"

    remove_local_images = _env("REMOVE_LOCAL_IMAGES", "true").lower() == "true"
    no_push = _env("NO_PUSH", "").lower() == "true"
    container_engine = _env("CONTAINER_ENGINE", "podman")

    # Strip remote prefix from BRANCH (e.g. "origin/main" -> "main")
    branch = branch.split("/")[-1]

    # Registry settings vary between CI and release builds.
    repo_arch = "arm64" if arch == "arm64" else "amd64"
    if ci_container:
        container_repo_hostname = _env("CONTAINER_REPO_HOSTNAME", "quay.ceph.io")
        container_repo_org = _env("CONTAINER_REPO_ORGANIZATION", "ceph-ci")
        container_repo = _env("CONTAINER_REPO", "ceph")
        version = ""
    else:
        container_repo_hostname = _env("CONTAINER_REPO_HOSTNAME", "quay.ceph.io")
        container_repo_org = _env("CONTAINER_REPO_ORGANIZATION", "ceph")
        container_repo = _env("CONTAINER_REPO", f"prerelease-{repo_arch}")
        version = _env("VERSION") or _git("describe", "--abbrev=0")

    container_repo_username = _env("CONTAINER_REPO_USERNAME")
    container_repo_password = _env("CONTAINER_REPO_PASSWORD")
    prerelease_username = _env("PRERELEASE_USERNAME")
    prerelease_password = _env("PRERELEASE_PASSWORD")

    # FROM_IMAGE default depends on branch:
    #   reef / squid  -> centos stream9
    #   tentacle / main / … -> rocky linux 10
    if "reef" in branch or "squid" in branch:
        default_from_image = "quay.io/centos/centos:stream9"
    else:
        default_from_image = "quay.io/rockylinux/rockylinux:10"
    from_image = _env("FROM_IMAGE", default_from_image)

    ganesha_repo_baseurl = _env("GANESHA_REPO_BASEURL", _DEFAULT_GANESHA_REPO_BASEURL)

    ceph_git_repo = _env("CEPH_GIT_REPO")
    if not ceph_git_repo:
        ceph_git_repo = (
            "https://github.com/ceph/ceph-ci.git"
            if ci_container
            else "https://github.com/ceph/ceph.git"
        )

    custom_ceph_repo_url = _env("CUSTOM_CEPH_REPO_URL")

    # ------------------------------------------------------------------
    # Validate required variables
    # ------------------------------------------------------------------
    errors = []
    if not no_push:
        for var, val in [
            ("CONTAINER_REPO_HOSTNAME", container_repo_hostname),
            ("CONTAINER_REPO_ORGANIZATION", container_repo_org),
            ("CONTAINER_REPO_USERNAME", container_repo_username),
            ("CONTAINER_REPO_PASSWORD", container_repo_password),
        ]:
            if not val:
                errors.append(var)
    if not ci_container and not version:
        errors.append("VERSION")
    if errors:
        log.error("Missing required environment variables: %s", ", ".join(errors))
        sys.exit(1)

    repopath = f"{container_repo_hostname}/{container_repo_org}/{container_repo}"

    # ------------------------------------------------------------------
    # Registry auth check (push a minimal test image)
    # ------------------------------------------------------------------
    if not no_push:
        minimal_image = f"{repopath}:minimal-test"
        log.info("Checking registry auth against %s", repopath)
        _run([container_engine, "rmi", minimal_image], check=False)
        _run(
            [container_engine, "build", "-f", "-", "-t", minimal_image],
            input="FROM scratch\n",
            text=True,
        )
        result = _run([container_engine, "push", minimal_image], check=False)
        if result.returncode != 0:
            log.error(
                "Not authenticated to %s; run 'podman login' / 'docker login' first.",
                repopath,
            )
            _run([container_engine, "rmi", minimal_image], check=False)
            sys.exit(1)
        _run([container_engine, "rmi", minimal_image], check=False)

    # ------------------------------------------------------------------
    # Distro detection & template rendering
    # ------------------------------------------------------------------
    distro = detect_distro(from_image)
    log.info(
        "FROM_IMAGE=%s  distro_id=%s  distro_major=%s  el_ver=%s  dist_path=%s",
        from_image,
        distro["distro_id"],
        distro["distro_major"],
        distro["el_ver"],
        distro["dist_path"],
    )

    # Escape '$' in the ganesha baseurl so Dockerfile ARG syntax treats it as
    # a literal dollar sign (DNF releasever/basearch variables).
    ganesha_repo_baseurl_for_arg = ganesha_repo_baseurl.replace("$", r"\$")

    context = {
        "from_image": from_image,
        "ganesha_repo_baseurl": ganesha_repo_baseurl,
        "ganesha_repo_baseurl_for_arg": ganesha_repo_baseurl_for_arg,
        "osd_flavor": flavor,
        "ci_container": "true" if ci_container else "false",
        "custom_ceph_repo_url": custom_ceph_repo_url,
        **distro,
    }

    # Resolve template path relative to this script when not absolute.
    template_path = args.template
    if not os.path.isabs(template_path):
        script_dir = Path(__file__).parent
        template_path = str(script_dir / template_path)

    log.info("Rendering template: %s", template_path)
    containerfile_content = render_template(template_path, context)

    containerfile_path = str(Path(template_path).parent / "Containerfile")
    with open(containerfile_path, "w") as fh:
        fh.write(containerfile_content)
    log.info("Rendered Containerfile written to: %s", containerfile_path)

    if args.no_build:
        log.info("--no-build specified; stopping after template render.")
        return

    # ------------------------------------------------------------------
    # Write prerelease credentials secret file
    # Use os.open() to atomically create the file with restricted permissions
    # (mode 0o600) so credentials are never readable by other users.
    # ------------------------------------------------------------------
    secret_file = str(Path(template_path).parent / "prerelease.secret.txt")
    fd = os.open(secret_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(f"PRERELEASE_USERNAME={prerelease_username}\n")
        fh.write(f"PRERELEASE_PASSWORD={prerelease_password}\n")

    build_local_tag = "build.py.output"
    try:
        # ------------------------------------------------------------------
        # Build
        # ------------------------------------------------------------------
        build_args = {
            "FROM_IMAGE": from_image,
            "CEPH_SHA1": ceph_sha1,
            "CEPH_GIT_REPO": ceph_git_repo,
            "CEPH_REF": branch,
            "OSD_FLAVOR": flavor,
            "CI_CONTAINER": "true" if ci_container else "false",
            "GANESHA_REPO_BASEURL": ganesha_repo_baseurl,
            "CUSTOM_CEPH_REPO_URL": custom_ceph_repo_url,
        }
        build_image(
            container_engine,
            containerfile_path,
            build_args,
            secret_file,
            build_local_tag,
        )

        # ------------------------------------------------------------------
        # Inspect the built image
        # ------------------------------------------------------------------
        image_id = get_image_id(container_engine, build_local_tag)
        if not image_id:
            log.error("Could not determine image ID for %s", build_local_tag)
            sys.exit(1)
        log.info("Built image ID: %s", image_id)

        metadata = inspect_image(container_engine, image_id)
        log.info("Image metadata: %s", metadata)

        # Use FROM_IMAGE from the container's ENV (set by the Containerfile)
        # to compute the fromtag; fall back to the value we passed in.
        image_from = metadata.get("FROM_IMAGE", from_image)
        image_arch = metadata.get("arch", arch)

        # ------------------------------------------------------------------
        # Compute and apply tags
        # ------------------------------------------------------------------
        builddate = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
        tags = compute_tags(
            repopath,
            branch,
            ceph_sha1,
            image_arch,
            flavor,
            image_from,
            ci_container,
            version,
            builddate,
        )

        for tag in tags:
            _run([container_engine, "tag", image_id, tag])

        # ------------------------------------------------------------------
        # Push
        # ------------------------------------------------------------------
        if not no_push:
            for tag in tags:
                _run([container_engine, "push", tag])
            if remove_local_images:
                _run([container_engine, "rmi", "-f"] + tags, check=False)

    finally:
        # Always remove the secret file
        try:
            os.remove(secret_file)
        except OSError:
            pass


if __name__ == "__main__":
    main()
