# Build Ceph within a Container

The Ceph project includes a script and additional files that help build
the ceph.git sources inside an OCI-style container. This script requires
Python 3 (tested with 3.8 or later) and Podman or Docker.

The script aims to make it simple for a developer or anyone
wanting to compile Ceph on Linux but not wanting to dedicate a full
physical or virtual host to the job. The containers encapsulate the
build dependencies and environment and are quick and easy to clean
up when you no longer need them.

# build-with-containers.py Introduction

The script exists within the ceph.git tree at
`src/script/build-with-container.py`. The script operates on the entire Ceph
repository and will automatically try to detect the root of the ceph.git tree.
At any time the `--help` option can be passed to the script for a complete
listing of command line options.

To start with the default options run `./src/script/build-with-container.py`.
The script will first check if a build image already exists, if not it
will construct one using the `Dockerfile.build` file in the Ceph tree [1].

Once a build container is available the script will build Ceph from source using
a build directory named `build` in the root of the source tree. The default
environment uses a base distribution of `centos9`.

You can select the base distribution to use with the `--distro`/`-d` option.
The list of available distribution bases include `ubuntu22.04` and `centos9`.
These choices are known to work well. Other more experimental choices,
including `ubuntu24.04` and `centos10`, are available as well but do not expect
these platforms to work without tinkering. This option is a shorthand for
specifying the base container image that will be used to construct the build
image, the tags used for that image, as well as helping determine what kind of
packages can be produced using that build image.

You can specify where build artifacts are written with the `--build-dir`/`-b`
option. For example `./src/script/build-with-container.py -b build.try1`.

The tool supports mutliple build targets. Often these targets are chained
together - for example almost all targets depend on the container target. To
select a target use the `--execute`/`-e` command line option. For example:
`./src/script/build-with-container.py -e tests` will execute the unit tests
after building them. (Note: the `--no-prereqs` option exists to disable the
chaining but this should not be needed in most circumstances)


[1] - This behavior can be customized with the `--image-sources`/`-I`
option.


## Examples:

Build from source code on CentOS 9, with a build directory named for the base
distribution:
```
./src/script/build-with-container.py -d centos9 -b build.centos9 -e build
```

Build from source code on Ubuntu 22.04, with a build directory named for the
base distribution:
```
./src/script/build-with-container.py -d ubuntu22.04 -b build.u2204 -e build
```

Build RPM packages on Centos 9:
```
./src/script/build-with-container.py -d centos9 -e packages
```

Build RPM packages with experimental RGW cuObject support on Rocky Linux 10:
```
./src/script/build-with-container.py -d rocky10 -e source-rpm -e rpm
```

Use `-d rocky9` for Rocky Linux 9:
```
./src/script/build-with-container.py -d rocky9 -e source-rpm -e rpm
```

cuObject support is experimental. Source and RPM builds include it by default;
runtime RDMA activation requires an explicit opt-in. See the
[S3 RDMA documentation](doc/radosgw/s3rdma.rst) for its status and limitations.
With the default `WITH_RADOSGW_CUOBJ_SHIM=ON`, the NVIDIA cuObject SDK is not
needed to build:
the cuObject server library is loaded at runtime if it is installed on the
RGW host, and S3 RDMA requests fail with HTTP 500 when it is not.

To link directly to cuObject and install its NVIDIA server SDK in the build
container, add `--without-rgw-cuobj-shim`:
```
./src/script/build-with-container.py -d rocky10 --without-rgw-cuobj-shim \
  -e source-rpm -e rpm
```
Use `-d rocky9` for EL9. This option enables cuObject support, sets
`WITH_RADOSGW_CUOBJ_SHIM=OFF` for source builds, and passes
`--with=cuobj --without=cuobj_shim` to RPM builds. It adds NVIDIA's matching
RHEL9 or RHEL10 repository inside the build image and installs
`libcuobjserver-devel` and its dependencies. Default builds do not add the
NVIDIA repository or install its packages.

Automatic SDK installation supports Rocky Linux, AlmaLinux, RHEL, and CentOS
Stream 9 and 10 on x86_64 and aarch64. Fedora is not supported for this option:
NVIDIA's Fedora repository does not provide `libcuobjserver-devel`. The
installer reports an error on unsupported distributions or architectures;
Fedora can still use the default shim build.

The same Rocky container commands can be run on AlmaLinux or RHEL hosts.
To select an AlmaLinux container instead, use the matching EL major version
and override the base image, for example:
```
./src/script/build-with-container.py -d rocky9 \
  --base-image=docker.io/library/almalinux:9 --tag=+alma \
  -e source-rpm -e rpm
```
Use `rocky10` and `almalinux:10` together for EL10. For a RHEL 10 container
on a subscribed RHEL host using Red Hat's Podman, run:
```
./src/script/build-with-container.py --container-engine=podman \
  -d rocky10 --base-image=registry.access.redhat.com/ubi10/ubi:latest \
  --tag=+rhel10 -e source-rpm -e rpm
```
For RHEL 9, use `rocky9`, `ubi9/ubi:latest` and `--tag=+rhel9` together.
The RHEL setup enables the full BaseOS, AppStream and CodeReady Builder
repositories and installs EPEL. EL10 also uses Ceph's lab-extras repository.
The full RHEL repositories must be available through the host subscription;
the public UBI repositories alone do not contain all Ceph build dependencies.
See Red Hat's
[container repository documentation](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/building_running_and_managing_containers/assembly_adding-software-to-a-ubi-container_building-running-and-managing-containers).
There are no separate `alma9`/`rhel9` distro selectors in this script.

The explicit `source-rpm` step regenerates the SRPM with the updated spec;
`-e rpm` alone may reuse an existing SRPM. Commit source and CMake changes
first: `make-dist` archives `HEAD` when creating the source tarball. Once the
SRPM contains these changes,
`./src/script/build-with-container.py -d rocky10 -e rpm` is enough.
For direct `rpmbuild` use, run `rpmbuild --rebuild ceph-<version>.src.rpm`
after installing the build dependencies. To omit cuObject support, add
`--without cuobj`, or pass `--rpmbuild-arg=--without=cuobj` to the container
build script. For direct cuObject linking, install the SDK and use
`rpmbuild --rebuild --without cuobj_shim ceph-<version>.src.rpm`. The container
option `--without-rgw-cuobj-shim` also handles SDK installation.

Images requested with `--without-rgw-cuobj-shim` have a `.cuobj-direct` suffix
in their automatically generated tags. The image cache check also compares
the shim setting when using an explicit tag. Source builds with `-e build` or
`-e configure` include cuObject support by CMake's default. The
`--without-rgw-cuobj-shim` option also reconfigures an existing source build
directory so that the requested mode takes effect. To switch back to the
shim, use a fresh build directory or set
`WITH_RADOSGW_CUOBJ_SHIM=ON` in the existing CMake cache.

`CONFIGURE_ARGS` applies to source builds, not RPM builds. To pass it from the
host into a source build container, use `--extra=-eCONFIGURE_ARGS` (or put it in
an `--env-file`). For RPM builds, the spec's `cuobj` and `cuobj_shim`
conditionals control support and linking; `CONFIGURE_ARGS` does not override
them.

The same direct-link option works for source builds:
```
./src/script/build-with-container.py -d rocky10 --without-rgw-cuobj-shim \
  -b build.cuobj-direct -e build
```
Direct linking requires the cuObject server library on the deployed RGW host,
even when RDMA is disabled. See the
[S3 RDMA build instructions](doc/radosgw/s3rdma.rst) for both modes and for
using `WITH_RADOSGW_CUOBJ_SHIM` with CMake directly.

Build Debian packages on Ubuntu 22.04 (Jammy):
```
./src/script/build-with-container.py -d ubuntu22.04 -e packages
```

## Common Targets
* build - Build Ceph sources. Compiles C/C++ code, etc
* tests - Execute unit tests. Runs the unit test suite, depends on `buildtests` to compile some of the test suites
* custom - Execute a custom command. See description below
* packages - Build Ceph packages of the selected distribution's native package type
* interactive - Start the build container in an interactive mode

### Custom Commands

The `custom` target can be used to run a single command that the script is not
already programmed to handle. The custom command must come after a `--` to terminate
the normal command line arguments for `build-with-container.py`. For example:
```
./src/script/build-with-container.py -d ubuntu22.04 -e custom -- shellcheck src/script/buildcontainer-setup.sh
```

### Interactive mode

The `interactive` target can be used to run a shell within the container. This
is handy for those times you want to run multiple commands, by hand, within the
container environment.
As an example:
```
./src/script/build-with-container.py -d ubuntu22.04 -e interactive
```


## Additional Features

### Control build image name and tags

The `build-with-container.py` script automatically generates images names based
on a standard name and auto-generated tag. By default the script names the
image `ceph-build` and assumes the images are local only - the image name will
not refer to any image registry. The images are tagged with the name of the
current branch and base distribution. For example, assuming we're on a branch
named `wip-test` and we execute the script with `-d ubuntu22.04` we will expect
or build an image named `ceph-build:wip-test.ubuntu22.04`.

The image name/repository can be customized using the `--image-repo` option.
The tag can be overridden by the `--tag` option or extended by using the
`--tag` option with a plus (+) character at the start of the value. For
example: `./src/script/build-with-container.py
--image-repo=quay.io/example/build-example --tag=foobar` would create or reuse
an image named `quay.io/example/build-example:foobar`.
`./src/script/build-with-container.py
--image-repo=quay.io/example/build-example --distro=centos9 --tag=+foobar`
would use an image named
`quay.io/example/build-example:wip-test.centos9.foobar`.

If one wants to override the name of the branch or the branch can not be
automatically detected the `--current-branch` option can be supplied to
customize this value.


### Control build image source

By default `build-with-container.py` will try reuse build images if they are
cached in the local container store. If the image is not present it will build
one. In addition to these default actions the script can be intructed to "pull"
an image from a remote registry, possibly avoiding the need to build an image.

How the build image is acquired can be controlled using the `--image-sources`
option.  The option takes a comma-separated list of terms. The terms are
`cache`, `pull`, and `build`:
* build - Create a new build image
* cache - Check for an existing image in local container storage
* pull - Pull an image form a container image registry

So for example if you did not want to fall back to building an image locally,
you could pass `--image-sources=cache,pull` to the script. Passing
`--image-sources=build` will force the script to rebuild an image even if would
be available elsewhere.

When an image is to be built the image base is typically derived from the name
of the distribution being used. However, the base image can be overridden on
the command line using the `--base-image` option. For example, if one had a
local registry with a CentOS 9 (Stream) base image the following example could
be used: `./src/script/build-with-container.py -d centos9 --base-image
myreg.example.com/ceph/centos-base:9`


### Customizing the build image

Sometimes a downstream consumer of `build-with-container.py` needs to install
or compile extra dependencies into the build image without carrying a local
patch on top of `buildcontainer-setup.sh` or `Dockerfile.build`. The
`--custom-image-build-command` option runs an arbitrary shell command while
the build image is being constructed, after the normal distro package setup:

```
./src/script/build-with-container.py -d centos9 --custom-image-build-command 'curl -sSL https://example.com/setup.sh | bash' -e build
```

The option may be given multiple times; commands run in the order given. Each
value is passed to `bash -c` as-is - there is no special directory or file
convention involved, so how (or whether) any referenced content gets into the
image is entirely up to the command itself (fetch it over the network, invoke
something already installed by a package manager, etc).


### Controlling where files are written

By default the directory holding the Ceph source tree is mounted at `/ceph`
within the container (controlled by the `--homedir` option). Various build tasks
will write files to this directory. In some cases it's useful to keep the
directory free from changes and so the `--overlay-dir` option can be used to
make that volume use an overlay.

The overlay directory will be automatically created if needed and will contain
a `content` directory for new or updated files and a `work` directory, a
special directory needed by the overlayfs. For example:
`./src/script/build-with-container.py --overlay-dir build.ovr -b build.inner -d
centos9` will end up creating a directory `build.ovr/content/build.inner` which
will contain the results of the compile that would typically appear in just
`build.inner`. Other writes that would have normally effected the source tree
will appear in `build.ovr/content`

The overlay can also be temporary, with no files persisted after the container
has exited. Pass `--overlay-dir=-` to enable this option. Note that invoking
`build-with-container.py` default targets may use multiple container instances
and passing this option will break those targets.

By default, the source bind mount uses `:Z` to let the container engine relabel
the tree for SELinux. If the tree contains files that cannot be relabeled,
such as admin sockets owned by another user under `build/asok`, Podman can fail
before running the build with an `lsetxattr ... operation not permitted` error.
Use `--no-selinux-label` to disable SELinux separation for the runtime build
containers and omit relabeling of the source and npm cache bind mounts:

```
./src/script/build-with-container.py -d rocky10 \
  --no-selinux-label -e source-rpm -e rpm
```


### Authenticating GitHub dependency fetches

Ceph builds may fetch dependencies from GitHub during CMake configuration or
other build steps (for example, via CMake's `FetchContent` or
`ExternalProject_Add`). When many builds share the same public IP, these
unauthenticated Git-over-HTTPS requests can be rate limited by GitHub.

To avoid this, provide a GitHub Personal Access Token (PAT) through the
`GITHUB_TOKEN` environment variable:

```
GITHUB_TOKEN="$MY_GITHUB_PAT" ./src/script/build-with-container.py -e build
```

The token can also be supplied via the `--env-file` option instead of the
process environment:

```
# in the env file
GITHUB_TOKEN=ghp_...
```

```
./src/script/build-with-container.py --env-file=my.env -e build
```

There is intentionally no command line option that accepts the token value
directly (e.g. `--github-token=...`). Putting a secret on the command line
exposes it through shell history, process listings, and command logging, so
always use one of the two mechanisms above.

When a token is available, `build-with-container.py` configures Git *inside
the runtime build container* to authenticate HTTPS requests to
`github.com`, using `x-access-token` as the username, per GitHub's documented
convention for token-based HTTPS authentication. This is applied to every
runtime container the script starts (build, tests, packages, custom commands,
and interactive mode).

This feature only authenticates Git-over-HTTPS fetches to `github.com` (the
operations CMake dependency fetches rely on). It does **not**:
* authenticate arbitrary `curl` requests,
* authenticate calls to the GitHub API,
* authenticate pulls from a container image registry (including GitHub
  Container Registry).

The token is never used when building the build *image* itself (the
`Dockerfile.build`-based image build only ever uses `--build-arg`, and the
token is deliberately never passed there, since build arguments can leak
into image metadata and build history).

Only `GITHUB_TOKEN` is recognized (not `GH_TOKEN`), consistent with the
variable name already used throughout this repository's own GitHub Actions
workflows.

We recommend using a minimally scoped, read-only PAT (for example, a
fine-grained token with only public repository read access) for this
purpose, and treating it like any other credential.
