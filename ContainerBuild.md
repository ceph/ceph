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
