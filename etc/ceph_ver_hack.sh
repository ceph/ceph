# Don't run this manually.  It's only here to be invoked inside the
# git checkout during `osc service dr`.  Here's why:
#
# - cmake, when it runs to set up the build system, in turn invokes
#   git to set CEPH_GIT_VER and CEPH_GIT_NICE_VER (src/CMakeLists.txt
#   line 237+).
# - Those two variables are substituted into src/ceph_ver.h.in.cmake,
#   generating build/src/include/ceph_ver.h.
# - When cmake is run inside OBS, it's working off a tarball, so
#   there's no git repo information, so CEPH_GIT_VER ends up set to
#   GITDIR-NOTFOUND, which breaks the build.
# - cmake *really* wants to be run from a git source checkout (at
#   least insofar as getting the version stuff set up goes).
#
# This script runs (git rev-parse HEAD ; git describe) to generate
# src/.git_version (which has the git commit hash on the first line
# and the pretty version on the second), then sed's these two values
# into src/ceph_ver.h.in.cmake in place of @CEPH_GIT_VER@ and
# @CEPH_GIT_NICE_VER@, so when cmake later runs as part of the build,
# it ends up (effectively) just copying that file to
# build/src/include/ceph_ver.h.  It doesn't matter that cmake will
# still internally set CEPH_GIT_VER and CEPH_GIT_NICE_VER to bogus
# values, because we've already done the variable substitution.
#
# This script is invoked from the _service file via:
#
#   <param name="commandtorun">sh etc/ceph_ver_hack.sh</param>
#
# It works because this script lives in the etc/ directory of the ceph 
# source tree. I had previously tried to do the below three commands 
# directly inside the "comandtorun" parameter, as follows:
#
#   <param name="commandtorun">sh -c 'src/make_version -g src/.git_version ; sed -i \"s/@CEPH_GIT_VER@/$(head -n1 src/.git_version)/\" src/ceph_ver.h.in.cmake ; sed -i \"s/@CEPH_GIT_NICE_VER@/$(tail -n1 src/.git_version)/\" src/ceph_ver.h.in.cmake'</param>
#
# ...but that doesn't work, because obs-service-tar_scm runs
# str.split() on it, which splits on whitespace and thus totally
# breaks the big long string I'm trying to pass to `sh -c`.
#
# This is the nastiest hack I've written in recent memory.  I'm
# very, very sorry.
#
# -- Tim Serong <tserong@suse.com> with minor modifications by
#    Nathan Cutler <ncutler@suse.com>
#

(git rev-parse HEAD ; git describe --match 'v*') 2> /dev/null > src/.git_version
sed -i "s/@CEPH_GIT_VER@/$(head -n1 src/.git_version)/" src/ceph_ver.h.in.cmake
sed -i "s/@CEPH_GIT_NICE_VER@/$(tail -n1 src/.git_version)/" src/ceph_ver.h.in.cmake
