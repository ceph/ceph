#!/usr/bin/env bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
set -xe
set -o pipefail

. /etc/os-release
base=${1:-/tmp/release}
releasedir="$base/$NAME/WORKDIR"
rm -fr "$(dirname "$releasedir")"

# Clean untracked files if we're in a git checkout
[ -e .git ] && git clean -dxf

# Determine version (use arg if provided by CI)
if [ -z "${2:-}" ]; then
  vers="$(git describe --match "v*" | sed 's/^v//')"
  dvers="${vers}-1"
else
  vers="${2}"
  dvers="${vers}-1${VERSION_CODENAME}"
fi

# Ensure source tarball exists
test -f "ceph-$vers.tar.bz2" || ./make-dist "$vers"

# Prepare Debian build tree
mkdir -p "$releasedir"
mv "ceph-$vers.tar.bz2" "$releasedir/ceph_${vers}.orig.tar.bz2"
tar -C "$releasedir" -jxf "$releasedir/ceph_${vers}.orig.tar.bz2"

cp -a debian "$releasedir/ceph-$vers/debian"
cd "$releasedir"

# Optionally remove -dbg packages
if [[ -n "${SKIP_DEBUG_PACKAGES:-}" ]]; then
  perl -ni -e 'print if(!(/^Package: .*-dbg$/../^$/))' "ceph-$vers/debian/control"
  perl -pi -e 's/--dbg-package.*//' "ceph-$vers/debian/rules"
fi

# Normalize build dir name for cache hits if requested
if [ "${CEPH_BUILD_NORMALIZE_PATHS:-false}" = "true" ]; then
  mv "ceph-$vers" ceph
  cd ceph
else
  cd "ceph-$vers"
fi

# Bump changelog if needed
chvers="$(head -1 debian/changelog | perl -ne 's/.*\(//; s/\).*//; print')"
if [ "$chvers" != "$dvers" ]; then
  DEBEMAIL="contact@ceph.com" dch -D "$VERSION_CODENAME" --force-distribution -b -v "$dvers" "new version"
fi

# --------------------------
# Parallelism: memory-aware
# --------------------------
# Use cgroup v2 memory limit if present; else MemAvailable.
limit_bytes="$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo max)"
if [ "$limit_bytes" = "max" ] || [ "$limit_bytes" -ge 9007199254740991 ]; then
  limit_bytes="$(awk '/MemAvailable:/ {print $2*1024}' /proc/meminfo)"
fi
mem_gib="$(awk -v b="$limit_bytes" 'BEGIN{printf "%.0f", b/1024/1024/1024}')"

# Assume ~2 GiB per job; keep 16 GiB headroom
jobs=$(( (mem_gib - 16) / 2 ))
[ "$jobs" -lt 1 ] && jobs=1

# Cap by half the CPUs (SMT inflates nproc for memory-bound workloads)
cpu_cap=$(( $(nproc) / 2 ))
[ "$cpu_cap" -lt 1 ] && cpu_cap=1
[ "$jobs" -gt "$cpu_cap" ] && jobs="$cpu_cap"

echo "Detected ~${mem_gib} GiB available, CPUs=$(nproc); using parallel=${jobs}"

export DEB_BUILD_OPTIONS="parallel=${jobs} ${DEB_BUILD_OPTIONS:-}"
export MAKEFLAGS="--output-sync=line -j${jobs}"
j="-j${jobs}"

# -----------------------------------
# LTO + debug flags
# -----------------------------------
if [ "${DISABLE_LTO:-false}" = "true" ]; then
  export DEB_BUILD_MAINT_OPTIONS="optimize=-lto ${DEB_BUILD_MAINT_OPTIONS:-}"
  export DEB_CFLAGS_MAINT_APPEND="${DEB_CFLAGS_MAINT_APPEND:-} -g1 -gsplit-dwarf"
  export DEB_CXXFLAGS_MAINT_APPEND="${DEB_CXXFLAGS_MAINT_APPEND:-} -g1 -gsplit-dwarf"
else
  export DEB_CFLAGS_MAINT_APPEND="${DEB_CFLAGS_MAINT_APPEND:-} -g1 -flto=1 -fno-fat-lto-objects"
  export DEB_CXXFLAGS_MAINT_APPEND="${DEB_CXXFLAGS_MAINT_APPEND:-} -g1 -flto=1 -fno-fat-lto-objects"
  DEB_CFLAGS_MAINT_APPEND="${DEB_CFLAGS_MAINT_APPEND/ -gsplit-dwarf/}"
  DEB_CXXFLAGS_MAINT_APPEND="${DEB_CXXFLAGS_MAINT_APPEND/ -gsplit-dwarf/}"
  export DEB_CFLAGS_MAINT_APPEND DEB_CXXFLAGS_MAINT_APPEND
fi

# -----------------------------------
# Linker flags (force bfd)
# -----------------------------------
export DEB_LDFLAGS_MAINT_APPEND="${DEB_LDFLAGS_MAINT_APPEND:-} -Wl,-O1,--as-needed,--no-keep-memory"
DEB_LDFLAGS_MAINT_APPEND="$(echo "$DEB_LDFLAGS_MAINT_APPEND" | sed 's/-fuse-ld=gold//g') -fuse-ld=bfd"
export DEB_LDFLAGS_MAINT_APPEND

# ccache unless SCCACHE=true
if [ "${SCCACHE:-}" != "true" ]; then
  PATH="/usr/lib/ccache:$PATH"
fi

# Also hard override the environment for safety
export CFLAGS="${CFLAGS/-fuse-ld=gold/} -fuse-ld=bfd"
export CXXFLAGS="${CXXFLAGS/-fuse-ld=gold/} -fuse-ld=bfd"
export LDFLAGS="${LDFLAGS/-fuse-ld=gold/} -fuse-ld=bfd"

# Build
PATH="$PATH" dpkg-buildpackage "$j" -uc -us

# Reprepro metadata
cd ../..
mkdir -p "$VERSION_CODENAME/conf"
cat > "$VERSION_CODENAME/conf/distributions" <<EOF
Codename: $VERSION_CODENAME
Suite: stable
Components: main
Architectures: $(dpkg --print-architecture) source
EOF
if [ ! -e conf ]; then
  ln -s "$VERSION_CODENAME/conf" conf
fi
reprepro --basedir "$(pwd)" include "$VERSION_CODENAME" WORKDIR/*.changes

# teuthology needs the version
echo "$dvers" > "$VERSION_CODENAME/version"
