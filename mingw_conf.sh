#!/usr/bin/env bash

# MINGW Settings:
# Due to inconsistencies between distributions, mingw versions, binaries,
# and directories must be determined (or defined) prior to building.

# This script expects the following variables:
# * OS - currently ubuntu, rhel, or suse. In the future we may attempt to
#   detect the platform.
# * MINGW_CMAKE_FILE - if set, a cmake toolchain file will be created
# * MINGW_POSIX_FLAGS - if set, Mingw Posix compatibility mode will be
#                       enabled by defining the according flags.
# * USE_MINGW_LLVM - allows using the mingw llvm toolchain
# * MINGW_LLVM_DIR - allows specifying the mingw-llvm toolchain location.
#                    If unset, we'll use the default path from build.deps.

SCRIPT_DIR="$(dirname "$BASH_SOURCE")"
SCRIPT_DIR="$(realpath "$SCRIPT_DIR")"

MINGW_CMAKE_FILE=${MINGW_CMAKE_FILE:-}
MINGW_POSIX_FLAGS=${MINGW_POSIX_FLAGS:-}

if [[ -n $USE_MINGW_LLVM ]]; then
    MINGW_LLVM_DIR=${MINGW_LLVM_DIR:-"$SCRIPT_DIR/build.deps/mingw-llvm"}
fi

# -Common mingw settings-
MINGW_PREFIX="x86_64-w64-mingw32-"
MINGW_BASE="x86_64-w64-mingw32"
MINGW_CPP="${MINGW_BASE}-c++"
MINGW_DLLTOOL="${MINGW_BASE}-dlltool"
MINGW_WINDRES="${MINGW_BASE}-windres"
MINGW_STRIP="${MINGW_BASE}-strip"
MINGW_OBJCOPY="${MINGW_BASE}-objcopy"

if [[ -n $USE_MINGW_LLVM ]]; then
    # This package isn't currently provided by Linux distributions, we're
    # fetching it from Github.
    export PATH="$MINGW_LLVM_DIR/bin:$PATH"
    mingwPosix=""
    mingwVersion="$(${MINGW_CPP}${mingwPosix} -dumpversion)"
    mingwX64IncludeDir="$MINGW_LLVM_DIR/x86_64-w64-mingw32/include"
    mingwX64BinDir="$MINGW_LLVM_DIR/x86_64-w64-mingw32/bin"
    mingwX64LibDir="$MINGW_LLVM_DIR/x86_64-w64-mingw32/lib"
    mingwTargetLibDir="$mingwX64BinDir"
    mingwLibpthreadDir="$mingwX64BinDir"
    PTW32Include="$mingwX64IncludeDir"
    PTW32Lib="$mingwX64LibDir"

    MINGW_CC="${MINGW_BASE}-clang${mingwPosix}"
    MINGW_CXX="${MINGW_BASE}-clang++${mingwPosix}"

    MINGW_FIND_ROOT_PATH="$MINGW_LLVM_DIR/x86_64-w64-mingw32"
else
    # -Distribution specific mingw settings-
    case "$OS" in
        ubuntu)
            mingwPosix="-posix"
            mingwLibDir="/usr/lib/gcc"
            mingwVersion="$(${MINGW_CPP}${mingwPosix} -dumpversion)"
            mingwTargetLibDir="${mingwLibDir}/${MINGW_BASE}/${mingwVersion}"
            mingwLibpthreadDir="/usr/${MINGW_BASE}/lib"
            PTW32Include=/usr/share/mingw-w64/include
            PTW32Lib=/usr/x86_64-w64-mingw32/lib
            ;;
        rhel)
            mingwPosix=""
            mingwLibDir="/usr/lib64/gcc"
            mingwVersion="$(${MINGW_CPP}${mingwPosix} -dumpversion)"
            mingwTargetLibDir="/usr/${MINGW_BASE}/sys-root/mingw/bin"
            mingwLibpthreadDir="$mingwTargetLibDir"
            PTW32Include=/usr/x86_64-w64-mingw32/sys-root/mingw/include
            PTW32Lib=/usr/x86_64-w64-mingw32/sys-root/mingw/lib
            ;;
        suse)
            mingwPosix=""
            mingwLibDir="/usr/lib64/gcc"
            mingwVersion="$(${MINGW_CPP}${mingwPosix} -dumpversion)"
            mingwTargetLibDir="/usr/${MINGW_BASE}/sys-root/mingw/bin"
            mingwLibpthreadDir="$mingwTargetLibDir"
            PTW32Include=/usr/x86_64-w64-mingw32/sys-root/mingw/include
            PTW32Lib=/usr/x86_64-w64-mingw32/sys-root/mingw/lib
            ;;
        *)
            echo "$ID is unknown, automatic mingw configuration is not possible."
            exit 1
            ;;
    esac
    MINGW_CC="${MINGW_BASE}-gcc${mingwPosix}"
    MINGW_CXX="${MINGW_BASE}-g++${mingwPosix}"

    # -Common mingw settings, dependent upon distribution specific settings-
    MINGW_FIND_ROOT_LIB_PATH="${mingwLibDir}/${MINGW_BASE}/${mingwVersion}"
    MINGW_FIND_ROOT_PATH="/usr/${MINGW_BASE} ${MINGW_FIND_ROOT_LIB_PATH}"
fi
# End MINGW configuration


if [[ -n $MINGW_CMAKE_FILE ]]; then
    cat > $MINGW_CMAKE_FILE <<EOL
set(CMAKE_SYSTEM_NAME Windows)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

# We'll need to use posix threads in order to use
# C++11 features, such as std::thread.
set(CMAKE_C_COMPILER ${MINGW_CC})
set(CMAKE_CXX_COMPILER ${MINGW_CXX})
set(CMAKE_RC_COMPILER ${MINGW_WINDRES})

set(CMAKE_FIND_ROOT_PATH ${MINGW_FIND_ROOT_PATH})
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# TODO: consider switching this to "ONLY". The issue with
# that is that all our libs should then be under
# CMAKE_FIND_ROOT_PATH and CMAKE_PREFIX_PATH would be ignored.
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
EOL
    if [[ -n $MINGW_POSIX_FLAGS ]]; then
        cat >> $MINGW_CMAKE_FILE <<EOL
# Some functions (e.g. localtime_r) will not be available unless we set
# the following flag.
add_definitions(-D_POSIX=1)
add_definitions(-D_POSIX_C_SOURCE=1)
add_definitions(-D_POSIX_=1)
add_definitions(-D_POSIX_THREADS=1)
EOL
    fi

    if [[ -n $USE_MINGW_LLVM ]]; then
        cat >> $MINGW_CMAKE_FILE <<EOL
add_definitions(-I$mingwX64IncludeDir)
add_compile_options(-march=native)
add_compile_options(-Wno-unknown-attributes)
EOL
    fi
fi
