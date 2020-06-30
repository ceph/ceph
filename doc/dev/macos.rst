build on MacOS
==============

Since we've switched to C++ 17, and the default clang shipped with Xcode 9 does not support all the C++ 17 language features, it's suggested to install clang using brew::

  brew install --with-toolchain llvm

and install all the necessary bits::

  brew install snappy ccache cmake pkg-config
  pip install cython

install FUSE if you want to build the FUSE support::

  brew cask install osxfuse

then, under the source directory of Ceph::

  mkdir build
  cd build
  cmake .. -DBOOST_J=4 \
    -DCMAKE_C_COMPILER=/usr/local/opt/llvm/bin/clang \
    -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++ \
    -DCMAKE_EXE_LINKER_FLAGS="-L/usr/local/opt/llvm/lib" \
    -DENABLE_GIT_VERSION=OFF \
    -DWITH_MANPAGE=OFF \
    -DWITH_LIBCEPHFS=OFF \
    -DWITH_XFS=OFF \
    -DWITH_KRBD=OFF \
    -DWITH_LTTNG=OFF \
    -DWITH_BABELTRACE=OFF \
    -DWITH_BLUESTORE=OFF \
    -DWITH_RADOSGW=OFF \
    -DWITH_SPDK=OFF \
    -DSNAPPY_ROOT_DIR=/usr/local/Cellar/snappy/1.1.7_1

The paths to ``nss`` and ``snappy`` might vary if newer versions of the packages are installed.

Also, please consider using boost v1.69 to address the bug of https://github.com/boostorg/atomic/issues/15.

Currently, the most practical uses for Ceph on MacOS might be FUSE and some other librados based applications.
