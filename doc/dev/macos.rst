build on MacOS
==============

Since we've switched to C++ 17, and the default clang shipped with Xcode 9 does not support all the C++ 17 language features, it's suggested to install clang using brew::

  brew install --with-toolchain llvm

and install all the necessary bits::

  brew install nss snappy ccache cmake pkg-config
  pip install cython

install FUSE if you want to build the FUSE support::

  brew cask install osxfuse

apply the patch at https://gist.github.com/tchaikov/c3f324a7c36fc9774739cea319d5c49b , to address https://public.kitware.com/Bug/view.php?id=15943 . We cannot bump up the required cmake version yet, because RHEL/CentOS does not have the newer cmake yet.

then, under the source directory of Ceph::

  mkdir build
  cd build
  PKG_CONFIG_PATH=/usr/local/Cellar/nss/3.33/lib/pkgconfig \
    CC=/usr/local/opt/llvm/bin/clang \
    CXX=/usr/local/opt/llvm/bin/clang++ \
    cmake .. -DBOOST_J=4 \
    -DENABLE_GIT_VERSION=OFF \
    -DWITH_EMBEDDED=OFF \
    -DWITH_MANPAGE=OFF \
    -DWITH_LIBCEPHFS=OFF \
    -DWITH_XFS=OFF \
    -DWITH_KRBD=OFF \
    -DWITH_LTTNG=OFF \
    -DWITH_BABELTRACE=OFF \
    -DWITH_BLUESTORE=OFF \
    -DWITH_RADOSGW=OFF \
    -DWITH_SPDK=OFF \
    -DSNAPPY_ROOT_DIR=/usr/local/Cellar/snappy/1.1.7

The paths to ``nss`` and ``snappy`` might vary if newer versions of the packages are installed.

Currently, the most practical uses for Ceph on MacOS might be FUSE and some other librados based applications.
