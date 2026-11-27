C++ Standard and libstdc++ ABI
==============================

The C++ standard that Ceph uses is set by ``CMAKE_CXX_STANDARD`` in
``src/CMakeLists.txt``.

On Enterprise Linux (EL) 8 and 9, such as RHEL, CentOS Stream, and Rocky Linux,
Ceph is built with ``gcc-toolset-13``, as set by ``gts_version`` in
``ceph.spec.in``. On EL10, Fedora, and Ubuntu 24.04 (Noble), Ceph is built with
the GCC that is packaged for the distribution. Ubuntu 22.04 (Jammy) packages
GCC 11, so on Jammy ``install-deps.sh`` installs GCC 12 from the Ubuntu
Toolchain PPA.

All of these toolchains use the new libstdc++ ABI_ by default, and Ceph does
not define ``_GLIBCXX_USE_CXX11_ABI``. This means that ``std::string`` is not
copy-on-write and that ``std::list::size()`` runs in constant time.

.. _ABI: https://gcc.gnu.org/onlinedocs/libstdc++/manual/using_dual_abi.html
