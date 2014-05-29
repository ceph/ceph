# spec file for package ceph
#
# Copyright (c) 2014 SUSE LINUX Products GmbH, Nuernberg, Germany.
#
# All modifications and additions to the file contributed by third parties
# remain the property of their copyright owners, unless otherwise agreed
# upon. The license for this file, and modifications and additions to the
# file, is the same license as for the pristine package itself (unless the
# license for the pristine package is not an Open Source License, in which
# case the license is the MIT License). An "Open Source License" is a
# license that conforms to the Open Source Definition (Version 1.9)
# published by the Open Source Initiative.

# Please submit bugfixes or comments via http://bugs.opensuse.org/
#

%if 0%{defined rhel_version}
%bcond_with gtk2
%else
%bcond_without gtk2
%endif

# it seems there is no usable tcmalloc rpm for x86_64; parts of
# google-perftools don't compile on x86_64, and apparently the
# decision was to not build the package at all, even if tcmalloc
# itself would have worked just fine.
# Beware: '%bcond_without' turns that feature on!  So as 'osd-mon'
# experiences core-dumps in 'tcmalloc::PageHeap::GrowHeap'...
%bcond_without tcmalloc

%if ! (0%{?fedora} > 12 || 0%{?rhel} > 5)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}
%endif

#################################################################################
# common
#################################################################################
Name:           ceph
Version:        0.80.1
Release:        6%{?dist}
Summary:        A Scalable Distributed File System
License:        GPL-2.0 and LGPL-2.1 and Apache-2.0 and MIT and GPL-2.0-with-autoconf-exception
Group:          System/Filesystems
URL:            http://ceph.com/
Source0:        http://ceph.com/download/%{name}-%{version}.tar.bz2
Source1:        README.SUSE.v0.2
Source2:        mkinitrd-root.on.rbd.tar.xz
Source3:        ceph-tmpfiles.d.conf
# filter spurious setgid warning - mongoose/civetweb is not trying to relinquish suid
Source4:        ceph-rpmlintrc
# PATCH-FIX-OPENSUSE rcfiles-remove-init-2.patch -- Scripts require $network which is unavailable in runlevel 2
Patch0:         rcfiles-remove-init-2.patch
# PATCH-FIX-OPENSUSE radosgw-init-opensuse.patch -- Run daemon as wwwrun, use startproc/killproc, and add status action
Patch1:         radosgw-init-opensuse.patch
Patch2:         ceph-mkcephfs.add.xfs.support.diff
Patch3:         ceph-init-ceph.add.xfs.support.diff
Patch4:         0001-fix-runlevels-for-start-scripts.patch
Patch5:         ceph-mkcephfs-spread-admin-keyring.v2.diff
# PATCH-FIX-OPENSUSE: Remove drop.ceph.com
# not yet cleanly applied
Patch6:         remove-ceph-drop.diff

# SLES specific patches
# not yet cleanly applied
Patch100:       ceph-add-syncfs-support.v3.diff
# it was rebased and enabled by mjura@suse.com
Patch101:       ceph-disk.patch
Requires:       librbd1 = %{version}-%{release}
Requires:       librados2 = %{version}-%{release}
Requires:       libcephfs1 = %{version}-%{release}
# python-ceph is used for client tools.
Requires:	python-ceph = %{version}-%{release}
Requires(post): binutils
BuildRoot:      %{_tmppath}/%{name}-%{version}-build
BuildRequires:  gcc-c++
BuildRequires:  libtool
BuildRequires:  boost-devel > 1.48
BuildRequires:  libedit-devel
BuildRequires:  perl
BuildRequires:  gdbm
BuildRequires:  pkgconfig
BuildRequires:  python
BuildRequires:  libuuid-devel
BuildRequires:  libaio-devel
BuildRequires:  libblkid-devel
BuildRequires:  snappy-devel
BuildRequires:  leveldb-devel
BuildRequires:  xfsprogs-devel
BuildRequires:  libxml2-devel
%if 0%{?suse_version} >= 1310
BuildRequires:  systemd
%endif

#################################################################################
# specific
#################################################################################
%if 0%{defined suse_version}
BuildRequires:  %insserv_prereq
Recommends:     logrotate
BuildRequires:  mozilla-nss-devel
BuildRequires:  keyutils-devel
BuildRequires:  libatomic-ops-devel
%else
BuildRequires:  nss-devel
BuildRequires:  keyutils-libs-devel
BuildRequires:  libatomic_ops-devel
Requires(post): chkconfig
Requires(preun):chkconfig
Requires(preun):initscripts
%endif
BuildRequires:  libcurl-devel
%ifnarch ppc ppc64 s390 s390x ia64
%if 0%{with tcmalloc}
# use isa so this will not be satisfied by
# google-perftools-devel.i686 on a x86_64 box
# http://rpm.org/wiki/PackagerDocs/ArchDependencies
BuildRequires:  gperftools-devel%{?_isa}
%endif
%endif

%description
Ceph is a distributed network file system designed to provide excellent
performance, reliability, and scalability.

#################################################################################
# packages
#################################################################################
%package fuse
License:        GPL-2.0
Summary:        Ceph fuse-based client
Group:          System/Filesystems
Requires:       %{name} = %{version}-%{release}
BuildRequires:  fuse-devel
%description fuse
FUSE based client for Ceph distributed network file system



%if 0%{?rbd_fuse}

%package -n rbd-fuse


Summary:        RBD fuse-based client
Group:          System/Filesystems
Requires:       %{name} = %{version}-%{release}
BuildRequires:  fuse-devel

%description -n rbd-fuse
FUSE based client for Ceph distributed network file system

%endif

%package devel
Summary:        Ceph headers
Group:          Development/Libraries/C and C++
License:        GPL-2.0
Requires:       %{name} = %{version}-%{release}
Requires:       librados2 = %{version}
Requires:       librbd1 = %{version}
Requires:       libcephfs1 = %{version}
%description devel
This package contains libraries and headers needed to develop programs
that use Ceph.

%package radosgw
License:        GPL-2.0
Summary:        Rados REST Gateway
Group:          System/Filesystems
Requires:       librados2 = %{version}-%{release}
%if 0%{defined suse_version}
BuildRequires:  libexpat-devel
BuildRequires:  FastCGI-devel
Requires:       apache2-mod_fcgid
%else
BuildRequires:  expat-devel
BuildRequires:  fcgi-devel
Requires:       mod_fcgid
%endif
%description radosgw
radosgw is an S3 HTTP REST gateway for the RADOS object store. It is
implemented as a FastCGI module using libfcgi, and can be used in
conjunction with any FastCGI capable web server.

%if %{with gtk2}

%package gcephtool
Summary:        Graphical Monitoring Tool for Ceph
Group:          System/Filesystems
License:        GPL-2.0
Requires:       gtk2 gtkmm24
BuildRequires:  gtk2-devel gtkmm24-devel
%description gcephtool
gcephtool is a graphical monitor for the clusters running the Ceph distributed
file system.
%endif

%if 0%{?suse_version}
%package resource-agents
Summary:        OCF-compliant Resource Agents for Ceph Daemons
Group:          System/Filesystems
License:        GPL-2.0
Requires:       %{name} = %{version}
Requires:       resource-agents
%description resource-agents
Resource agents for monitoring and managing Ceph daemons
under Open Cluster Framework (OCF) compliant resource
managers such as Pacemaker.
%endif

%package -n librados2
Summary:        RADOS distributed object store client library
Group:          System/Filesystems
License:        GPL-2.0
%description -n librados2
RADOS is a reliable, autonomic distributed object storage cluster
developed as part of the Ceph distributed storage system. This is a
shared library allowing applications to access the distributed object
store using a simple file-like interface.

%package -n librbd1
Summary:        RADOS Block Device Client Library
Group:          System/Filesystems
License:        GPL-2.0
Requires:       librados2 = %{version}-%{release}
%description -n librbd1
RBD is a block device striped across multiple distributed objects in
RADOS, a reliable, autonomic distributed object storage cluster
developed as part of the Ceph distributed storage system. This is a
shared library allowing applications to manage these block devices.

%package -n libcephfs1
Summary:        Ceph distributed file system client library
Group:          System/Filesystems
License:        GPL-2.0
%description -n libcephfs1
Ceph is a distributed network file system designed to provide excellent
performance, reliability, and scalability. This is a shared library
allowing applications to access a Ceph distributed file system via a
POSIX-like interface.


%if 0%{?cephfs_java}

%package -n libcephfs_jni1


Summary:        Java Native Interface library for CephFS Java bindings
Group:          System/Filesystems
Requires:       java
%if 0%{?rhel_version} || 0%{?centos_version}
BuildRequires:  java-1.6.0-openjdk-devel
%else
%if 0%{?fedora}
BuildRequires:  java-1.7.0-openjdk-devel
%else
BuildRequires:  java-devel
%endif
%endif
%endif



%package -n python-ceph
Summary:        Python Libraries for the Ceph Distributed Filesystem
Group:          System/Filesystems
License:        GPL-2.0
Requires:       librados2 = %{version}-%{release}
Requires:       librbd1 = %{version}-%{release}
Requires:       libcephfs1 = %{version}-%{release}
%if 0%{defined suse_version}
%py_requires
%endif
%description -n python-ceph
This package contains Python libraries for interacting with Cephs RADOS
object storage.

%package -n ceph-test


Summary:        Ceph benchmarks and test tools
Group:          System/Filesystems
Requires:       libcephfs1 = %{version}-%{release}
Requires:       librados2 = %{version}-%{release}
Requires:       librbd1 = %{version}-%{release}

%description -n ceph-test
This package contains Ceph benchmarks and test tools.

%package -n ceph-keys
Summary:        Evil Keys
Group:          System/Filesystems
License:        GPL-2.0
%if 0%{defined suse_version}
%py_requires
%endif
%description -n ceph-keys
what look like some ceph keys that shodul nto rearly exist in packaging.

#################################################################################
# common
#################################################################################
%prep
%setup -q
%patch0 -p1
%patch1 -p1
%patch2 -p1
%patch3 -p1
%patch4 -p1
%patch5 -p1
%patch6 -p1
%patch100 -p1
%patch101 -p1

%build
./autogen.sh

export RPM_OPT_FLAGS=`echo $RPM_OPT_FLAGS | sed -e 's/i386/i486/'`

# be explicit about --with/without-tcmalloc because the
# autoconf default differs from what's needed for rpm
%{configure}    CPPFLAGS="$java_inc" \
                --localstatedir=/var \
                --sysconfdir=/etc \
                --docdir=%{_docdir}/ceph \
                --without-hadoop \
                --with-radosgw \
                --with-nss \
                --with-rest-bench \
                --with-debug=yes \
%if 0%{?cephfs_java}
                --enable-cephfs-java \
%endif
%if 0%{?suse_version}
                --with-ocf \
%endif
%if 0%{with system_leveldb}
                --with-system-leveldb \
%else
                --without-system-leveldb \
%endif
%ifnarch ppc ppc64 s390 s390x ia64
%if 0%{with tcmalloc}
                --with-tcmalloc \
%else
                --without-tcmalloc \
%endif
%else
                --without-tcmalloc \
%endif
                CFLAGS="$RPM_OPT_FLAGS" CXXFLAGS="$RPM_OPT_FLAGS"

# fix bug in specific version of libedit-devel
%if 0%{defined suse_version}
sed -i -e "s/-lcurses/-lncurses/g" Makefile
sed -i -e "s/-lcurses/-lncurses/g" src/Makefile
sed -i -e "s/-lcurses/-lncurses/g" man/Makefile
%endif

make %{?_smp_mflags}

%install
make DESTDIR=$RPM_BUILD_ROOT install
find $RPM_BUILD_ROOT -type f -name "*.la" -exec rm -f {} ';'
find $RPM_BUILD_ROOT -type f -name "*.a" -exec rm -f {} ';'
install -D src/init-ceph $RPM_BUILD_ROOT%{_initrddir}/ceph
install -D src/init-radosgw $RPM_BUILD_ROOT%{_initrddir}/ceph-radosgw
mkdir -p $RPM_BUILD_ROOT/usr/sbin
ln -sf ../../etc/init.d/ceph %{buildroot}/usr/sbin/rcceph
ln -sf ../../etc/init.d/ceph-radosgw %{buildroot}/usr/sbin/rcceph-radosgw
install -m 0644 -D src/logrotate.conf $RPM_BUILD_ROOT%{_sysconfdir}/logrotate.d/ceph
chmod 0644 $RPM_BUILD_ROOT%{_docdir}/ceph/sample.ceph.conf
chmod 0644 $RPM_BUILD_ROOT%{_docdir}/ceph/sample.fetch_config
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/lib/ceph/tmp/
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/log/ceph/
%if 0%{?suse_version} >= 1310
%{__install} -d -m 0755 %{buildroot}/%{_tmpfilesdir}
%{__install} -m 0644 %{SOURCE3} %{buildroot}/%{_tmpfilesdir}/%{name}.conf
%else
mkdir -p $RPM_BUILD_ROOT%{_localstatedir}/run/ceph/
%endif
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/ceph/
cp %{S:1} $RPM_BUILD_ROOT/%{_docdir}/ceph/README.SUSE
rm $RPM_BUILD_ROOT%{python_sitelib}/*.pyo
%clean
rm -rf $RPM_BUILD_ROOT

%post
/sbin/ldconfig
%if 0%{?suse_version} >= 1310
systemd-tmpfiles --create %{_tmpfilesdir}/%{name}.conf
%endif
#/sbin/chkconfig --add ceph

#%preun
#%if %{defined suse_version}
#%stop_on_removal ceph
#%endif
#if [ $1 = 0 ] ; then
#    /sbin/service ceph stop >/dev/null 2>&1
#    /sbin/chkconfig --del ceph
#fi

#%postun
#/sbin/ldconfig
#if [ "$1" -ge "1" ] ; then
#    /sbin/service ceph condrestart >/dev/null 2>&1 || :
#fi
#%if %{defined suse_version}
#%restart_on_update ceph
#%insserv_cleanup
#%endif

#################################################################################
# files
#################################################################################
%files
%defattr(-,root,root,-)
%docdir %{_docdir}
%dir %{_docdir}/ceph
%{_docdir}/ceph/sample.ceph.conf
%{_docdir}/ceph/sample.fetch_config
%{_docdir}/ceph/README.SUSE
%{_bindir}/ceph
%{_bindir}/cephfs
%{_bindir}/ceph-conf
%{_bindir}/ceph-clsinfo
%{_bindir}/crushtool
%{_bindir}/monmaptool
%{_bindir}/osdmaptool
%{_bindir}/ceph-authtool
%{_bindir}/ceph-syn
%{_bindir}/ceph-run
%{_bindir}/ceph-mon
%{_bindir}/ceph-mds
%{_bindir}/ceph-osd
%{_bindir}/ceph-rbdnamer
%{_bindir}/librados-config
%{_bindir}/ceph_test_objectstore_workloadgen
%{_bindir}/rados
%{_bindir}/rbd
%{_bindir}/ceph-debugpack
%{_bindir}/ceph-client-debug
%{_bindir}/ceph-coverage
%{_bindir}/ceph-dencoder
%{_bindir}/ceph-brag
%{_bindir}/ceph-crush-location
%{_bindir}/ceph-post-file
%{_bindir}/ceph-rest-api
%{_bindir}/ceph_filestore_dump
%{_bindir}/ceph_filestore_tool
%{_bindir}/ceph_mon_store_converter
%{_bindir}/rbd-fuse
%{_bindir}/ceph-monstore-tool
%{_bindir}/ceph-osdomap-tool
%{_bindir}/ceph_erasure_code
%{_bindir}/ceph_erasure_code_benchmark
%{_initrddir}/ceph
%dir %{_libdir}/rados-classes
/sbin/mkcephfs
/sbin/mount.ceph
%{_sbindir}/ceph-create-keys
%{_sbindir}/ceph-disk
%{_sbindir}/ceph-disk-activate
%{_sbindir}/ceph-disk-prepare
%{_sbindir}/ceph-disk-udev
/sbin/mount.fuse.ceph
%{_libdir}/ceph
%config %{_sysconfdir}/bash_completion.d/ceph
%config %{_sysconfdir}/bash_completion.d/rados
%config %{_sysconfdir}/bash_completion.d/radosgw-admin
%config %{_sysconfdir}/bash_completion.d/rbd
%config(noreplace) %{_sysconfdir}/logrotate.d/ceph
%{_mandir}/man8/ceph-mon.8*
%{_mandir}/man8/ceph-mds.8*
%{_mandir}/man8/ceph-osd.8*
%{_mandir}/man8/mkcephfs.8*
%{_mandir}/man8/ceph-run.8*
%{_mandir}/man8/ceph-syn.8*
%{_mandir}/man8/crushtool.8*
%{_mandir}/man8/osdmaptool.8*
%{_mandir}/man8/monmaptool.8*
%{_mandir}/man8/ceph-conf.8*
%{_mandir}/man8/ceph.8*
%{_mandir}/man8/cephfs.8*
%{_mandir}/man8/ceph-post-file.8.gz
%{_mandir}/man8/ceph-rest-api.8.gz
%{_mandir}/man8/mount.ceph.8*
%{_mandir}/man8/rados.8*
%{_mandir}/man8/rbd.8*
%{_mandir}/man8/rbd-fuse.8*
%{_mandir}/man8/ceph-authtool.8*
%{_mandir}/man8/ceph-debugpack.8*
%{_mandir}/man8/ceph-clsinfo.8.gz
%{_mandir}/man8/librados-config.8.gz
%{_mandir}/man8/ceph-dencoder.8.gz
%{_mandir}/man8/ceph-rbdnamer.8.gz
%{_mandir}/man8/ceph-post-file.8.gz
%{_mandir}/man8/ceph-rest-api.8.gz
%dir %{_localstatedir}/lib/ceph/
%dir %{_localstatedir}/lib/ceph/tmp/
%dir %{_localstatedir}/log/ceph/
%if 0%{?suse_version} < 1310
%ghost %dir %{_localstatedir}/run/ceph/
%else
%dir %{_tmpfilesdir}/
%{_tmpfilesdir}/%{name}.conf
%endif
%dir %{_sysconfdir}/ceph/
/usr/sbin/rcceph
%{_libdir}/rados-classes/libcls_rbd.so*
%{_libdir}/rados-classes/libcls_rgw.so*
%{_libdir}/rados-classes/libcls_hello.so
%{_libdir}/rados-classes/libcls_kvs.so
%{_libdir}/rados-classes/libcls_lock.so
%{_libdir}/rados-classes/libcls_log.so
%{_libdir}/rados-classes/libcls_refcount.so
%{_libdir}/rados-classes/libcls_replica_log.so
%{_libdir}/rados-classes/libcls_statelog.so
%{_libdir}/rados-classes/libcls_user.so
%{_libdir}/rados-classes/libcls_user.so.1
%{_libdir}/rados-classes/libcls_user.so.1.0.0
%{_libdir}/rados-classes/libcls_version.so
#################################################################################
%files fuse
%defattr(-,root,root,-)
%{_bindir}/ceph-fuse
%{_mandir}/man8/ceph-fuse.8*

%if 0%{?rbd_fuse}
%files -n rbd-fuse
%defattr(-,root,root,-)
%{_bindir}/rbd-fuse
%{_mandir}/man8/rbd-fuse.8*
%endif


#################################################################################
%files devel
%defattr(-,root,root,-)
%dir %{_includedir}/cephfs
%{_includedir}/cephfs/libcephfs.h
%dir %{_includedir}/rados
%{_includedir}/rados/memory.h
%{_includedir}/rados/rados_types.h
%{_includedir}/rados/rados_types.hpp
%{_includedir}/rados/librados.h
%{_includedir}/rados/librados.hpp
%{_includedir}/rados/buffer.h
%{_includedir}/rados/page.h
%{_includedir}/rados/crc32c.h
%dir %{_includedir}/rbd
%{_includedir}/rbd/librbd.h
%{_includedir}/rbd/librbd.hpp
%{_includedir}/rbd/features.h
%{_libdir}/libcephfs.so
%{_libdir}/librbd.so
%{_libdir}/librados.so

#################################################################################
%files radosgw
%defattr(-,root,root,-)
%{_initrddir}/ceph-radosgw
%{_bindir}/radosgw
%{_bindir}/radosgw-admin
%{_mandir}/man8/radosgw.8*
%{_mandir}/man8/radosgw-admin.8*
%{_sbindir}/rcceph-radosgw

%post radosgw
/sbin/ldconfig
%if %{defined suse_version}
%fillup_and_insserv -f -y ceph-radosgw
%endif

%preun radosgw
%if %{defined suse_version}
%stop_on_removal ceph-radosgw
%endif

%postun radosgw
/sbin/ldconfig
%if %{defined suse_version}
%restart_on_update ceph-radosgw
%insserv_cleanup
%endif


#################################################################################

%if 0%{?suse_version}
%files resource-agents
%defattr(0755,root,root,-)
%dir /usr/lib/ocf
%dir /usr/lib/ocf/resource.d
%dir /usr/lib/ocf/resource.d/ceph
/usr/lib/ocf/resource.d/ceph/ceph
/usr/lib/ocf/resource.d/ceph/mds
/usr/lib/ocf/resource.d/ceph/mon
/usr/lib/ocf/resource.d/ceph/osd
/usr/lib/ocf/resource.d/ceph/rbd
/usr/lib/ocf/resource.d/%{name}/*
%endif

#################################################################################
%files -n librados2
%defattr(-,root,root,-)
%{_libdir}/librados.so.*

%post -n librados2
/sbin/ldconfig

%postun -n librados2
/sbin/ldconfig

#################################################################################
%files -n librbd1
%defattr(-,root,root,-)
%{_libdir}/librbd.so.*

%post -n librbd1
/sbin/ldconfig

%postun -n librbd1
/sbin/ldconfig

#################################################################################
%files -n libcephfs1
%defattr(-,root,root,-)
%{_libdir}/libcephfs.so.*

%post -n libcephfs1
/sbin/ldconfig

%postun -n libcephfs1
/sbin/ldconfig

#################################################################################
%files -n python-ceph
%defattr(-,root,root,-)
%{python_sitelib}/rados.py*
%{python_sitelib}/rbd.py*
%{python_sitelib}/ceph_argparse.py*
%{python_sitelib}/ceph_rest_api.py*
%{python_sitelib}/cephfs.py*
%changelog

%files -n ceph-keys
%defattr(-,root,root,-)
%dir /usr/share/ceph
/usr/share/ceph/id_dsa_drop.ceph.com
/usr/share/ceph/id_dsa_drop.ceph.com.pub
/usr/share/ceph/known_hosts_drop.ceph.com




%files -n ceph-test
%defattr(-,root,root,-)
%{_bindir}/rest-bench
%{_bindir}/ceph_bench_log
%{_bindir}/ceph_dupstore
%{_bindir}/ceph_kvstorebench
%{_bindir}/ceph_multi_stress_watch
%{_bindir}/ceph_omapbench
%{_bindir}/ceph_psim
%{_bindir}/ceph_radosacl
%{_bindir}/ceph_rgw_jsonparser
%{_bindir}/ceph_rgw_multiparser
%{_bindir}/ceph_scratchtool
%{_bindir}/ceph_scratchtoolpp
%{_bindir}/ceph_smalliobench
%{_bindir}/ceph_smalliobenchrbd
%{_bindir}/ceph_smalliobenchdumb
%{_bindir}/ceph_smalliobenchfs
%{_bindir}/ceph_streamtest
%{_bindir}/ceph_test_cfuse_cache_invalidate
%{_bindir}/ceph_test_cls_hello
%{_bindir}/ceph_test_cls_lock
%{_bindir}/ceph_test_cls_log
%{_bindir}/ceph_test_cls_rbd
%{_bindir}/ceph_test_cls_refcount
%{_bindir}/ceph_test_cls_replica_log
%{_bindir}/ceph_test_cls_rgw
%{_bindir}/ceph_test_cls_rgw_log
%{_bindir}/ceph_test_cls_rgw_meta
%{_bindir}/ceph_test_cls_rgw_opstate
%{_bindir}/ceph_test_cls_statelog
%{_bindir}/ceph_test_cls_version
%{_bindir}/ceph_test_msgr
%{_bindir}/ceph_test_rados
%{_bindir}/ceph_test_rados_api_cmd
%{_bindir}/ceph_test_rados_api_lock
%{_bindir}/ceph_test_snap_mapper
%{_bindir}/ceph_test_filejournal
%{_bindir}/ceph_test_filestore_idempotent
%{_bindir}/ceph_test_filestore_idempotent_sequence
%{_bindir}/ceph_test_ioctls
%{_bindir}/ceph_test_keyvaluedb_atomicity
%{_bindir}/ceph_test_keyvaluedb_iterators
%{_bindir}/ceph_test_libcephfs
%{_bindir}/ceph_test_librbd
%{_bindir}/ceph_test_librbd_fsx
%{_bindir}/ceph_test_mon_workloadgen
%{_bindir}/ceph_test_mutate
%{_bindir}/ceph_test_object_map
%{_bindir}/ceph_test_objectcacher_stress
%{_bindir}/ceph_test_rados_api_aio
%{_bindir}/ceph_test_rados_api_cls
%{_bindir}/ceph_test_rados_api_io
%{_bindir}/ceph_test_rados_api_list
%{_bindir}/ceph_test_rados_api_misc
%{_bindir}/ceph_test_rados_api_pool
%{_bindir}/ceph_test_rados_api_snapshots
%{_bindir}/ceph_test_rados_api_stat
%{_bindir}/ceph_test_rados_api_watch_notify
%{_bindir}/ceph_test_rewrite_latency
%{_bindir}/ceph_test_stress_watch
%{_bindir}/ceph_test_trans
%{_bindir}/ceph_test_cors
%{_bindir}/ceph_test_crypto
%{_bindir}/ceph_test_keys
%{_bindir}/ceph_test_rados_delete_pools_parallel
%{_bindir}/ceph_test_rados_list_parallel
%{_bindir}/ceph_test_rados_open_pools_parallel
%{_bindir}/ceph_test_rados_watch_notify
%{_bindir}/ceph_test_signal_handlers
%{_bindir}/ceph_test_timers
%{_bindir}/ceph_tpbench
%{_bindir}/ceph_xattr_bench
%{_bindir}/ceph-kvstore-tool
%{_bindir}/ceph_test_c_headers
%{_bindir}/ceph_test_get_blkdev_size
%{_bindir}/ceph_test_objectstore
%{_bindir}/ceph_test_rados_api_c_read_operations
%{_bindir}/ceph_test_rados_api_c_write_operations
%{_bindir}/ceph_test_rados_api_tier
%{_bindir}/ceph_test_rgw_manifest

