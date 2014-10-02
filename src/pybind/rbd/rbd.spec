#
# spec file for package rbd
#

%if ! (0%{?fedora} > 12 || 0%{?rhel} > 5)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}
%endif

#################################################################################
# common
#################################################################################
Name:		python-rbd
Version: 	0.80.6
Release: 	0
Summary: 	Python bindings for Ceph RBD
License: 	MIT
Group:   	System/Filesystems
URL:     	http://ceph.com/
Source0: 	%{name}-%{version}.tar.bz2
BuildRoot:      %{_tmppath}/%{name}-%{version}-build
BuildRequires:  python-devel
BuildRequires:	python-setuptools
%if 0%{?suse_version} && 0%{?suse_version} <= 1110
%{!?python_sitelib: %global python_sitelib %(python -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%else
BuildArch:      noarch
%endif

#################################################################################
# specific
#################################################################################
%if 0%{defined suse_version}
%py_requires
%if 0%{?suse_version} > 1210
Requires:       gptfdisk
%else
Requires:       scsirastools
%endif
%else
Requires:       gdisk
%endif

%if 0%{?rhel}
BuildRequires: 	python >= %{pyver}
Requires: 	python >= %{pyver}
%endif

%description
Python bindings for Ceph RBD

%prep
#%setup -q -n %{name}
%setup -q

%build
#python setup.py build

%install
python setup.py install --prefix=%{_prefix} --root=%{buildroot}

%clean
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf "$RPM_BUILD_ROOT"

%files
%defattr(-,root,root)
%doc LICENSE README.rst
%{python_sitelib}/*

%changelog
