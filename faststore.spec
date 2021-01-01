%define FastStoreServer faststore-server
%define FastStoreClient faststore-client
%define FastStoreDevel faststore-devel
%define FastStoreDebuginfo faststore-debuginfo
%define CommitVersion %(echo $COMMIT_VERSION)

Name: faststore
Version: 1.1.1
Release: 1%{?dist}
Summary: a high performance distributed file storage service
License: AGPL v3.0
Group: Arch/Tech
URL:  http://github.com/happyfish100/faststore/
Source: http://github.com/happyfish100/faststore/%{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n) 

BuildRequires: libfastcommon-devel >= 1.0.46
BuildRequires: libserverframe-devel >= 1.1.2
Requires: %__cp %__mv %__chmod %__grep %__mkdir %__install %__id
Requires: libfastcommon >= 1.0.46
Requires: libserverframe >= 1.1.2
Requires: %{FastStoreServer} = %{version}-%{release}
Requires: %{FastStoreClient} = %{version}-%{release}

%description
a high performance distributed file storage service.
commit version: %{CommitVersion}

%package -n %{FastStoreServer}
Requires: libfastcommon >= 1.0.46
Requires: libserverframe >= 1.1.2
Summary: FastStore server

%package -n %{FastStoreClient}
Requires: libfastcommon >= 1.0.46
Requires: libserverframe >= 1.1.2
Summary: FastStore client library and tools

%package -n %{FastStoreDevel}
Requires: %{FastStoreClient} = %{version}-%{release}
Summary: header files of FastStore client library

%description -n %{FastStoreServer}
FastStore server
commit version: %{CommitVersion}

%description -n %{FastStoreClient}
FastStore client library and tools
commit version: %{CommitVersion}

%description -n %{FastStoreDevel}
This package provides the header files of libfsclient and libfsapi
commit version: %{CommitVersion}


%prep
%setup -q

%build
./make.sh clean && ./make.sh

%install
rm -rf %{buildroot}
DESTDIR=$RPM_BUILD_ROOT ./make.sh install

%post

%preun

%postun

%clean
rm -rf %{buildroot}

%files

%files -n %{FastStoreServer}
/usr/bin/fs_serverd

%files -n %{FastStoreClient}
%defattr(-,root,root,-)
/usr/lib64/libfsclient.so*
/usr/lib64/libfsapi.so*
/usr/bin/fs_cluster_stat
/usr/bin/fs_delete
/usr/bin/fs_fused
/usr/bin/fs_read
/usr/bin/fs_write

%files -n %{FastStoreDevel}
%defattr(-,root,root,-)
/usr/include/faststore/client/*
/usr/include/fastsore/api/*

%changelog
* Fri Jan 1 2021 YuQing <384681@qq.com>
- first RPM release (1.0)
