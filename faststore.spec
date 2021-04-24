%define FastStoreServer faststore-server
%define FastStoreClient faststore-client
%define FastStoreDevel  faststore-devel
%define FastStoreDebuginfo faststore-debuginfo
%define FastStoreConfig faststore-config
%define CommitVersion %(echo $COMMIT_VERSION)

Name: faststore
Version: 2.0.0
Release: 1%{?dist}
Summary: a high performance distributed file storage service
License: AGPL v3.0
Group: Arch/Tech
URL:  http://github.com/happyfish100/faststore/
Source: http://github.com/happyfish100/faststore/%{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n) 

BuildRequires: libfastcommon-devel >= 1.0.49
BuildRequires: libserverframe-devel >= 1.1.6
BuildRequires: FastCFS-auth-devel >= 2.0.0
Requires: %__cp %__mv %__chmod %__grep %__mkdir %__install %__id
Requires: libfastcommon >= 1.0.49
Requires: libserverframe >= 1.1.6
Requires: FastCFS-auth-client >= 2.0.0
Requires: FastCFS-auth-config >= 2.0.0
Requires: %{FastStoreServer} = %{version}-%{release}
Requires: %{FastStoreClient} = %{version}-%{release}

%description
a high performance distributed file storage service.
commit version: %{CommitVersion}

%package -n %{FastStoreServer}
Requires: libfastcommon >= 1.0.49
Requires: libserverframe >= 1.1.6
Requires: FastCFS-auth-client >= 2.0.0
Requires: FastCFS-auth-config >= 2.0.0
Requires: %{FastStoreConfig} >= 1.0.0
Summary: FastStore server

%package -n %{FastStoreClient}
Requires: libfastcommon >= 1.0.49
Requires: libserverframe >= 1.1.6
Requires: FastCFS-auth-client >= 2.0.0
Requires: FastCFS-auth-config >= 2.0.0
Requires: %{FastStoreConfig} >= 1.0.0
Summary: FastStore client library and tools

%package -n %{FastStoreDevel}
Requires: %{FastStoreClient} = %{version}-%{release}
Summary: header files of FastStore client library

%package -n %{FastStoreConfig}
Summary: faststore config files for sample

%description -n %{FastStoreServer}
FastStore server
commit version: %{CommitVersion}

%description -n %{FastStoreClient}
FastStore client library and tools
commit version: %{CommitVersion}

%description -n %{FastStoreDevel}
This package provides the header files of libfsclient and libfsapi
commit version: %{CommitVersion}

%description -n %{FastStoreConfig}
faststore config files for sample including server and client
commit version: %{CommitVersion}


%prep
%setup -q

%build
./make.sh clean && ./make.sh

%install
rm -rf %{buildroot}
DESTDIR=$RPM_BUILD_ROOT ./make.sh install
CONFDIR=%{buildroot}/etc/fastcfs/fstore/
SYSTEMDIR=%{buildroot}/usr/lib/systemd/system/
mkdir -p $CONFDIR
mkdir -p $SYSTEMDIR
cp conf/*.conf $CONFDIR
cp systemd/faststore.service $SYSTEMDIR

%post

%preun

%postun

%clean
rm -rf %{buildroot}

%post -n %{FastStoreServer}
mkdir -p /opt/fastcfs/fstore
mkdir -p /opt/faststore/data

%post -n %{FastStoreClient}
mkdir -p /opt/fastcfs/fstore

%files

%files -n %{FastStoreServer}
%defattr(-,root,root,-)
/usr/bin/fs_serverd
%config(noreplace) /usr/lib/systemd/system/faststore.service

%files -n %{FastStoreClient}
%defattr(-,root,root,-)
/usr/lib64/libfsclient.so*
/usr/lib64/libfsapi.so*
/usr/bin/fs_cluster_stat
/usr/bin/fs_service_stat
/usr/bin/fs_delete
/usr/bin/fs_read
/usr/bin/fs_write

%files -n %{FastStoreDevel}
%defattr(-,root,root,-)
/usr/include/faststore/client/*
/usr/include/fastsore/api/*

%files -n %{FastStoreConfig}
%defattr(-,root,root,-)
%config(noreplace) /etc/fastcfs/fstore/*.conf

%changelog
* Fri Jan 1 2021 YuQing <384681@qq.com>
- first RPM release (1.0)
