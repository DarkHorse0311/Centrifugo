# To Install:
#
# sudo yum -y install rpmdevtools && rpmdev-setuptree
# wget https://raw.github.com/nmilford/rpm-centrifugo/master/centrifugo.spec -O ~/rpmbuild/SPECS/centrifugo.spec
# wget https://github.com/coreos/centrifugo/releases/download/v2.0.5/centrifugo-v2.0.5-linux-amd64.tar.gz -O ~/rpmbuild/SOURCES/centrifugo-v2.0.5-linux-amd64.tar.gz
# wget https://raw.github.com/nmilford/rpm-centrifugo/master/centrifugo.initd -O ~/rpmbuild/SOURCES/centrifugo.initd
# wget https://raw.github.com/nmilford/rpm-centrifugo/master/centrifugo.sysconfig -O ~/rpmbuild/SOURCES/centrifugo.sysconfig
# wget https://raw.github.com/nmilford/rpm-centrifugo/master/centrifugo.nofiles.conf -O ~/rpmbuild/SOURCES/centrifugo.nofiles.conf
# wget https://raw.github.com/nmilford/rpm-centrifugo/master/centrifugo.logrotate -O ~/rpmbuild/SOURCES/centrifugo.logrotate
# rpmbuild -bb ~/rpmbuild/SPECS/centrifugo.spec

%define debug_package %{nil}
%define centrifugo_user  %{name}
%define centrifugo_group %{name}
%define centrifugo_data  %{_localstatedir}/lib/%{name}

Name:      centrifugo
Version:   %{version}
Release:   %{release}
Summary:   Real-time messaging server
License:   MIT
URL:       https://github.com/centrifugal/centrifugo
Group:     Apps/sys
Source0:   https://github.com/centrifugal/%{name}/releases/download/v%{version}/%{name}-%{version}-linux-amd64.tar.gz
Source1:   %{name}.initd
Buildroot: %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)
Packager:  Alexandr Emelin <frvzmb@gmail.com>
Requires(pre): shadow-utils
Requires(post): /sbin/chkconfig
Requires(preun): /sbin/chkconfig, /sbin/service
Requires(postun): /sbin/service

%description
Real-time messaging server written in Go

%prep
cd ~/rpmbuild/BUILD
rm -rf %{name}-%{version}-linux-amd64
unzip ~/rpmbuild/SOURCES/%{name}-%{version}-linux-amd64.zip
if [ $? -ne 0 ]; then
  exit $?
fi
ls
cd %{name}-%{version}-linux-amd64
chown -R $USER.$USER .
chmod -R a+rX,g-w,o-w .

%build
rm -rf %{buildroot}

echo  %{buildroot}

%install
install -d -m 755 %{buildroot}/%{_sbindir}
install    -m 755 %{_builddir}/%{name}-%{version}-linux-amd64/centrifugo    %{buildroot}/%{_sbindir}


install -d -m 755 %{buildroot}/%{_localstatedir}/log/%{name}
install -d -m 755 %{buildroot}/%{_localstatedir}/lib/%{name}

install -d -m 755 %{buildroot}/%{_initrddir}
install    -m 755 %_sourcedir/%{name}.initd        %{buildroot}/%{_initrddir}/%{name}

install -d -m 755 %{buildroot}/%{_sysconfdir}/security/limits.d/
install    -m 644 %_sourcedir/%{name}.nofiles.conf %{buildroot}/%{_sysconfdir}/security/limits.d/%{name}.nofiles.conf

install -d -m 755 %{buildroot}/%{_sysconfdir}/%{name}/
install    -m 644 %_sourcedir/%{name}.config.json %{buildroot}/%{_sysconfdir}/%{name}/config.json

%clean
rm -rf %{buildroot}

%pre
getent group %{centrifugo_group} >/dev/null || groupadd -r %{centrifugo_group}
getent passwd %{centrifugo_user} >/dev/null || /usr/sbin/useradd --comment "centrifugo Daemon User" --shell /bin/bash -M -r -g %{centrifugo_group} --home %{centrifugo_data} %{centrifugo_user}

%post
chkconfig --add %{name}

%preun
if [ $1 = 0 ]; then
  service %{name} stop > /dev/null 2>&1
  chkconfig --del %{name}
fi

%files
%defattr(-,root,root)
%{_sbindir}/centrifugo
%attr(0755,%{centrifugo_user},%{centrifugo_group}) %dir %{_localstatedir}/log/%{name}
%attr(0755,%{centrifugo_user},%{centrifugo_group}) %dir %{_localstatedir}/lib/%{name}
%{_initrddir}/centrifugo
%{_sysconfdir}/centrifugo/config.json
%{_sysconfdir}/security/limits.d/%{name}.nofiles.conf
