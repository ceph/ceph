FROM quay.ceph.io/ceph-ci/ceph:master

RUN dnf install which sudo glibc-all-langpacks langpacks-en -y
RUN yum -y install glibc-locale-source glibc-langpack-en

RUN localedef -c -f UTF-8 -i en_US en_US.UTF-8
COPY locale.conf /etc/locale.conf

EXPOSE 8443
