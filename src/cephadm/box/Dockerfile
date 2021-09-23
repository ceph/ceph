# https://developers.redhat.com/blog/2014/05/05/running-systemd-within-docker-container/
FROM centos:8 as centos-systemd
ENV container docker
#RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done); \
#rm -f /lib/systemd/system/multi-user.target.wants/*;\
#rm -f /etc/systemd/system/*.wants/*;\
#rm -f /lib/systemd/system/local-fs.target.wants/*; \
#rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
#rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
#rm -f /lib/systemd/system/basic.target.wants/*;\
#rm -f /lib/systemd/system/anaconda.target.wants/*;
RUN dnf -y install chrony firewalld lvm2 \
  openssh-server openssh-clients python3 yum-utils && dnf clean all
RUN systemctl enable chronyd firewalld sshd


FROM centos-systemd as centos-systemd-docker
# To cache cephadm images
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN dnf -y install docker-ce && \
  dnf clean all && systemctl enable docker


EXPOSE 8443

RUN dnf install sudo which -y

FROM centos-systemd-docker
WORKDIR /root
COPY start /usr/local/bin

CMD [ "/usr/sbin/init" ]
