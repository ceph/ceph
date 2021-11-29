# https://developers.redhat.com/blog/2014/05/05/running-systemd-within-docker-container/
FROM centos:8 as centos-systemd
ENV container docker
ENV CEPHADM_PATH=/usr/local/sbin/cephadm
RUN dnf -y install chrony firewalld lvm2 \
  openssh-server openssh-clients python3 \
  yum-utils sudo which && dnf clean all

RUN systemctl enable chronyd firewalld sshd


FROM centos-systemd as centos-systemd-docker
# To cache cephadm images
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN dnf -y install docker-ce && \
    dnf clean all && systemctl enable docker

# ssh utilities
RUN dnf install epel-release -y && dnf makecache && dnf install sshpass -y

EXPOSE 8443
EXPOSE 22

FROM centos-systemd-docker
WORKDIR /root
# VOLUME /var/run/docker.sock
COPY start /usr/local/bin

CMD [ "/usr/sbin/init" ]
