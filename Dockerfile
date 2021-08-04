FROM ubuntu:latest
ENV DEBIAN_FRONTEND=nonintercative
COPY . /teuthology
WORKDIR /teuthology
RUN chmod +x /teuthology/bootstrap
RUN apt-get update && apt-get install -y \ 
    git qemu-utils python3-dev libssl-dev \
    python3-pip python3-virtualenv vim \
    libev-dev libvirt-dev libffi-dev \
    libyaml-dev lsb-release && apt-get \ 
    clean all
RUN echo -e 'lock_server: http://paddles:8080\n\
results_server: http://paddles:8080\n\
queue_host: 0.0.0.0\n\
queue_port: 11300\n\
teuthology_path: ./' >> ~/.teuthology.yaml
RUN mkdir archive_dir
RUN mkdir log
CMD  ./bootstrap && ./virtualenv/bin/teuthology-suite \
-v --ceph-repo https://github.com/ceph/ceph.git \
--suite-repo https://github.com/ceph/ceph.git \
-c master -m smithi --subset 9000/100000 --limit 1 \
--suite rados:mgr -k distro --filter 'tasks/progress' \
--suite-branch master -p 75 --force-priority -n 100 \
&& tail -f /dev/null