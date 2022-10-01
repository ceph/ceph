FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

RUN microdnf install --nodocs \
    bash       \
    curl       \
    iproute    \
    keepalived-2.1.5 \
 && rm /etc/keepalived/keepalived.conf && microdnf clean all

COPY /skel /

RUN chmod +x init.sh

CMD ["./init.sh"]

# Build specific labels
LABEL maintainer="Guillaume Abrioux <gabrioux@redhat.com>"
LABEL com.redhat.component="keepalived-container"
LABEL version=2.1.5
LABEL name="keepalived"
LABEL description="keepalived for Ceph"
LABEL summary="Provides keepalived on RHEL 8 for Ceph."
LABEL io.k8s.display-name="Keepalived on RHEL 8"
LABEL io.openshift.tags="Ceph keepalived"
