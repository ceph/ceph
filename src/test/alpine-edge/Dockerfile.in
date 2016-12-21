FROM alpine:%%os_version%%
COPY install-deps.sh /root/
COPY APKBUILD.in /root/alpine/APKBUILD.in
RUN apk --update --no-cache add bash sudo
RUN if test %%USER%% != root ; then adduser -D -H -u %%user_id%% %%USER%% && echo '%%USER%% ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers ; fi
# build dependencies
RUN cd /root ; ./install-deps.sh

