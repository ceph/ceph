
cluster="ceph"
target=exec_cmd("hostname")

exec_cmd("yum -y install ceph-radosgw")
exec_cmd("mkdir -p /var/log/ceph/radosgw")

# create keying
exec_cmd("ceph-authtool --create-keyring /etc/ceph/ceph.client.radosgw.keyring")

# create key and chown permission
exec_cmd("ceph-authtool /etc/ceph/{cluster_name}.client.radosgw.keyring -n client.rgw.{host} "
        "--gen-key".format(cluster_name=cluster, host=target) )
exec_cmd("ceph-authtool -n client.rgw.{host} --cap osd 'allow rwx' "
        "--cap mon 'allow rwx' /etc/ceph/{cluster_name}.client.radosgw.keyring".format(
            cluster_name=cluster, host=target) )
exec_cmd("ceph auth add client.rgw.{host} -i /etc/ceph/{cluster_name}.client.radosgw.keyring".format(
    cluster_name=cluster, host=target) )

#ceph.conf
exec_cmd('''echo "[client.rgw.{target}]"  >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "host=${target}" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "keyring=/etc/ceph/{cluster_name}.client.radosgw.keyring" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "log file=/var/log/ceph/radosgw/{cluster_name}.radosgw.gateway.log" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "rgw_s3_auth_use_keystone = False" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "rgw_frontends = civetweb port=${port}" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "rgw enable usage log = true" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )
exec_cmd('''echo "" >> /etc/ceph/{cluster_name}.conf'''.format(
    cluster_name=cluster, host=target) )

#start rgw
exec_cmd("systemct start ceph-radosgw@rgw.{target}".format(host=target) )
exec_cmd("systemctl enable ceph-radosgw@rgw.${target}".format(host=target) )

exec_cmd("radosg-admin user create --uid admin --display-name 'admin'")
exec_cmd("systemctl stop ceph-radosgw@rgw.{target}".format(host=target) )

