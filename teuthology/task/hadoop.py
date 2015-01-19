from cStringIO import StringIO
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

HADOOP_2x_URL = "http://apache.osuosl.org/hadoop/common/hadoop-2.5.2/hadoop-2.5.2.tar.gz"

def get_slaves_data(ctx):
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/slaves".format(tdir=tempdir)
    nodes = ctx.cluster.only(teuthology.is_type('hadoop.slave'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    data = '\n'.join(hosts)
    return path, data

def get_masters_data(ctx):
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/masters".format(tdir=tempdir)
    nodes = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    data = '\n'.join(hosts)
    return path, data

def get_core_site_data(ctx, config):
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/core-site.xml".format(tdir=tempdir)
    nodes = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    host = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes][0]

    if config.get('hdfs', False):
        data_tmpl = """
<configuration>
     <property>
         <name>fs.defaultFS</name>
         <value>hdfs://{namenode}:9000</value>
     </property>
     <property>
         <name>hadoop.tmp.dir</name>
         <value>{tdir}/hadoop_tmp</value>
     </property>
</configuration>
"""
    else:
        data_tmpl = """
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>ceph://{namenode}:6789/</value>
  </property>
  <property>
    <name>fs.defaultFS</name>
    <value>ceph://{namenode}:6789/</value>
  </property>
  <property>
    <name>ceph.conf.file</name>
    <value>/etc/ceph/ceph.conf</value>
  </property>
  <property>
    <name>ceph.mon.address</name>
    <value>{namenode}:6789</value>
  </property>
  <property>
    <name>ceph.auth.id</name>
    <value>admin</value>
  </property>
  <property>
    <name>ceph.data.pools</name>
    <value>cephfs_data</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.ceph.impl</name>
    <value>org.apache.hadoop.fs.ceph.CephFs</value>
  </property>
  <property>
    <name>fs.ceph.impl</name>
    <value>org.apache.hadoop.fs.ceph.CephFileSystem</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>{tdir}/hadoop_tmp$</value>
  </property>
</configuration>
"""
    return path, data_tmpl.format(tdir=tempdir, namenode=host)

def get_mapred_site_data(ctx):
    data_tmpl = """
<configuration>
     <property>
         <name>mapred.job.tracker</name>
         <value>{namenode}:9001</value>
     </property>
     <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>
"""
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/mapred-site.xml".format(tdir=tempdir)
    nodes = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    assert len(hosts) == 1
    host = hosts[0]
    return path, data_tmpl.format(namenode=host)

def get_yarn_site_data(ctx):
    data_tmpl = """
<configuration>
<property>
 <name>yarn.resourcemanager.resourcetracker.address</name>
 <value>{namenode}:8025</value>  
</property>
<property>
 <name>yarn.resourcemanager.scheduler.address</name>
 <value>{namenode}:8030</value>  
</property>
<property>
 <name>yarn.resourcemanager.address</name>
 <value>{namenode}:8050</value>  
</property>
<property>
 <name>yarn.resourcemanager.admin.address</name>
 <value>{namenode}:8041</value>  
</property>
<property>
  <name>yarn.resourcemanager.hostname</name>
  <value>{namenode}</value>
</property>
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>
</configuration>
"""
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/yarn-site.xml".format(tdir=tempdir)
    nodes = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    assert len(hosts) == 1
    host = hosts[0]
    return path, data_tmpl.format(namenode=host)

def get_hdfs_site_data(ctx):
    data = """
<configuration>
     <property>
         <name>dfs.replication</name>
         <value>1</value>
     </property>
</configuration>
"""
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/hdfs-site.xml".format(tdir=tempdir)
    return path, data

def configure(ctx, config, hadoops, hadoop_dir):
    tempdir = teuthology.get_testdir(ctx)

    log.info("Writing Hadoop slaves file...")
    for remote in hadoops.remotes:
        path, data = get_slaves_data(ctx)
        teuthology.write_file(remote, path, StringIO(data))

    log.info("Writing Hadoop masters file...")
    for remote in hadoops.remotes:
        path, data = get_masters_data(ctx)
        teuthology.write_file(remote, path, StringIO(data))

    log.info("Writing Hadoop core-site.xml file...")
    for remote in hadoops.remotes:
        path, data = get_core_site_data(ctx, config)
        teuthology.write_file(remote, path, StringIO(data))

    log.info("Writing Hadoop yarn-site.xml file...")
    for remote in hadoops.remotes:
        path, data = get_yarn_site_data(ctx)
        teuthology.write_file(remote, path, StringIO(data))

    log.info("Writing Hadoop hdfs-site.xml file...")
    for remote in hadoops.remotes:
        path, data = get_hdfs_site_data(ctx)
        teuthology.write_file(remote, path, StringIO(data))

    log.info("Writing Hadoop mapred-site.xml file...")
    for remote in hadoops.remotes:
        path, data = get_mapred_site_data(ctx)
        teuthology.write_file(remote, path, StringIO(data))

    log.info("Setting JAVA_HOME in hadoop-env.sh...")
    for remote in hadoops.remotes:
        path = "{tdir}/hadoop/etc/hadoop/hadoop-env.sh".format(tdir=tempdir)
        data = "JAVA_HOME=/usr/lib/jvm/default-java\n" # FIXME: RHEL?
        teuthology.prepend_lines_to_file(remote, path, data)

@contextlib.contextmanager
def install_hadoop(ctx, config):
    testdir = teuthology.get_testdir(ctx)

    log.info("Downloading Hadoop...")
    hadoop_tarball = "{tdir}/hadoop.tar.gz".format(tdir=testdir)
    hadoops = ctx.cluster.only(teuthology.is_type('hadoop'))
    run.wait(
        hadoops.run(
            args = [
                'wget',
                '-nv',
                '-O',
                hadoop_tarball,
                HADOOP_2x_URL
            ],
            wait = False,
            )
        )

    log.info("Create directory for Hadoop install...")
    hadoop_dir = "{tdir}/hadoop".format(tdir=testdir)
    run.wait(
        hadoops.run(
            args = [
                'mkdir',
                hadoop_dir
            ],
            wait = False,
            )
        )

    log.info("Unpacking Hadoop...")
    run.wait(
        hadoops.run(
            args = [
                'tar',
                'xzf',
                hadoop_tarball,
                '--strip-components=1',
                '-C',
                hadoop_dir
            ],
            wait = False,
            )
        )

    log.info("Removing Hadoop download...")
    run.wait(
        hadoops.run(
            args = [
                'rm',
                hadoop_tarball
            ],
            wait = False,
            )
        )

    log.info("Create Hadoop temporary directory...")
    hadoop_tmp_dir = "{tdir}/hadoop_tmp".format(tdir=testdir)
    run.wait(
        hadoops.run(
            args = [
                'mkdir',
                hadoop_tmp_dir
            ],
            wait = False,
            )
        )

    configure(ctx, config, hadoops, hadoop_dir)

    try:
        yield
    finally:
        run.wait(
            hadoops.run(
                args = [
                    'rm',
                    '-rf',
                    hadoop_dir,
                    hadoop_tmp_dir
                ],
                wait = False,
                )
            )

@contextlib.contextmanager
def start_hadoop(ctx, config):
    testdir = teuthology.get_testdir(ctx)
    hadoop_dir = "{tdir}/hadoop/".format(tdir=testdir)
    masters = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    assert len(masters.remotes) == 1
    master = masters.remotes.keys()[0]

    log.info("Formatting HDFS...")
    master.run(
        args = [
            hadoop_dir + "bin/hadoop",
            "namenode",
            "-format"
        ],
        wait = True,
        )

    log.info("Stopping Hadoop daemons")
    master.run(
        args = [
            hadoop_dir + "sbin/stop-yarn.sh"
        ],
        wait = True,
        )

    master.run(
        args = [
            hadoop_dir + "sbin/stop-dfs.sh"
        ],
        wait = True,
        )

    log.info("Starting HDFS...")
    master.run(
        args = [
            hadoop_dir + "sbin/start-dfs.sh"
        ],
        wait = True,
        )

    log.info("Starting YARN...")
    master.run(
        args = [
            hadoop_dir + "sbin/start-yarn.sh"
        ],
        wait = True,
        )

    try:
        yield

    finally:
        log.info("Stopping Hadoop daemons")

        master.run(
            args = [
                hadoop_dir + "sbin/stop-yarn.sh"
                ],
            wait = True,
            )

        master.run(
            args = [
                hadoop_dir + "sbin/stop-dfs.sh"
                ],
            wait = True,
            )

        run.wait(
            ctx.cluster.run(
                args = [
                    'sudo',
                    'skill',
                    '-9',
                    'java'
                    ],
                wait = False
                )
            )

@contextlib.contextmanager
def task(ctx, config):
    if config is None:
        config = {}
    assert isinstance(config, dict), "task hadoop config must be dictionary"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('hadoop', {}))

    tasks = [
        lambda: install_hadoop(ctx=ctx, config=config),
        lambda: start_hadoop(ctx=ctx, config=config),
    ]

    with contextutil.nested(*tasks):
        yield
