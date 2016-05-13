from cStringIO import StringIO
import contextlib
import logging
from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run
from ..exceptions import UnsupportedPackageTypeError

log = logging.getLogger(__name__)

HADOOP_2x_URL = "http://apache.osuosl.org/hadoop/common/hadoop-2.5.2/hadoop-2.5.2.tar.gz"

def dict_to_hadoop_conf(items):
    out = "<configuration>\n"
    for key, value in items.iteritems():
        out += "  <property>\n"
        out += "    <name>" + key + "</name>\n"
        out += "    <value>" + value + "</value>\n"
        out += "  </property>\n"
    out += "</configuration>\n"
    return out

def is_hadoop_type(type_):
    return lambda role: role.startswith('hadoop.' + type_)

def get_slaves_data(ctx):
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/slaves".format(tdir=tempdir)
    nodes = ctx.cluster.only(is_hadoop_type('slave'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    data = '\n'.join(hosts)
    return path, data

def get_masters_data(ctx):
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/masters".format(tdir=tempdir)
    nodes = ctx.cluster.only(is_hadoop_type('master'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    data = '\n'.join(hosts)
    return path, data

def get_core_site_data(ctx, config):
    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/core-site.xml".format(tdir=tempdir)
    nodes = ctx.cluster.only(is_hadoop_type('master'))
    host = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes][0]

    conf = {}
    if config.get('hdfs', False):
        conf.update({
            'fs.defaultFS': 'hdfs://{namenode}:9000',
            'hadoop.tmp.dir': '{tdir}/hadoop_tmp',
        })
    else:
        conf.update({
            'fs.default.name': 'ceph://{namenode}:6789/',
            'fs.defaultFS': 'ceph://{namenode}:6789/',
            'ceph.conf.file': '/etc/ceph/ceph.conf',
            'ceph.mon.address': '{namenode}:6789',
            'ceph.auth.id': 'admin',
            #'ceph.data.pools': 'cephfs_data',
            'fs.AbstractFileSystem.ceph.impl': 'org.apache.hadoop.fs.ceph.CephFs',
            'fs.ceph.impl': 'org.apache.hadoop.fs.ceph.CephFileSystem',
        })

    data_tmpl = dict_to_hadoop_conf(conf)
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
    nodes = ctx.cluster.only(is_hadoop_type('master'))
    hosts = [s.ssh.get_transport().getpeername()[0] for s in nodes.remotes]
    assert len(hosts) == 1
    host = hosts[0]
    return path, data_tmpl.format(namenode=host)

def get_yarn_site_data(ctx):
    conf = {}
    conf.update({
        'yarn.resourcemanager.resourcetracker.address': '{namenode}:8025',
        'yarn.resourcemanager.scheduler.address': '{namenode}:8030',
        'yarn.resourcemanager.address': '{namenode}:8050',
        'yarn.resourcemanager.admin.address': '{namenode}:8041',
        'yarn.resourcemanager.hostname': '{namenode}',
        'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
        'yarn.nodemanager.sleep-delay-before-sigkill.ms': '10000',
    })
    data_tmpl = dict_to_hadoop_conf(conf)

    tempdir = teuthology.get_testdir(ctx)
    path = "{tdir}/hadoop/etc/hadoop/yarn-site.xml".format(tdir=tempdir)
    nodes = ctx.cluster.only(is_hadoop_type('master'))
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

def configure(ctx, config, hadoops):
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
        if remote.os.package_type == 'rpm':
            data = "JAVA_HOME=/usr/lib/jvm/java\n"
        elif remote.os.package_type == 'deb':
            data = "JAVA_HOME=/usr/lib/jvm/default-java\n"
        else:
            raise UnsupportedPackageTypeError(remote)
        teuthology.prepend_lines_to_file(remote, path, data)

    if config.get('hdfs', False):
        log.info("Formatting HDFS...")
        testdir = teuthology.get_testdir(ctx)
        hadoop_dir = "{tdir}/hadoop/".format(tdir=testdir)
        masters = ctx.cluster.only(is_hadoop_type('master'))
        assert len(masters.remotes) == 1
        master = masters.remotes.keys()[0]
        master.run(
            args = [
                hadoop_dir + "bin/hadoop",
                "namenode",
                "-format"
            ],
            wait = True,
            )

@contextlib.contextmanager
def install_hadoop(ctx, config):
    testdir = teuthology.get_testdir(ctx)

    log.info("Downloading Hadoop...")
    hadoop_tarball = "{tdir}/hadoop.tar.gz".format(tdir=testdir)
    hadoops = ctx.cluster.only(is_hadoop_type(''))
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

    if not config.get('hdfs', False):
        log.info("Fetching cephfs-hadoop...")

        sha1, url = teuthology.get_ceph_binary_url(
                package = "hadoop",
                format = "jar",
                dist = "precise",
                arch = "x86_64",
                flavor = "basic",
                branch = "master")

        run.wait(
            hadoops.run(
                args = [
                    'wget',
                    '-nv',
                    '-O',
                    "{tdir}/cephfs-hadoop.jar".format(tdir=testdir), # FIXME
                    url + "/cephfs-hadoop-0.80.6.jar", # FIXME
                ],
                wait = False,
                )
            )

        run.wait(
            hadoops.run(
                args = [
                    'mv',
                    "{tdir}/cephfs-hadoop.jar".format(tdir=testdir),
                    "{tdir}/hadoop/share/hadoop/common/".format(tdir=testdir),
                ],
                wait = False,
                )
            )

        # Copy JNI native bits. Need to do this explicitly because the
        # handling is dependent on the os-type.
        for remote in hadoops.remotes:
            libcephfs_jni_path = None
            if remote.os.package_type == 'rpm':
                libcephfs_jni_path = "/usr/lib64/libcephfs_jni.so.1.0.0"
            elif remote.os.package_type == 'deb':
                libcephfs_jni_path = "/usr/lib/jni/libcephfs_jni.so"
            else:
                raise UnsupportedPackageTypeError(remote)

            libcephfs_jni_fname = "libcephfs_jni.so"
            remote.run(
                args = [
                    'cp',
                    libcephfs_jni_path,
                    "{tdir}/hadoop/lib/native/{fname}".format(tdir=testdir,
                        fname=libcephfs_jni_fname),
                ])

        run.wait(
            hadoops.run(
                args = [
                    'cp',
                    "/usr/share/java/libcephfs.jar",
                    "{tdir}/hadoop/share/hadoop/common/".format(tdir=testdir),
                ],
                wait = False,
                )
            )

    configure(ctx, config, hadoops)

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
    masters = ctx.cluster.only(is_hadoop_type('master'))
    assert len(masters.remotes) == 1
    master = masters.remotes.keys()[0]

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

    if config.get('hdfs', False):
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
