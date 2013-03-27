from cStringIO import StringIO

import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel
from ..orchestra import run

log = logging.getLogger(__name__)

###################
# This installeds and configures Hadoop, but requires that Ceph is already installed and running.
##################

## Check that there is exactly one master and at least one slave configured
@contextlib.contextmanager
def validate_config(ctx, config):
    log.info('Vaidating Hadoop configuration')
    slaves = ctx.cluster.only(teuthology.is_type('hadoop.slave'))

    if (len(slaves.remotes) < 1):
        raise Exception("At least one hadoop.slave must be specified")
    else:
        log.info(str(len(slaves.remotes)) + " slaves specified")

    masters = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    if (len(masters.remotes) == 1):
        pass
    else:
        raise Exception("Exactly one hadoop.master must be specified. Currently there are " + str(len(masters.remotes)))

    try: 
        yield

    finally:
        pass

## Add required entries to conf/hadoop-env.sh
def write_hadoop_env(ctx, config):
    hadoopEnvFile = "{tdir}/apache_hadoop/conf/hadoop-env.sh".format(tdir=teuthology.get_testdir(ctx))

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote, hadoopEnvFile, 
'''export JAVA_HOME=/usr/lib/jvm/default-java
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/share/java/libcephfs.jar:{tdir}/apache_hadoop/build/hadoop-core*.jar:{tdir}/inktank_hadoop/build/hadoop-cephfs.jar
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS"
'''.format(tdir=teuthology.get_testdir(ctx))     )
        log.info("wrote file: " + hadoopEnvFile + " to host: " + str(remote))

## Add required entries to conf/core-site.xml
def write_core_site(ctx, config):
    testdir = teuthology.get_testdir(ctx)
    coreSiteFile = "{tdir}/apache_hadoop/conf/core-site.xml".format(tdir=testdir)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():

        # check the config to see if we should use hdfs or ceph
        default_fs_string = ""
        if config.get('hdfs'):
            default_fs_string = 'hdfs://{master_ip}:54310'.format(master_ip=get_hadoop_master_ip(ctx))
        else:
            default_fs_string = 'ceph:///'

        teuthology.write_file(remote, coreSiteFile, 
'''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file.  -->
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop/tmp</value>
    </property>
    <property>
        <name>fs.default.name</name>
        <value>{default_fs}</value>
    </property>
    <property>
      <name>ceph.conf.file</name>
      <value>/etc/ceph/ceph.conf</value>
    </property>
    <property>
      <name>fs.ceph.impl</name>
      <value>org.apache.hadoop.fs.ceph.CephFileSystem</value>
    </property>
</configuration>
'''.format(tdir=teuthology.get_testdir(ctx), default_fs=default_fs_string))

        log.info("wrote file: " + coreSiteFile + " to host: " + str(remote))

## finds the hadoop.master in the ctx and then pulls out just the IP address
def get_hadoop_master_ip(ctx):
    remote, _ = _get_master(ctx)
    master_name,master_port= remote.ssh.get_transport().getpeername()
    log.info('master name: {name} port {port}'.format(name=master_name,port=master_port))
    return master_name

## Add required entries to conf/mapred-site.xml
def write_mapred_site(ctx):
    mapredSiteFile = "{tdir}/apache_hadoop/conf/mapred-site.xml".format(tdir=teuthology.get_testdir(ctx))

    master_ip = get_hadoop_master_ip(ctx)
    log.info('adding host {remote} as jobtracker'.format(remote=master_ip))

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote, mapredSiteFile, 
'''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
    <property>
        <name>mapred.job.tracker</name>
        <value>{remote}:54311</value>
    </property>
</configuration>  
'''.format(remote=master_ip))

        log.info("wrote file: " + mapredSiteFile + " to host: " + str(remote))

## Add required entries to conf/hdfs-site.xml
def write_hdfs_site(ctx):
    hdfsSiteFile = "{tdir}/apache_hadoop/conf/hdfs-site.xml".format(tdir=teuthology.get_testdir(ctx))

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote, hdfsSiteFile, 
'''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
'''     )
        log.info("wrote file: " + hdfsSiteFile + " to host: " + str(remote))

## Add required entries to conf/slaves 
## These nodes host TaskTrackers and DataNodes
def write_slaves(ctx):
    log.info('Setting up slave nodes...')

    slavesFile = "{tdir}/apache_hadoop/conf/slaves".format(tdir=teuthology.get_testdir(ctx))
    tmpFile = StringIO()

    slaves = ctx.cluster.only(teuthology.is_type('hadoop.slave'))
    for remote, roles_for_host in slaves.remotes.iteritems():
        tmpFile.write('{remote}\n'.format(remote=remote.ssh.get_transport().getpeername()[0]))

    tmpFile.seek(0)

    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote=remote, path=slavesFile, data=tmpFile)
        tmpFile.seek(0)
        log.info("wrote file: " + slavesFile + " to host: " + str(remote))

## Add required entries to conf/masters 
## These nodes host JobTrackers and Namenodes
def write_master(ctx):
    mastersFile = "{tdir}/apache_hadoop/conf/masters".format(tdir=teuthology.get_testdir(ctx))
    master = _get_master(ctx)
    master_remote, _ = master


    hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
    for remote, roles_for_host in hadoopNodes.remotes.iteritems():
        teuthology.write_file(remote, mastersFile, '{master_host}\n'.format(master_host=master_remote.ssh.get_transport().getpeername()[0]))
        log.info("wrote file: " + mastersFile + " to host: " + str(remote))

## Call the various functions that configure Hadoop
def _configure_hadoop(ctx, config):
    log.info('writing out config files')

    write_hadoop_env(ctx, config)
    write_core_site(ctx, config)
    write_mapred_site(ctx)
    write_hdfs_site(ctx)
    write_slaves(ctx)
    write_master(ctx)



@contextlib.contextmanager
def configure_hadoop(ctx, config):
    _configure_hadoop(ctx,config)

    log.info('config.get(hdfs): {hdfs}'.format(hdfs=config.get('hdfs')))

    if config.get('hdfs'):
        log.info('hdfs option specified. Setting up hdfs')

        # let's run this from the master
        master = _get_master(ctx)
        remote, _ = master
        remote.run(
        args=["{tdir}/apache_hadoop/bin/hadoop".format(tdir=teuthology.get_testdir(ctx)),
              "namenode",
              "-format"],
            wait=True,
        )

    log.info('done setting up hadoop')

    try: 
        yield

    finally:
        log.info('Removing hdfs directory')
        run.wait(
            ctx.cluster.run(
                args=[
                    'rm',
                    '-rf',
                    '/tmp/hadoop',
                    ],
                wait=False,
                ),
            )

def _start_hadoop(ctx, remote, config):
    testdir = teuthology.get_testdir(ctx)
    if config.get('hdfs'):
        remote.run(
            args=['{tdir}/apache_hadoop/bin/start-dfs.sh'.format(tdir=testdir), ],
            wait=True,
        )
        log.info('done starting hdfs')

    remote.run(
        args=['{tdir}/apache_hadoop/bin/start-mapred.sh'.format(tdir=testdir), ],
        wait=True,
    )
    log.info('done starting mapred')


def _stop_hadoop(ctx, remote, config):
    testdir = teuthology.get_testdir(ctx)
    remote.run(
        args=['{tdir}/apache_hadoop/bin/stop-mapred.sh'.format(tdir=testdir), ],
        wait=True,
    )

    if config.get('hdfs'):
        remote.run(
            args=['{tdir}/apache_hadoop/bin/stop-dfs.sh'.format(tdir=testdir), ],
            wait=True,
        )

    log.info('done stopping hadoop')

def _get_master(ctx):
    master = ctx.cluster.only(teuthology.is_type('hadoop.master'))
    assert 1 == len(master.remotes.items()), 'There must be exactly 1 hadoop.master configured'

    return master.remotes.items()[0]

@contextlib.contextmanager
def start_hadoop(ctx, config):
    master = _get_master(ctx)
    remote, _ = master

    log.info('Starting hadoop on {remote}\n'.format(remote=remote.ssh.get_transport().getpeername()[0]))
    _start_hadoop(ctx, remote, config)

    try: 
        yield

    finally:
        log.info('Running stop-mapred.sh on {remote}'.format(remote=remote.ssh.get_transport().getpeername()[0]))
        _stop_hadoop(ctx, remote, config)

# download and untar the most recent apache hadoop binaries into {testdir}/apache_hadoop
def _download_apache_hadoop_binaries(ctx, remote, hadoop_url):
    log.info('_download_apache_hadoop_binaries: path {path} on host {host}'.format(path=hadoop_url, host=str(remote)))
    fileName = 'apache-hadoop.tgz'
    testdir = teuthology.get_testdir(ctx)
    remote.run(
        args=[
            'mkdir', '-p', '-m0755', '{tdir}/apache_hadoop'.format(tdir=testdir),
            run.Raw('&&'),
            'echo',
            '{fileName}'.format(fileName=fileName),
            run.Raw('|'),
            'wget',
            '-nv',
            '-O-',
            '--base={url}'.format(url=hadoop_url),
            # need to use --input-file to make wget respect --base
            '--input-file=-',
            run.Raw('|'),
            'tar', '-xzf', '-', '-C', '{tdir}/apache_hadoop'.format(tdir=testdir),
        ],
    )

# download and untar the most recent Inktank hadoop binaries into {testdir}/hadoop
def _download_inktank_hadoop_binaries(ctx, remote, hadoop_url):
    log.info('_download_inktank_hadoop_binaries: path {path} on host {host}'.format(path=hadoop_url, host=str(remote)))
    fileName = 'hadoop.tgz'
    testdir = teuthology.get_testdir(ctx)
    remote.run(
        args=[
            'mkdir', '-p', '-m0755', '{tdir}/inktank_hadoop'.format(tdir=testdir),
            run.Raw('&&'),
            'echo',
            '{fileName}'.format(fileName=fileName),
            run.Raw('|'),
            'wget',
            '-nv',
            '-O-',
            '--base={url}'.format(url=hadoop_url),
            # need to use --input-file to make wget respect --base
            '--input-file=-',
            run.Raw('|'),
            'tar', '-xzf', '-', '-C', '{tdir}/inktank_hadoop'.format(tdir=testdir),
        ],
    )

# copy hadoop-cephfs.jar and hadoop-cephfs-test.jar into apache_hadoop
def _copy_hadoop_cephfs_jars(ctx, remote, from_dir, to_dir):
    testdir = teuthology.get_testdir(ctx)
    log.info('copy jars from {from_dir} to {to_dir} on host {host}'.format(from_dir=from_dir, to_dir=to_dir, host=str(remote)))
    file_names = [ 'hadoop-cephfs.jar', 'hadoop-cephfs-test.jar' ] 
    for file_name in file_names:
        log.info('Copying file {file_name}'.format(file_name=file_name))
        remote.run(
            args=[ 'cp', '{tdir}/{from_dir}/{file_name}'.format(tdir=testdir,from_dir=from_dir,file_name=file_name),
                '{tdir}/{to_dir}/'.format(tdir=testdir,to_dir=to_dir)
            ],
        )

def _node_binaries(ctx, config, remote, inktank_hadoop_bindir_url, apache_hadoop_bindir_url):
    _download_inktank_hadoop_binaries(ctx, remote, inktank_hadoop_bindir_url)
    _download_apache_hadoop_binaries(ctx, remote, apache_hadoop_bindir_url)
    _copy_hadoop_cephfs_jars(ctx, remote, 'inktank_hadoop/build', 'apache_hadoop/build')

@contextlib.contextmanager
def binaries(ctx, config):
    path = config.get('path')

    if path is None:
        # fetch Apache Hadoop from gitbuilder
        log.info('Fetching and unpacking Apache Hadoop binaries from gitbuilder...')
        apache_sha1, apache_hadoop_bindir_url = teuthology.get_ceph_binary_url(
            package='apache-hadoop',
            branch=config.get('apache_branch'),
            tag=config.get('tag'),
            sha1=config.get('sha1'),
            flavor=config.get('flavor'),
            format=config.get('format'),
            dist=config.get('dist'),
            arch=config.get('arch'),
            )
        log.info('apache_hadoop_bindir_url %s' % (apache_hadoop_bindir_url))
        ctx.summary['apache-hadoop-sha1'] = apache_sha1

        # fetch Inktank Hadoop from gitbuilder
        log.info('Fetching and unpacking Inktank Hadoop binaries from gitbuilder...')
        inktank_sha1, inktank_hadoop_bindir_url = teuthology.get_ceph_binary_url(
            package='hadoop',
            branch=config.get('inktank_branch'),
            tag=config.get('tag'),
            sha1=config.get('sha1'),
            flavor=config.get('flavor'),
            format=config.get('format'),
            dist=config.get('dist'),
            arch=config.get('arch'),
            )
        log.info('inktank_hadoop_bindir_url %s' % (inktank_hadoop_bindir_url))
        ctx.summary['inktank-hadoop-sha1'] = inktank_sha1

    else:
        raise Exception("The hadoop task does not support the path argument at present")

    with parallel() as p:
        hadoopNodes = ctx.cluster.only(teuthology.is_type('hadoop'))
        # these can happen independently
        for remote in hadoopNodes.remotes.iterkeys():
            p.spawn(_node_binaries, ctx, config, remote, inktank_hadoop_bindir_url, apache_hadoop_bindir_url)

    try:
        yield
    finally:
        log.info('Removing hadoop binaries...')
        run.wait(
            ctx.cluster.run(
                args=[ 'rm', '-rf', '--', '{tdir}/apache_hadoop'.format(tdir=teuthology.get_testdir(ctx))],
                wait=False,
                ),
            )
        run.wait(
            ctx.cluster.run(
                args=[ 'rm', '-rf', '--', '{tdir}/inktank_hadoop'.format(tdir=teuthology.get_testdir(ctx))],
                wait=False,
                ),
            )

## A Hadoop NameNode will stay in safe mode for 30 seconds by default.
## This method blocks until the NameNode is out of safe mode.
@contextlib.contextmanager
def out_of_safemode(ctx, config):

    if config.get('hdfs'):
        log.info('Waiting for the Namenode to exit safe mode...')

        master = _get_master(ctx)
        remote, _ = master
        remote.run(
            args=["{tdir}/apache_hadoop/bin/hadoop".format(tdir=teuthology.get_testdir(ctx)),
                  "dfsadmin",
                  "-safemode",
                  "wait"],
            wait=True,
        )
    else:
        pass

    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Hadoop cluster.

    This depends on either having ceph installed prior to hadoop, like so:

    roles:
    - [mon.0, mds.0, osd.0, hadoop.master.0]
    - [mon.1, osd.1, hadoop.slave.0]
    - [mon.2, hadoop.slave.1]

    tasks:
    - ceph:
    - hadoop:

    Or if you want to use HDFS under Hadoop, this will configure Hadoop
    for HDFS and start it along with MapReduce. Note that it does not 
    require Ceph be installed.

    roles:
    - [hadoop.master.0]
    - [hadoop.slave.0]
    - [hadoop.slave.1]

    tasks:
    - hadoop:
        hdfs: True

    This task requires exactly one hadoop.master be specified 
    and at least one hadoop.slave.

    This does *not* do anything with the Hadoop setup. To run wordcount, 
    you could use pexec like so (after the hadoop task):

    - pexec: 
        hadoop.slave.0:
          - mkdir -p /tmp/hadoop_input
          - wget http://ceph.com/qa/hadoop_input_files.tar -O /tmp/hadoop_input/files.tar
          - cd /tmp/hadoop_input/; tar -xf /tmp/hadoop_input/files.tar
          - {tdir}/hadoop/bin/hadoop fs -mkdir wordcount_input
          - {tdir}/hadoop/bin/hadoop fs -put /tmp/hadoop_input/*txt wordcount_input/
          - {tdir}/hadoop/bin/hadoop jar {tdir}/hadoop/build/hadoop-example*jar wordcount wordcount_input wordcount_output
          - rm -rf /tmp/hadoop_input
    """.format(tdir=teuthology.get_testdir(ctx))

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task hadoop only supports a dictionary for configuration"

    dist = 'precise' 
    format = 'jar'
    arch = 'x86_64'
    flavor = config.get('flavor', 'basic')

    ctx.summary['flavor'] = flavor

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('hadoop', {}))

    apache_branch = None
    if config.get('apache_hadoop_branch') is not None:
        apache_branch = config.get('apache_hadoop_branch')
    else:
        apache_branch = 'branch-1.0' # hadoop branch to acquire

    inktank_branch = None
    if config.get('inktank_hadoop_branch') is not None:
        inktank_branch = config.get('inktank_hadoop_branch')
    else:
        inktank_branch = 'cephfs/branch-1.0' # default branch name

    # replace any '/' with a '_' to match the artifact paths
    inktank_branch = inktank_branch.replace('/','_')
    apache_branch = apache_branch.replace('/','_')

    with contextutil.nested(
        lambda: validate_config(ctx=ctx, config=config),
        lambda: binaries(ctx=ctx, config=dict(
                tag=config.get('tag'),
                sha1=config.get('sha1'),
                path=config.get('path'),
                flavor=flavor,
                dist=config.get('dist', dist),
                format=format,
                arch=arch,
                apache_branch=apache_branch,
                inktank_branch=inktank_branch,
                )),
        lambda: configure_hadoop(ctx=ctx, config=config),
        lambda: start_hadoop(ctx=ctx, config=config),
        lambda: out_of_safemode(ctx=ctx, config=config),
        ):
        yield
