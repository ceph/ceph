import contextlib
import logging
import time
from teuthology import misc
from teuthology.orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
     Run Hadoop S3A tests using Ceph
     usage:
      -tasks:
         ceph-ansible:
         s3a-hadoop:
           maven-version: '3.3.9' (default)
           hadoop-version: '2.7.3'
           bucket-name: 's3atest' (default)
           access-key: 'anykey' (uses a default value)
           secret-key: 'secretkey' ( uses a default value)
           dnsmasq-name: 's3.ceph.com'
    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('s3a-hadoop', {}))
    testdir = misc.get_testdir(ctx)
    rgws = ctx.cluster.only(misc.is_type('rgw'))
    # use the first rgw node to test s3a
    rgw_node = rgws.remotes.keys()[0]
    # get versions
    maven_major = config.get('maven-major', 'maven-3')
    maven_version = config.get('maven-version', '3.6.0')
    hadoop_ver = config.get('hadoop-version', '2.9.2')
    bucket_name = config.get('bucket-name', 's3atest')
    access_key = config.get('access-key', 'EGAQRD2ULOIFKFSKCT4F')
    dnsmasq_name = config.get('dnsmasq-name', 's3.ceph.com')
    secret_key = config.get(
        'secret-key',
        'zi816w1vZKfaSM85Cl0BxXTwSLyN7zB4RbTswrGb')

    # set versions for cloning the repo
    apache_maven = 'apache-maven-{maven_version}-bin.tar.gz'.format(
        maven_version=maven_version)
    maven_link = 'http://www-us.apache.org/dist/maven/' + \
        '{maven_major}/{maven_version}/binaries/'.format(maven_major=maven_major, maven_version=maven_version) + apache_maven
    hadoop_git = 'https://github.com/apache/hadoop'
    hadoop_rel = 'hadoop-{ver} rel/release-{ver}'.format(ver=hadoop_ver)
    if hadoop_ver == 'trunk':
        # just checkout a new branch out of trunk
        hadoop_rel = 'hadoop-ceph-trunk'
    install_prereq(rgw_node)
    rgw_node.run(
        args=[
            'cd',
            testdir,
            run.Raw('&&'),
            'wget',
            maven_link,
            run.Raw('&&'),
            'tar',
            '-xvf',
            apache_maven,
            run.Raw('&&'),
            'git',
            'clone',
            run.Raw(hadoop_git),
            run.Raw('&&'),
            'cd',
            'hadoop',
            run.Raw('&&'),
            'git',
            'checkout',
            '-b',
            run.Raw(hadoop_rel)
        ]
    )
    configure_s3a(rgw_node, dnsmasq_name, access_key, secret_key, bucket_name, testdir)
    fix_rgw_config(rgw_node, dnsmasq_name)
    setup_user_bucket(rgw_node, dnsmasq_name, access_key, secret_key, bucket_name, testdir)
    if hadoop_ver.startswith('2.8'):
        # test all ITtests but skip AWS test using public bucket landsat-pds
        # which is not available from within this test
        test_options = '-Dit.test=ITestS3A* -Dparallel-tests -Dscale \
                        -Dfs.s3a.scale.test.timeout=1200 \
                        -Dfs.s3a.scale.test.huge.filesize=256M verify'
    else:
        test_options = 'test -Dtest=S3a*,TestS3A*'
    try:
        run_s3atest(rgw_node, maven_version, testdir, test_options)
        yield
    finally:
        log.info("Done s3a testing, Cleaning up")
        for fil in ['apache*', 'hadoop*', 'venv*', 'create*']:
            rgw_node.run(args=['rm', run.Raw('-rf'), run.Raw('{tdir}/{file}'.format(tdir=testdir, file=fil))])


def install_prereq(client):
    """
    Install pre requisites for RHEL and CentOS
    TBD: Ubuntu
    """
    if client.os.name == 'rhel' or client.os.name == 'centos':
        client.run(
               args=[
                    'sudo',
                    'yum',
                    'install',
                    '-y',
                    'protobuf-c.x86_64',
                    'java',
                    'java-1.8.0-openjdk-devel',
                    'dnsmasq'
                    ]
                )


def fix_rgw_config(client, name):
    """
    Fix RGW config in ceph.conf, we need rgw dns name entry
    and also modify the port to use :80 for s3a tests to work
    """
    rgw_dns_name = 'rgw dns name = {name}'.format(name=name)
    ceph_conf_path = '/etc/ceph/ceph.conf'
    # append rgw_dns_name
    client.run(
        args=[
            'sudo',
            'sed',
            run.Raw('-i'),
            run.Raw("'/client.rgw*/a {rgw_name}'".format(rgw_name=rgw_dns_name)),
            ceph_conf_path

        ]
    )
    # listen on port 80
    client.run(
        args=[
            'sudo',
            'sed',
            run.Raw('-i'),
            run.Raw('s/:8080/:80/'),
            ceph_conf_path
        ]
    )
    client.run(args=['cat', ceph_conf_path])
    client.run(args=['sudo', 'systemctl', 'restart', 'ceph-radosgw.target'])
    client.run(args=['sudo', 'systemctl', 'status', 'ceph-radosgw.target'])
    # sleep for daemon to be completely up before creating admin user
    time.sleep(10)


def setup_user_bucket(client, dns_name, access_key, secret_key, bucket_name, testdir):
    """
    Create user with access_key and secret_key that will be
    used for the s3a testdir
    """
    client.run(
        args=[
            'sudo',
            'radosgw-admin',
            'user',
            'create',
            run.Raw('--uid'),
            's3a',
            run.Raw('--display-name=s3a cephtests'),
            run.Raw('--access-key={access_key}'.format(access_key=access_key)),
            run.Raw('--secret-key={secret_key}'.format(secret_key=secret_key)),
            run.Raw('--email=s3a@ceph.com'),
        ]
    )
    client.run(
        args=[
            'virtualenv',
            '{testdir}/venv'.format(testdir=testdir),
            run.Raw('&&'),
            run.Raw('{testdir}/venv/bin/pip'.format(testdir=testdir)),
            'install',
            'boto'
        ]
    )
    create_bucket = """
#!/usr/bin/env python
import boto
import boto.s3.connection
access_key = '{access_key}'
secret_key = '{secret_key}'

conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = '{dns_name}',
        is_secure=False,
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
bucket = conn.create_bucket('{bucket_name}')
for bucket in conn.get_all_buckets():
        print bucket.name + "\t" + bucket.creation_date
""".format(access_key=access_key, secret_key=secret_key, dns_name=dns_name, bucket_name=bucket_name)
    py_bucket_file = '{testdir}/create_bucket.py'.format(testdir=testdir)
    misc.sudo_write_file(
        remote=client,
        path=py_bucket_file,
        data=create_bucket,
        perms='0744',
        )
    client.run(
        args=[
            'cat',
            '{testdir}/create_bucket.py'.format(testdir=testdir),
        ]
    )
    client.run(
        args=[
            '{testdir}/venv/bin/python'.format(testdir=testdir),
            '{testdir}/create_bucket.py'.format(testdir=testdir),
        ]
    )


def run_s3atest(client, maven_version, testdir, test_options):
    """
    Finally run the s3a test
    """
    aws_testdir = '{testdir}/hadoop/hadoop-tools/hadoop-aws/'.format(testdir=testdir)
    run_test = '{testdir}/apache-maven-{maven_version}/bin/mvn'.format(testdir=testdir, maven_version=maven_version)
    # Remove AWS CredentialsProvider tests as it hits public bucket from AWS
    # better solution is to create the public bucket on local server and test
    rm_test = 'rm src/test/java/org/apache/hadoop/fs/s3a/ITestS3AAWSCredentialsProvider.java'
    client.run(
        args=[
            'cd',
            run.Raw(aws_testdir),
            run.Raw('&&'),
            run.Raw(rm_test),
            run.Raw('&&'),
            run.Raw(run_test),
            run.Raw(test_options)
        ]
    )


def configure_s3a(client, dns_name, access_key, secret_key, bucket_name, testdir):
    """
    Use the template to configure s3a test, Fill in access_key, secret_key
    and other details required for test.
    """
    config_template = """<configuration>
<property>
<name>fs.s3a.endpoint</name>
<value>{name}</value>
</property>

<property>
<name>fs.contract.test.fs.s3a</name>
<value>s3a://{bucket_name}/</value>
</property>

<property>
<name>fs.s3a.connection.ssl.enabled</name>
<value>false</value>
</property>

<property>
<name>test.fs.s3n.name</name>
<value>s3n://{bucket_name}/</value>
</property>

<property>
<name>test.fs.s3a.name</name>
<value>s3a://{bucket_name}/</value>
</property>

<property>
<name>test.fs.s3.name</name>
<value>s3://{bucket_name}/</value>
</property>

<property>
<name>fs.s3.awsAccessKeyId</name>
<value>{access_key}</value>
</property>

<property>
<name>fs.s3.awsSecretAccessKey</name>
<value>{secret_key}</value>
</property>

<property>
<name>fs.s3n.awsAccessKeyId</name>
<value>{access_key}</value>
</property>

<property>
<name>fs.s3n.awsSecretAccessKey</name>
<value>{secret_key}</value>
</property>

<property>
<name>fs.s3a.access.key</name>
<description>AWS access key ID. Omit for Role-based authentication.</description>
<value>{access_key}</value>
</property>

<property>
<name>fs.s3a.secret.key</name>
<description>AWS secret key. Omit for Role-based authentication.</description>
<value>{secret_key}</value>
</property>
</configuration>
""".format(name=dns_name, bucket_name=bucket_name, access_key=access_key, secret_key=secret_key)
    config_path = testdir + '/hadoop/hadoop-tools/hadoop-aws/src/test/resources/auth-keys.xml'
    misc.write_file(
        remote=client,
        path=config_path,
        data=config_template,
    )
    # output for debug
    client.run(args=['cat', config_path])
