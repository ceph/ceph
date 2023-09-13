import contextlib
import logging
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
           maven-version: '3.6.3' (default)
           hadoop-version: '2.9.2'
           bucket-name: 's3atest' (default)
           access-key: 'anykey' (uses a default value)
           secret-key: 'secretkey' ( uses a default value)
           role: client.0
    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    assert hasattr(ctx, 'rgw'), 's3a-hadoop must run after the rgw task'

    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('s3a-hadoop', {}))
    testdir = misc.get_testdir(ctx)

    role = config.get('role')
    (remote,) = ctx.cluster.only(role).remotes.keys()
    endpoint = ctx.rgw.role_endpoints.get(role)
    assert endpoint, 's3tests: no rgw endpoint for {}'.format(role)

    # get versions
    maven_major = config.get('maven-major', 'maven-3')
    maven_version = config.get('maven-version', '3.6.3')
    hadoop_ver = config.get('hadoop-version', '2.9.2')
    bucket_name = config.get('bucket-name', 's3atest')
    access_key = config.get('access-key', 'EGAQRD2ULOIFKFSKCT4F')
    secret_key = config.get(
        'secret-key',
        'zi816w1vZKfaSM85Cl0BxXTwSLyN7zB4RbTswrGb')

    # set versions for cloning the repo
    apache_maven = 'apache-maven-{maven_version}-bin.tar.gz'.format(
        maven_version=maven_version)
    maven_link = 'http://archive.apache.org/dist/maven/' + \
        '{maven_major}/{maven_version}/binaries/'.format(maven_major=maven_major, maven_version=maven_version) + apache_maven
    hadoop_git = 'https://github.com/apache/hadoop'
    hadoop_rel = 'hadoop-{ver} rel/release-{ver}'.format(ver=hadoop_ver)
    if hadoop_ver == 'trunk':
        # just checkout a new branch out of trunk
        hadoop_rel = 'hadoop-ceph-trunk'
    install_prereq(remote)
    remote.run(
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
    configure_s3a(remote, endpoint.dns_name, access_key, secret_key, bucket_name, testdir)
    setup_user_bucket(remote, endpoint.dns_name, access_key, secret_key, bucket_name, testdir)
    if hadoop_ver.startswith('2.8'):
        # test all ITtests but skip AWS test using public bucket landsat-pds
        # which is not available from within this test
        test_options = '-Dit.test=ITestS3A* -Dparallel-tests -Dscale \
                        -Dfs.s3a.scale.test.timeout=1200 \
                        -Dfs.s3a.scale.test.huge.filesize=256M verify'
    else:
        test_options = 'test -Dtest=S3a*,TestS3A*'
    try:
        run_s3atest(remote, maven_version, testdir, test_options)
        yield
    finally:
        log.info("Done s3a testing, Cleaning up")
        for fil in ['apache*', 'hadoop*', 'venv*', 'create*']:
            remote.run(args=['rm', run.Raw('-rf'), run.Raw('{tdir}/{file}'.format(tdir=testdir, file=fil))])


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
            run.Raw('--display-name="s3a cephtests"'),
            run.Raw('--access-key={access_key}'.format(access_key=access_key)),
            run.Raw('--secret-key={secret_key}'.format(secret_key=secret_key)),
            run.Raw('--email=s3a@ceph.com'),
        ]
    )
    client.run(
        args=[
            'python3',
            '-m',
            'venv',
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
        print(bucket.name + "\t" + bucket.creation_date)
""".format(access_key=access_key, secret_key=secret_key, dns_name=dns_name, bucket_name=bucket_name)
    py_bucket_file = '{testdir}/create_bucket.py'.format(testdir=testdir)
    client.sudo_write_file(py_bucket_file, create_bucket, mode='0744')
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
    client.write_file(config_path, config_template)
    # output for debug
    client.run(args=['cat', config_path])
