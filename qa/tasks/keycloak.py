"""
Deploy and configure Keycloak for Teuthology
"""
import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)

def get_keycloak_version(config):
    for client, client_config in config.items():
        if 'keycloak_version' in client_config:
            keycloak_version = client_config.get('keycloak_version')
    return keycloak_version

def get_keycloak_dir(ctx, config):
    keycloak_version = get_keycloak_version(config)
    current_version = 'keycloak-'+keycloak_version
    return '{tdir}/{ver}'.format(tdir=teuthology.get_testdir(ctx),ver=current_version)

def run_in_keycloak_dir(ctx, client, config, args, **kwargs):
    return ctx.cluster.only(client).run(
        args=[ 'cd', get_keycloak_dir(ctx,config), run.Raw('&&'), ] + args,
        **kwargs
    )

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)

@contextlib.contextmanager
def install_packages(ctx, config):
    """
    Downloading the two required tar files
    1. Keycloak
    2. Wildfly (Application Server)
    """
    assert isinstance(config, dict)
    log.info('Installing packages for Keycloak...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        test_dir=teuthology.get_testdir(ctx)
        current_version = get_keycloak_version(config)
        link1 = 'https://downloads.jboss.org/keycloak/'+current_version+'/keycloak-'+current_version+'.tar.gz'
        toxvenv_sh(ctx, remote, ['wget', link1])
        
        file1 = 'keycloak-'+current_version+'.tar.gz'
        toxvenv_sh(ctx, remote, ['tar', '-C', test_dir, '-xvzf', file1])

        link2 ='https://downloads.jboss.org/keycloak/'+current_version+'/adapters/keycloak-oidc/keycloak-wildfly-adapter-dist-'+current_version+'.tar.gz' 
        toxvenv_sh(ctx, remote, ['cd', '{tdir}'.format(tdir=get_keycloak_dir(ctx,config)), run.Raw('&&'), 'wget', link2])
        
        file2 = 'keycloak-wildfly-adapter-dist-'+current_version+'.tar.gz'
        toxvenv_sh(ctx, remote, ['tar', '-C', '{tdir}'.format(tdir=get_keycloak_dir(ctx,config)), '-xvzf', '{tdr}/{file}'.format(tdr=get_keycloak_dir(ctx,config),file=file2)])

    try:
        yield
    finally:
        log.info('Removing packaged dependencies of Keycloak...')
        for client in config:
            ctx.cluster.only(client).run(
                args=['rm', '-rf', '{tdir}'.format(tdir=get_keycloak_dir(ctx,config))],
            )

@contextlib.contextmanager
def download_conf(ctx, config):
    """
    Downloads confi.py used in run_admin_cmds
    """
    assert isinstance(config, dict)
    log.info('Downloading conf...')
    testdir = teuthology.get_testdir(ctx)
    conf_branch = 'main'
    conf_repo = 'https://github.com/TRYTOBE8TME/scripts.git'
    for (client, _) in config.items():
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', conf_branch,
                conf_repo,
                '{tdir}/scripts'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing conf...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/scripts'.format(tdir=testdir),
                    ],
                )

@contextlib.contextmanager
def build(ctx,config):
    """
    Build process which needs to be done before starting a server.
    """
    assert isinstance(config, dict)
    log.info('Building Keycloak...')
    for (client,_) in config.items():
        run_in_keycloak_dir(ctx, client, config,['cd', 'bin', run.Raw('&&'), './jboss-cli.sh', '--file=adapter-elytron-install-offline.cli'])
    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def run_keycloak(ctx,config):
    """
    This includes two parts:
    1. Adding a user to keycloak which is actually used to log in when we start the server and check in browser.
    2. Starting the server.
    """
    assert isinstance(config, dict)
    log.info('Bringing up Keycloak...')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        
        ctx.cluster.only(client).run(
            args=[
                '{tdir}/bin/add-user-keycloak.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                '-r', 'master',
                '-u', 'admin',
                '-p', 'admin',
            ],
        )

        toxvenv_sh(ctx, remote, ['cd', '{tdir}/bin'.format(tdir=get_keycloak_dir(ctx,config)), run.Raw('&&'), './standalone.sh', run.Raw('&'), 'exit'])
    try:
        yield
    finally:
        log.info('Stopping Keycloak Server...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            toxvenv_sh(ctx, remote, ['cd', '{tdir}/bin'.format(tdir=get_keycloak_dir(ctx,config)), run.Raw('&&'), './jboss-cli.sh', '--connect', 'command=:shutdown'])

@contextlib.contextmanager
def run_admin_cmds(ctx,config):
    """
    Running Keycloak Admin commands(kcadm commands) in order to get the token, aud value, thumbprint and realm name.
    """
    assert isinstance(config, dict)
    log.info('Running admin commands...')
    for (client,_) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()

        remote.run(
           args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'config', 'credentials',
                '--server', 'http://localhost:8080/auth',
                '--realm', 'master',
                '--user', 'admin',
                '--password', 'admin',
                '--client', 'admin-cli',
                ],
            )

        realm_name='demorealm'
        realm='realm={}'.format(realm_name)

        remote.run(
           args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'create', 'realms',
                '-s', realm,
                '-s', 'enabled=true',
                '-s', 'accessTokenLifespan=1800',
                '-o',
            ],
        )

        client_name='my_client'
        client='clientId={}'.format(client_name)

        remote.run(
           args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'create', 'clients',
                '-r', realm_name,
                '-s', client,
                '-s', 'directAccessGrantsEnabled=true',
                '-s', 'redirectUris=["http://localhost:8080/myapp/*"]',
            ],
        )

        ans1= toxvenv_sh(ctx, remote, 
                 [
                  'cd', '{tdir}/bin'.format(tdir=get_keycloak_dir(ctx,config)), run.Raw('&&'), 
                  './kcadm.sh', 'get', 'clients', 
                  '-r', realm_name, 
                  '-F', 'id,clientId', run.Raw('|'), 
                  'jq', '-r', '.[] | select (.clientId == "my_client") | .id'
                 ])

        pre0=ans1.rstrip()
        pre1="clients/{}".format(pre0)

        remote.run(
            args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'update', pre1,
                '-r', realm_name,
                '-s', 'enabled=true',
                '-s', 'serviceAccountsEnabled=true',
                '-s', 'redirectUris=["http://localhost:8080/myapp/*"]',
            ],
        )

        ans2= pre1+'/client-secret'

        out2= toxvenv_sh(ctx, remote, 
                 [
                  'cd', '{tdir}/bin'.format(tdir=get_keycloak_dir(ctx,config)), run.Raw('&&'), 
                  './kcadm.sh', 'get', ans2, 
                  '-r', realm_name, 
                  '-F', 'value'
                 ])

        ans0= '{client}:{secret}'.format(client=client_name,secret=out2[15:51])
        ans3= 'client_secret={}'.format(out2[15:51])
        clientid='client_id={}'.format(client_name)

        proto_map = pre1+"/protocol-mappers/models"
        uname = "username=testuser"
        upass = "password=testuser"

        remote.run(
            args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'create', 'users',
                '-s', uname,
                '-s', 'enabled=true',
                '-s', 'attributes.\"https://aws.amazon.com/tags\"=\"{"principal_tags":{"Department":["Engineering", "Marketing"]}}\"',
                '-r', realm_name, 
            ],
        )

        sample = 'testuser'

        remote.run(
            args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'set-password',
                '-r', realm_name,
                '--username', sample,
                '--new-password', sample,
            ],
        )

        file_path = '{tdir}/scripts/confi.py'.format(tdir=teuthology.get_testdir(ctx))

        remote.run(
            args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'create', proto_map,
                '-r', realm_name,
                '-f', file_path,
            ],
        )

        remote.run(
            args=[
                '{tdir}/bin/kcadm.sh'.format(tdir=get_keycloak_dir(ctx,config)),
                'config', 'credentials',
                '--server', 'http://localhost:8080/auth',
                '--realm', realm_name,
                '--user', sample,
                '--password', sample,
                '--client', 'admin-cli',
            ],
        )

        out9= toxvenv_sh(ctx, remote,
                 [
                  'curl', '-k', '-v',
                  '-X', 'POST',
                  '-H', 'Content-Type:application/x-www-form-urlencoded',
                  '-d', 'scope=openid',
                  '-d', 'grant_type=password',
                  '-d', clientid,
                  '-d', ans3,
                  '-d', uname,
                  '-d', upass,
                  'http://localhost:8080/auth/realms/'+realm_name+'/protocol/openid-connect/token', run.Raw('|'),
                  'jq', '-r', '.access_token'
                 ])

        user_token_pre = out9.rstrip()
        user_token = '{}'.format(user_token_pre)

        out3= toxvenv_sh(ctx, remote, 
                 [
                  'curl', '-k', '-v', 
                  '-X', 'POST', 
                  '-H', 'Content-Type:application/x-www-form-urlencoded', 
                  '-d', 'scope=openid', 
                  '-d', 'grant_type=client_credentials', 
                  '-d', clientid, 
                  '-d', ans3, 
                  'http://localhost:8080/auth/realms/'+realm_name+'/protocol/openid-connect/token', run.Raw('|'), 
                  'jq', '-r', '.access_token'
                 ])

        pre2=out3.rstrip()
        acc_token= 'token={}'.format(pre2)
        ans4= '{}'.format(pre2)

        out4= toxvenv_sh(ctx, remote, 
                 [
                  'curl', '-k', '-v', 
                  '-X', 'GET', 
                  '-H', 'Content-Type:application/x-www-form-urlencoded', 
                  'http://localhost:8080/auth/realms/'+realm_name+'/protocol/openid-connect/certs', run.Raw('|'), 
                  'jq', '-r', '.keys[].x5c[]'
                 ])

        pre3=out4.rstrip()
        cert_value='{}'.format(pre3)
        start_value= "-----BEGIN CERTIFICATE-----\n"
        end_value= "\n-----END CERTIFICATE-----"
        user_data=""
        user_data+=start_value
        user_data+=cert_value
        user_data+=end_value

        remote.write_file(
            path='{tdir}/bin/certificate.crt'.format(tdir=get_keycloak_dir(ctx,config)),
            data=user_data
            )

        out5= toxvenv_sh(ctx, remote, 
                 [
                  'openssl', 'x509', 
                  '-in', '{tdir}/bin/certificate.crt'.format(tdir=get_keycloak_dir(ctx,config)), 
                  '--fingerprint', '--noout', '-sha1'
                 ])

        pre_ans= '{}'.format(out5[17:76])
        ans5=""

        for character in pre_ans:
            if(character!=':'):
                ans5+=character

        str1 = 'curl'
        str2 = '-k'
        str3 = '-v'
        str4 = '-X'
        str5 = 'POST'
        str6 = '-u'
        str7 = '-d'
        str8 = 'http://localhost:8080/auth/realms/'+realm_name+'/protocol/openid-connect/token/introspect'

        out6= toxvenv_sh(ctx, remote,
                 [
                  str1, str2, str3, str4, str5, str6, ans0, str7, acc_token, str8, run.Raw('|'), 'jq', '-r', '.aud'
                 ])

        out7= toxvenv_sh(ctx, remote,
                 [ 
                  str1, str2, str3, str4, str5, str6, ans0, str7, acc_token, str8, run.Raw('|'), 'jq', '-r', '.sub'
                 ])

        out8= toxvenv_sh(ctx, remote,
                 [ 
                  str1, str2, str3, str4, str5, str6, ans0, str7, acc_token, str8, run.Raw('|'), 'jq', '-r', '.azp'
                 ])

        ans6=out6.rstrip()
        ans7=out7.rstrip()
        ans8=out8.rstrip()

        os.environ['TOKEN']=ans4
        os.environ['THUMBPRINT']=ans5
        os.environ['AUD']=ans6
        os.environ['SUB']=ans7
        os.environ['AZP']=ans8
        os.environ['USER_TOKEN']=user_token
        os.environ['KC_REALM']=realm_name

    try:
        yield
    finally:
        log.info('Removing certificate.crt file...')
        for (client,_) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                 args=['rm', '-f',
                       '{tdir}/bin/certificate.crt'.format(tdir=get_keycloak_dir(ctx,config)),
                 ],
                 )

            remote.run(
                 args=['rm', '-f',
                       '{tdir}/confi.py'.format(tdir=teuthology.get_testdir(ctx)),
                 ],
                 )

@contextlib.contextmanager
def task(ctx,config):
    """
    To run keycloak the prerequisite is to run the tox task. Following is the way how to run
    tox and then keycloak::

    tasks:
    - tox: [ client.0 ]
    - keycloak:
        client.0:
            keycloak_version: 11.0.0
   
    To pass extra arguments to nose (e.g. to run a certain test)::

    tasks:
    - tox: [ client.0 ]
    - keycloak:
        client.0:
            keycloak_version: 11.0.0
    - s3tests:
        client.0:
          extra_attrs: ['webidentity_test']
 
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task keycloak only supports a list or dictionary for configuration"

    if not hasattr(ctx, 'tox'):
        raise ConfigError('keycloak must run after the tox task')

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    log.debug('Keycloak config is %s', config)
    
    with contextutil.nested(
        lambda: install_packages(ctx=ctx, config=config),
        lambda: build(ctx=ctx, config=config),
        lambda: run_keycloak(ctx=ctx, config=config),
        lambda: download_conf(ctx=ctx, config=config),
        lambda: run_admin_cmds(ctx=ctx, config=config),
        ):
        yield

