#!/usr/bin/python
import contextlib
import errno
import logging
import os
import paramiko
import re

from cStringIO import StringIO
from teuthology import contextutil
from ..orchestra import run

log = logging.getLogger(__name__)

# generatees a public and private key
def generate_keys():
    key = paramiko.RSAKey.generate(2048)
    privateString = StringIO()
    key.write_private_key(privateString)
    return key.get_base64(), privateString.getvalue()

# deletes the keys and removes ~/.ssh/authorized_keys entries we added
def cleanup_keys(ctx, public_key):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()

    for host in ctx.cluster.remotes.iterkeys():
        username, hostname = str(host).split('@')
        log.info('cleaning up keys on {host}'.format(host=hostname, user=username))

        client.connect(hostname, username=username)
        client.exec_command('rm ~/.ssh/id_rsa')
        client.exec_command('rm ~/.ssh/id_rsa.pub')

        # get the absolute path for authorized_keys
        stdin, stdout, stderr = client.exec_command('ls ~/.ssh/authorized_keys')
        auth_keys_file = stdout.readlines()[0].rstrip()

        mySftp = client.open_sftp()

        # write to a different authorized_keys file in case something
        # fails 1/2 way through (don't want to break ssh on the vm)
        old_auth_keys_file = mySftp.open(auth_keys_file)
        new_auth_keys_file = mySftp.open(auth_keys_file + '.new', 'w')
        out_keys = []

        for line in old_auth_keys_file.readlines():
            match = re.search(re.escape(public_key), line)

            if match:
                pass
            else:
                new_auth_keys_file.write(line)

        # close the files
        old_auth_keys_file.close()
        new_auth_keys_file.close()

        # now try to do an atomic-ish rename. If we botch this, it's bad news
        stdin, stdout, stderr = client.exec_command('mv ~/.ssh/authorized_keys.new ~/.ssh/authorized_keys')

        mySftp.close()
        client.close()

@contextlib.contextmanager
def tweak_ssh_config(ctx, config):   
    run.wait(
        ctx.cluster.run(
            args=[
                'echo', 
                'StrictHostKeyChecking no\n', 
                run.Raw('>'), 
                run.Raw('/home/ubuntu/.ssh/config'),
            ],
            wait=False,
        )
    )

    try: 
        yield

    finally:
        run.wait(
            ctx.cluster.run(
                args=['rm',run.Raw('/home/ubuntu/.ssh/config')],
            wait=False
            ),
        )

@contextlib.contextmanager
def push_keys_to_host(ctx, config, public_key, private_key):   

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()

    for host in ctx.cluster.remotes.iterkeys():
        log.info('host: {host}'.format(host=host))
        username, hostname = str(host).split('@')
        log.info('pushing keys to {host} for {user}'.format(host=hostname, user=username))

        client.connect(hostname, username=username)
        client.exec_command('echo "{priv_key}" > ~/.ssh/id_rsa'.format(priv_key=private_key))
        # the default file permissions cause ssh to balk
        client.exec_command('chmod 500 ~/.ssh/id_rsa')
        client.exec_command('echo "ssh-rsa {pub_key} {user_host}" > ~/.ssh/id_rsa.pub'.format(pub_key=public_key,user_host=host))
       
        # for this host, add all hosts to the ~/.ssh/authorized_keys file
        for inner_host in ctx.cluster.remotes.iterkeys():
            client.exec_command('echo "ssh-rsa {pub_key} {user_host}" >> ~/.ssh/authorized_keys'.format(pub_key=public_key,user_host=str(inner_host)))


        client.close()

    try: 
        yield

    finally:
        # cleanup the keys
        log.info("Cleaning up SSH keys")
        cleanup_keys(ctx, public_key)


@contextlib.contextmanager
def task(ctx, config):
    """
    Creates a set of RSA keys, distributes the same key pair
    to all hosts listed in ctx.cluster, and adds all hosts
    to all others authorized_keys list. 

    During cleanup it will delete .ssh/id_rsa, .ssh/id_rsa.pub 
    and remove the entries in .ssh/authorized_keys while leaving
    pre-existing entries in place. 
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task hadoop only supports a dictionary for configuration"

    # this does not need to do cleanup and does not depend on 
    # ctx, so I'm keeping it outside of the nested calls
    public_key_string, private_key_string = generate_keys()

    with contextutil.nested(
        lambda: push_keys_to_host(ctx, config, public_key_string, private_key_string),
        lambda: tweak_ssh_config(ctx, config),   

        ):
        yield

