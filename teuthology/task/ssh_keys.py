#!/usr/bin/python
import contextlib
import logging
import paramiko
import re

from cStringIO import StringIO
from teuthology import contextutil
import teuthology.misc as misc
from ..orchestra import run

log = logging.getLogger(__name__)
ssh_keys_user = 'ssh-keys-user'

# generatees a public and private key
def generate_keys():
    key = paramiko.RSAKey.generate(2048)
    privateString = StringIO()
    key.write_private_key(privateString)
    return key.get_base64(), privateString.getvalue()

def particular_ssh_key_test(line_to_test, ssh_key):
    match = re.match('[\w-]+ {key} \S+@\S+'.format(key=re.escape(ssh_key)), line_to_test)

    if match:
        return False
    else:
        return True

def ssh_keys_user_line_test(line_to_test, username ):
    match = re.match('[\w-]+ \S+ {username}@\S+'.format(username=username), line_to_test)

    if match:
        return False
    else:
        return True

# deletes keys that were previously generated
def pre_run_cleanup_keys(ctx):
    log.info('cleaning up any left-over keys from previous tests')

    for remote in ctx.cluster.remotes:
        username, hostname = str(remote).split('@')
        if "" == username or "" == hostname:
            continue
        else:
          path = '/home/{user}/.ssh/authorized_keys'.format(user=username)
        
          misc.remove_lines_from_file(remote, path, ssh_keys_user_line_test, ssh_keys_user)

# deletes the keys and removes ~/.ssh/authorized_keys entries we added
def cleanup_added_key(ctx, public_key):
    log.info('cleaning up keys added for testing')

    for remote in ctx.cluster.remotes:
        username, hostname = str(remote).split('@')
        if "" == username or "" == hostname:
            continue
        else:
          log.info('  cleaning up keys for user {user} on {host}'.format(host=hostname, user=username))

          misc.delete_file(remote, '/home/{user}/.ssh/id_rsa'.format(user=username))
          misc.delete_file(remote, '/home/{user}/.ssh/id_rsa.pub'.format(user=username))
          path = '/home/{user}/.ssh/authorized_keys'.format(user=username)

          misc.remove_lines_from_file(remote, path, particular_ssh_key_test, public_key)

@contextlib.contextmanager
def tweak_ssh_config(ctx, config):   
    run.wait(
        ctx.cluster.run(
            args=[
                'echo',
                'StrictHostKeyChecking no\n',
                run.Raw('>'),
                run.Raw('/home/ubuntu/.ssh/config'),
                run.Raw('&&'),
                'echo',
                'UserKnownHostsFile ',
                run.Raw('/dev/null'),
                run.Raw('>>'),
                run.Raw('/home/ubuntu/.ssh/config'),
                run.Raw('&&'),
                run.Raw('chmod 600 /home/ubuntu/.ssh/config'),
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

    log.info('generated public key {pub_key}'.format(pub_key=public_key))

    # add an entry for all hosts in ctx to auth_keys_data
    auth_keys_data = ''

    for inner_host in ctx.cluster.remotes.iterkeys():
        inner_username, inner_hostname = str(inner_host).split('@')
        # create a 'user@hostname' string using our fake hostname
        fake_hostname = '{user}@{host}'.format(user=ssh_keys_user,host=str(inner_hostname))
        auth_keys_data += '\nssh-rsa {pub_key} {user_host}'.format(pub_key=public_key,user_host=fake_hostname)

    # for each host in ctx, add keys for all other hosts
    for remote in ctx.cluster.remotes:
        username, hostname = str(remote).split('@')
        if "" == username or "" == hostname:
            continue
        else:
            log.info('pushing keys to {host} for {user}'.format(host=hostname, user=username))

            # adding a private key
            priv_key_file = '/home/{user}/.ssh/id_rsa'.format(user=username)
            priv_key_data = '{priv_key}'.format(priv_key=private_key)
            # Hadoop requires that .ssh/id_rsa have permissions of '500'
            misc.create_file(remote, priv_key_file, priv_key_data, str(500))

            # then a private key
            pub_key_file = '/home/{user}/.ssh/id_rsa.pub'.format(user=username)
            pub_key_data = 'ssh-rsa {pub_key} {user_host}'.format(pub_key=public_key,user_host=str(remote))
            misc.create_file(remote, pub_key_file, pub_key_data)

            # adding appropriate entries to the authorized_keys file for this host
            auth_keys_file = '/home/{user}/.ssh/authorized_keys'.format(user=username)

            # now add the list of keys for hosts in ctx to ~/.ssh/authorized_keys 
            misc.append_lines_to_file(remote, auth_keys_file, auth_keys_data)

    try: 
        yield

    finally:
        # cleanup the keys
        log.info("Cleaning up SSH keys")
        cleanup_added_key(ctx, public_key)


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

    # cleanup old keys if they were somehow left behind
    pre_run_cleanup_keys(ctx)

    with contextutil.nested(
        lambda: tweak_ssh_config(ctx, config),
        lambda: push_keys_to_host(ctx, config, public_key_string, private_key_string),
        #lambda: tweak_ssh_config(ctx, config),
        ):
        yield

