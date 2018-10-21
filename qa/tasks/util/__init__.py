from teuthology import misc
from teuthology.orchestra import run

def check_config_key(conf_dict, keyname, default_value):
    """
    Ensure config dict (can be any dictionary) has a value for a given key.
    """
    if not isinstance(conf_dict, dict):
        raise ConfigError("Non-dict value provided for configuration dictionary")
    if keyname not in conf_dict:
        conf_dict[keyname] = default_value
    return conf_dict[keyname]

def get_remote(ctx, cluster, service_type, service_id):
    """
    Get the Remote for the host where a particular role runs.

    :param cluster: name of the cluster the service is part of
    :param service_type: e.g. 'mds', 'osd', 'client'
    :param service_id: The third part of a role, e.g. '0' for
                       the role 'ceph.client.0'
    :return: a Remote instance for the host where the
             requested role is placed
    """
    def _is_instance(role):
        role_tuple = misc.split_role(role)
        return role_tuple == (cluster, service_type, str(service_id))
    try:
        (remote,) = ctx.cluster.only(_is_instance).remotes.keys()
    except ValueError:
        raise KeyError("Service {0}.{1}.{2} not found".format(cluster,
                                                              service_type,
                                                              service_id))
    return remote

def get_remote_for_role(ctx, role):
    return get_remote(ctx, *misc.split_role(role))

def copy_directory_recursively(from_path, to_remote, to_path=None):
    """
    Recursively copies a local directory to a remote.
    """
    if to_path is None:
        to_path = from_path
    misc.sh("scp -r -v {from_path} {host}:{to_path}".format(
            from_path=from_path, host=to_remote.name, to_path=to_path))

def sudo_append_to_file(remote, path, data):
    """
    Append data to a remote file

    :param remote: Remote site.
    :param path: Path on the remote being written to.
    :param data: Data to be written.
    """
    remote.run(
        args=[
            'sudo',
            'sh',
            '-c',
            'cat >> ' + path,
        ],
        stdin=data,
    )
