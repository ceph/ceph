from teuthology import misc
from teuthology.exceptions import ConfigError


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


def introspect_roles(ctx, logger, quiet=True):
    """
    Creates the following keys in ctx:

        nodes,
        nodes_client_only,
        nodes_cluster,
        nodes_gateway,
        nodes_storage, and
        nodes_storage_only.

    These are all simple lists of hostnames.

    Also creates

        ctx['remotes'],

    which is a dict of teuthology "remote" objects, which look like this:

        { remote1_name: remote1_obj, ..., remoten_name: remoten_obj }

    Also creates

        ctx['role_types']

    which is just like the "roles" list, except it contains only unique
    role types per node.

    Finally, creates:

        ctx['role_lookup_table']

    which will look something like this:

        {
            "osd": { "osd.0": osd0remname, ..., "osd.n": osdnremname },
            "mon": { "mon.a": monaremname, ..., "mon.n": monnremname },
            ...
        }

    and

        ctx['remote_lookup_table']

    which looks like this:

        {
            remote0name: [ "osd.0", "client.0" ],
            ...
            remotenname: [ remotenrole0, ..., remotenrole99 ],
        }

    (In other words, remote_lookup_table is just like the roles
    stanza, except the role lists are keyed by remote name.)
    """
    # initialization phase
    cluster_roles = ['mon', 'mgr', 'osd', 'mds']
    non_storage_cluster_roles = ['mon', 'mgr', 'mds']
    gateway_roles = ['rgw', 'igw', 'ganesha']
    roles = ctx.config['roles']
    nodes = []
    nodes_client_only = []
    nodes_cluster = []
    non_storage_cluster_nodes = []
    nodes_gateway = []
    nodes_storage = []
    nodes_storage_only = []
    remotes = {}
    role_types = []
    role_lookup_table = {}
    remote_lookup_table = {}
    # introspection phase
    idx = 0
    for node_roles_list in roles:
        assert isinstance(node_roles_list, list), \
            "node_roles_list is a list"
        assert node_roles_list, "node_roles_list is not empty"
        remote = get_remote_for_role(ctx, node_roles_list[0])
        role_types.append([])
        if not quiet:
            logger.debug("Considering remote name {}, hostname {}"
                         .format(remote.name, remote.hostname))
        nodes += [remote.hostname]
        remotes[remote.hostname] = remote
        remote_lookup_table[remote.hostname] = node_roles_list
        # inner loop: roles (something like "osd.1" or "c2.mon.a")
        for role in node_roles_list:
            # FIXME: support multiple clusters as used in, e.g.,
            # rgw/multisite suite
            role_arr = role.split('.')
            if len(role_arr) != 2:
                raise ConfigError("Unsupported role ->{}<-"
                                  .format(role))
            (role_type, _) = role_arr
            if role_type not in role_lookup_table:
                role_lookup_table[role_type] = {}
            role_lookup_table[role_type][role] = remote.hostname
            if role_type in cluster_roles:
                nodes_cluster += [remote.hostname]
            if role_type in gateway_roles:
                nodes_gateway += [remote.hostname]
            if role_type in non_storage_cluster_roles:
                non_storage_cluster_nodes += [remote.hostname]
            if role_type == 'osd':
                nodes_storage += [remote.hostname]
            if role_type not in role_types[idx]:
                role_types[idx] += [role_type]
        idx += 1
    nodes_cluster = list(set(nodes_cluster))
    nodes_gateway = list(set(nodes_gateway))
    nodes_storage = list(set(nodes_storage))
    nodes_storage_only = []
    for node in nodes_storage:
        if node not in non_storage_cluster_nodes:
            if node not in nodes_gateway:
                nodes_storage_only += [node]
    nodes_client_only = list(
        set(nodes).difference(set(nodes_cluster).union(set(nodes_gateway)))
        )
    if not quiet:
        logger.debug("nodes_client_only is ->{}<-".format(nodes_client_only))
    assign_vars = [
        'nodes',
        'nodes_client_only',
        'nodes_cluster',
        'nodes_gateway',
        'nodes_storage',
        'nodes_storage_only',
        'remote_lookup_table',
        'remotes',
        'role_lookup_table',
        'role_types',
        ]
    for var in assign_vars:
        exec("ctx['{var}'] = {var}".format(var=var))
    ctx['dev_env'] = True if len(nodes_cluster) < 4 else False
    if not quiet:
        # report phase
        logger.info("ROLE INTROSPECTION REPORT")
        report_vars = assign_vars + ['dev_env']
        for var in report_vars:
            logger.info("{} == {}".format(var, ctx[var]))


def remote_run_script_as_root(remote, path, data, args=None):
    """
    Wrapper around misc.write_file to simplify the design pattern:
    1. use misc.write_file to create bash script on the remote
    2. use Remote.run to run that bash script via "sudo bash $SCRIPT"
    """
    misc.write_file(remote, path, data)
    cmd = 'sudo bash {}'.format(path)
    if args:
        cmd += ' ' + ' '.join(args)
    remote.run(label=path, args=cmd)


def sudo_append_to_file(remote, path, data):
    """
    Append data to a remote file. Standard 'cat >>' - creates file
    if it doesn't exist, but all directory components in the file
    path must exist.

    :param remote: Remote site.
    :param path: Path on the remote being written to.
    :param data: Python string containing data to be written.
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
