from teuthology import misc

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
