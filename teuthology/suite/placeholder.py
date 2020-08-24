import copy


class Placeholder(object):
    """
    A placeholder for use with substitute_placeholders. Simply has a 'name'
    attribute.
    """
    def __init__(self, name):
        self.name = name


def substitute_placeholders(input_dict, values_dict):
    """
    Replace any Placeholder instances with values named in values_dict. In the
    case of None values, the key is omitted from the result.

    Searches through nested dicts.

    :param input_dict:  A dict which may contain one or more Placeholder
                        instances as values.
    :param values_dict: A dict, with keys matching the 'name' attributes of all
                        of the Placeholder instances in the input_dict, and
                        values to be substituted.
    :returns:           The modified input_dict
    """
    input_dict = copy.deepcopy(input_dict)

    def _substitute(input_dict, values_dict):
        for key, value in list(input_dict.items()):
            if isinstance(value, dict):
                _substitute(value, values_dict)
            elif isinstance(value, Placeholder):
                if values_dict[value.name] is None:
                    del input_dict[key]
                    continue
                # If there is a Placeholder without a corresponding entry in
                # values_dict, we will hit a KeyError - we want this.
                input_dict[key] = values_dict[value.name]
        return input_dict

    return _substitute(input_dict, values_dict)


# Template for the config that becomes the base for each generated job config
dict_templ = {
    'branch': Placeholder('ceph_branch'),
    'sha1': Placeholder('ceph_hash'),
    'teuthology_branch': Placeholder('teuthology_branch'),
    'archive_upload': Placeholder('archive_upload'),
    'archive_upload_key': Placeholder('archive_upload_key'),
    'machine_type': Placeholder('machine_type'),
    'nuke-on-error': True,
    'os_type': Placeholder('distro'),
    'os_version': Placeholder('distro_version'),
    'overrides': {
        'admin_socket': {
            'branch': Placeholder('ceph_branch'),
        },
        'ceph': {
            'conf': {
                'mon': {
                    'debug mon': 20,
                    'debug ms': 1,
                    'debug paxos': 20},
                'mgr': {
                    'debug mgr': 20,
                    'debug ms': 1},
                'osd': {
                    'debug filestore': 20,
                    'debug journal': 20,
                    'debug ms': 20,
                    'debug osd': 25
                }
            },
            'log-whitelist': ['\(MDS_ALL_DOWN\)',
                              '\(MDS_UP_LESS_THAN_MAX\)'],
            'log-ignorelist': ['\(MDS_ALL_DOWN\)',
                              '\(MDS_UP_LESS_THAN_MAX\)'],
            'sha1': Placeholder('ceph_hash'),
        },
        'ceph-deploy': {
            'conf': {
                'client': {
                    'log file': '/var/log/ceph/ceph-$name.$pid.log'
                },
                'mon': {
                    'osd default pool size': 2
                }
            }
        },
        'install': {
            'ceph': {
                'sha1': Placeholder('ceph_hash'),
            }
        },
        'workunit': {
            'branch': Placeholder('suite_branch'),
            'sha1': Placeholder('suite_hash'),
        }
    },
    'repo': Placeholder('ceph_repo'),
    'sleep_before_teardown': 0,
    'suite': Placeholder('suite'),
    'suite_repo': Placeholder('suite_repo'),
    'suite_relpath': Placeholder('suite_relpath'),
    'suite_branch': Placeholder('suite_branch'),
    'suite_sha1': Placeholder('suite_hash'),
    'tasks': [],
}
