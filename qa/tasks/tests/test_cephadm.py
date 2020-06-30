from tasks import cephadm

v1 = """
[registries.search]
registries = ['registry.access.redhat.com', 'registry.redhat.io', 'docker.io', 'quay.io']

[registries.insecure]
registries = []
"""

v2 = """
unqualified-search-registries = ["registry.access.redhat.com", "registry.redhat.io", "docker.io", 'quay.io']

[[registry]]
prefix = "registry.access.redhat.com"
location = "registry.access.redhat.com"
insecure = false
blocked = false

[[registry]]
prefix = "registry.redhat.io"
location = "registry.redhat.io"
insecure = false
blocked = false

[[registry]]
prefix = "docker.io"
location = "docker.io"
insecure = false
blocked = false

[[registry.mirror]]
location = "vossi04.front.sepia.ceph.com:5000"
insecure = true

[[registry]]
prefix = "quay.io"
location = "quay.io"
insecure = false
blocked = false
"""

expected = {
    'unqualified-search-registries': ['registry.access.redhat.com', 'registry.redhat.io',
                                      'docker.io', 'quay.io'],
    'registry': [
        {'prefix': 'registry.access.redhat.com',
         'location': 'registry.access.redhat.com',
         'insecure': False,
         'blocked': False},
        {'prefix': 'registry.redhat.io',
         'location': 'registry.redhat.io',
         'insecure': False,
         'blocked': False},
        {'prefix': 'docker.io',
         'location': 'docker.io',
         'insecure': False,
         'blocked': False,
         'mirror': [{'location': 'vossi04.front.sepia.ceph.com:5000',
                     'insecure': True}]},
        {'prefix': 'quay.io',
         'location': 'quay.io',
         'insecure': False,
         'blocked': False}
    ]
}

def test_add_mirror():
    assert cephadm.registries_add_mirror_to_docker_io(v1, 'vossi04.front.sepia.ceph.com:5000') == expected
    assert cephadm.registries_add_mirror_to_docker_io(v2, 'vossi04.front.sepia.ceph.com:5000') == expected
