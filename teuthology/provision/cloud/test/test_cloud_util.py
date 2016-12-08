from mock import patch, MagicMock
from pytest import mark

from teuthology.provision.cloud import util


@mark.parametrize(
    'path, exists',
    [
        ('/fake/path', True),
        ('/fake/path', False),
    ]
)
def test_get_user_ssh_pubkey(path, exists):
    with patch('os.path.exists') as m_exists:
        m_exists.return_value = exists
        with patch('teuthology.provision.cloud.util.file') as m_file:
            m_file.return_value = MagicMock(spec=file)
            util.get_user_ssh_pubkey(path)
            if exists:
                assert m_file.called_once_with(path, 'rb')


@mark.parametrize(
    'input_, func, expected',
    [
        [
            [
                dict(sub0=dict(key0=0, key1=0)),
                dict(sub0=dict(key1=1, key2=2)),
            ],
            lambda x, y: x > y,
            dict(sub0=dict(key0=0, key1=1, key2=2))
        ],
        [
            [
                dict(),
                dict(sub0=dict(key1=1, key2=2)),
            ],
            lambda x, y: x > y,
            dict(sub0=dict(key1=1, key2=2))
        ],
        [
            [
                dict(sub0=dict(key1=1, key2=2)),
                dict(),
            ],
            lambda x, y: x > y,
            dict(sub0=dict(key1=1, key2=2))
        ],
        [
            [
                dict(sub0=dict(key0=0, key1=0, key2=0)),
                dict(sub0=dict(key0=1, key2=3), sub1=dict(key0=0)),
                dict(sub0=dict(key0=3, key1=2, key2=1)),
                dict(sub0=dict(key1=3),
                     sub1=dict(key0=3, key1=0)),
            ],
            lambda x, y: x > y,
            dict(sub0=dict(key0=3, key1=3, key2=3),
                 sub1=dict(key0=3, key1=0))
        ],
    ]
)
def test_combine_dicts(input_, func, expected):
    assert util.combine_dicts(input_, func) == expected
