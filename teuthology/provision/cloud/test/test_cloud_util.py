import datetime
import dateutil
import json

from mock import patch, mock_open
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
        with patch('teuthology.provision.cloud.util.open', mock_open(), create=True) as m_open:
            util.get_user_ssh_pubkey(path)
            if exists:
                m_open.assert_called_once_with(path)


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


def get_datetime(offset_hours=0):
    delta = datetime.timedelta(hours=offset_hours)
    return datetime.datetime.now(dateutil.tz.tzutc()) + delta


def get_datetime_string(offset_hours=0):
    obj = get_datetime(offset_hours)
    return obj.strftime(util.AuthToken.time_format)


class TestAuthToken(object):
    klass = util.AuthToken

    def setup(self):
        default_expires = get_datetime_string(0)
        self.test_data = dict(
            value='token_value',
            endpoint='endpoint',
            expires=default_expires,
        )
        self.patchers = dict()
        self.patchers['m_open'] = patch(
            'teuthology.provision.cloud.util.open'
        )
        self.patchers['m_exists'] = patch(
            'os.path.exists'
        )
        self.patchers['m_file_lock'] = patch(
            'teuthology.provision.cloud.util.FileLock'
        )
        self.mocks = dict()
        for name, patcher in self.patchers.items():
            self.mocks[name] = patcher.start()

    def teardown(self):
        for patcher in self.patchers.values():
            patcher.stop()

    def get_obj(self, name='name', directory='/fake/directory'):
        return self.klass(
            name=name,
            directory=directory,
        )

    def test_no_token(self):
        obj = self.get_obj()
        self.mocks['m_exists'].return_value = False
        with obj:
            assert obj.value is None
            assert obj.expired is True

    @mark.parametrize(
        'test_data, expired',
        [
            [
                dict(
                    value='token_value',
                    endpoint='endpoint',
                    expires=get_datetime_string(-1),
                ),
                True
            ],
            [
                dict(
                    value='token_value',
                    endpoint='endpoint',
                    expires=get_datetime_string(1),
                ),
                False
            ],
        ]
    )
    def test_token_read(self, test_data, expired):
        obj = self.get_obj()
        self.mocks['m_exists'].return_value = True
        self.mocks['m_open'].return_value.__enter__.return_value.read.return_value = \
            json.dumps(test_data)
        with obj:
            if expired:
                assert obj.value is None
                assert obj.expired is True
            else:
                assert obj.value == test_data['value']

    def test_token_write(self):
        obj = self.get_obj()
        datetime_obj = get_datetime(0)
        datetime_string = get_datetime_string(0)
        self.mocks['m_exists'].return_value = False
        with obj:
            obj.write('value', datetime_obj, 'endpoint')
        m_open = self.mocks['m_open']
        write_calls = m_open.return_value.__enter__.return_value.write\
            .call_args_list
        assert len(write_calls) == 1
        expected = json.dumps(dict(
            value='value',
            expires=datetime_string,
            endpoint='endpoint',
        ))
        assert write_calls[0][0][0] == expected
