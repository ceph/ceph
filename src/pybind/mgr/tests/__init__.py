# type: ignore

import json
import logging
import os

if 'UNITTEST' in os.environ:

    # Mock ceph_module. Otherwise every module that is involved in a testcase and imports it will
    # raise an ImportError

    import sys

    try:
        from unittest import mock
    except ImportError:
        import mock

    M_classes = set()

    class M(object):
        """
        Note that:

        * self.set_store() populates self._store
        * self.set_module_option() populates self._store[module_name]
        * self.get(thing) comes from self._store['_ceph_get' + thing]

        """

        def mock_store_get(self, kind, key, default):
            if not hasattr(self, '_store'):
                self._store = {}
            return self._store.get(f'mock_store/{kind}/{key}', default)

        def mock_store_set(self, kind, key, value):
            if not hasattr(self, '_store'):
                self._store = {}
            k = f'mock_store/{kind}/{key}'
            if value is None:
                if k in self._store:
                    del self._store[k]
            else:
                self._store[k] = value

        def mock_store_prefix(self, kind, prefix):
            if not hasattr(self, '_store'):
                self._store = {}
            full_prefix = f'mock_store/{kind}/{prefix}'
            kind_len = len(f'mock_store/{kind}/')
            return {
                k[kind_len:]: v for k, v in self._store.items()
                if k.startswith(full_prefix)
            }

        def _ceph_get_store(self, k):
            return self.mock_store_get('store', k, None)

        def _ceph_set_store(self, k, v):
            self.mock_store_set('store', k, v)

        def _ceph_get_store_prefix(self, prefix):
            return self.mock_store_prefix('store', prefix)

        def _ceph_get_module_option(self, module, key, localized_prefix=None):
            try:
                _, val, _ = self.check_mon_command({
                    'prefix': 'config get',
                    'who': 'mgr',
                    'key': f'mgr/{module}/{key}'
                })
            except FileNotFoundError:
                val = None
            mo = [o for o in self.MODULE_OPTIONS if o['name'] == key]
            if len(mo) >= 1:  # >= 1, cause self.MODULE_OPTIONS. otherwise it
                #               fails when importing multiple modules.
                if 'default' in mo and val is None:
                    val = mo[0]['default']
                if val is not None:
                    cls = {
                        'str': str,
                        'secs': int,
                        'bool': lambda s: bool(s) and s != 'false' and s != 'False',
                        'int': int,
                    }[mo[0].get('type', 'str')]
                    return cls(val)
                return val
            else:
                return val if val is not None else ''

        def _ceph_set_module_option(self, module, key, val):
            _, _, _ = self.check_mon_command({
                'prefix': 'config set',
                'who': 'mgr',
                'name': f'mgr/{module}/{key}',
                'value': val
            })
            return val

        def _ceph_get(self, data_name):
            return self.mock_store_get('_ceph_get', data_name, mock.MagicMock())

        def _ceph_send_command(self, res, svc_type, svc_id, command, tag, inbuf, *, one_shot=False):

            cmd = json.loads(command)
            getattr(self, '_mon_commands_sent', []).append(cmd)

            # Mocking the config store is handy sometimes:
            def config_get():
                who = cmd['who'].split('.')
                whos = ['global'] + ['.'.join(who[:i + 1]) for i in range(len(who))]
                for attepmt in reversed(whos):
                    val = self.mock_store_get('config', f'{attepmt}/{cmd["key"]}', None)
                    if val is not None:
                        return val
                return None

            def config_set():
                self.mock_store_set('config', f'{cmd["who"]}/{cmd["name"]}', cmd['value'])
                return ''

            def config_rm():
                self.mock_store_set('config', f'{cmd["who"]}/{cmd["name"]}', None)
                return ''

            def config_dump():
                r = []
                for prefix, value in self.mock_store_prefix('config', '').items():
                    section, name = prefix.split('/', 1)
                    r.append({
                        'name': name,
                        'section': section,
                        'value': value
                    })
                return json.dumps(r)

            outb = ''
            if cmd['prefix'] == 'config get':
                outb = config_get()
            elif cmd['prefix'] == 'config set':
                outb = config_set()
            elif cmd['prefix'] == 'config dump':
                outb = config_dump()
            elif cmd['prefix'] == 'config rm':
                outb = config_rm()
            elif hasattr(self, '_mon_command_mock_' + cmd['prefix'].replace(' ', '_')):
                a = getattr(self, '_mon_command_mock_' + cmd['prefix'].replace(' ', '_'))
                outb = a(cmd)

            res.complete(0, outb, '')

        def _ceph_get_foreign_option(self, entity, name):
            who = entity.split('.')
            whos = ['global'] + ['.'.join(who[:i + 1]) for i in range(len(who))]
            for attepmt in reversed(whos):
                val = self.mock_store_get('config', f'{attepmt}/{name}', None)
                if val is not None:
                    return val
            return None

        def assert_issued_mon_command(self, command):
            assert command in self._mon_commands_sent, self._mon_commands_sent

        @property
        def _logger(self):
            return logging.getLogger(__name__)

        @_logger.setter
        def _logger(self, _):
            pass

        def __init__(self, *args):
            self._mon_commands_sent = []
            if not hasattr(self, '_store'):
                self._store = {}

            if self.__class__ not in M_classes:
                # call those only once.
                self._register_commands('')
                self._register_options('')
                M_classes.add(self.__class__)

            super(M, self).__init__()
            self._ceph_get_version = mock.Mock()
            self._ceph_get_ceph_conf_path = mock.MagicMock()
            self._ceph_get_option = mock.MagicMock()
            self._ceph_get_context = mock.MagicMock()
            self._ceph_register_client = mock.MagicMock()
            self._ceph_set_health_checks = mock.MagicMock()
            self._configure_logging = lambda *_: None
            self._unconfigure_logging = mock.MagicMock()
            self._ceph_log = mock.MagicMock()
            self._ceph_dispatch_remote = lambda *_: None
            self._ceph_get_mgr_id = mock.MagicMock()

    cm = mock.Mock()
    cm.BaseMgrModule = M
    cm.BaseMgrStandbyModule = M
    sys.modules['ceph_module'] = cm

    def mock_ceph_modules():
        class MockRadosError(Exception):
            def __init__(self, message, errno=None):
                super(MockRadosError, self).__init__(message)
                self.errno = errno

            def __str__(self):
                msg = super(MockRadosError, self).__str__()
                if self.errno is None:
                    return msg
                return '[errno {0}] {1}'.format(self.errno, msg)

        class MockObjectNotFound(Exception):
            pass

        sys.modules.update({
            'rados': mock.MagicMock(
                Error=MockRadosError,
                OSError=MockRadosError,
                ObjectNotFound=MockObjectNotFound),
            'rbd': mock.Mock(),
            'cephfs': mock.Mock(),
        })

    # Unconditionally mock the rados objects when we're imported
    mock_ceph_modules()  # type: ignore
