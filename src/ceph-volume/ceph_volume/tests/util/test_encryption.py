from ceph_volume.util import encryption


class TestStatus(object):

    def test_skips_unuseful_lines(self, stub_call):
        out = ['some line here', '  device: /dev/sdc1']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {'device': '/dev/sdc1'}

    def test_removes_extra_quotes(self, stub_call):
        out = ['some line here', '  device: "/dev/sdc1"']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {'device': '/dev/sdc1'}

    def test_ignores_bogus_lines(self, stub_call):
        out = ['some line here', '  ']
        stub_call((out, '', 0))
        assert encryption.status('/dev/sdc1') == {}


class TestDmcryptClose(object):

    def test_mapper_exists(self, fake_run, tmpfile):
        file_name = tmpfile(name='mapper-device')
        encryption.dmcrypt_close(file_name)
        arguments = fake_run.calls[0]['args'][0]
        assert arguments[0] == 'cryptsetup'
        assert arguments[1] == 'remove'
        assert arguments[2].startswith('/')

    def test_mapper_does_not_exist(self, fake_run):
        file_name = '/path/does/not/exist'
        encryption.dmcrypt_close(file_name)
        assert fake_run.calls == []


class TestDmcryptKey(object):

    def test_dmcrypt_with_default_size(self, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        result = encryption.create_dmcrypt_key()
        assert len(result) == 172

    def test_dmcrypt_with_custom_size(self, conf_ceph_stub):
        conf_ceph_stub('''
        [global]
        fsid=asdf
        [osd]
        osd_dmcrypt_size=8
        ''')
        result = encryption.create_dmcrypt_key()
        assert len(result) == 172
