import pytest

import smbutil


@pytest.mark.default
def test_arbitrary_listdir(smb_cfg, share_name):
    with smbutil.connection(smb_cfg, share_name) as sharep:
        contents = sharep.listdir()
        assert isinstance(contents, list)


@pytest.mark.default
def test_create_dir(smb_cfg, share_name):
    with smbutil.connection(smb_cfg, share_name) as sharep:
        tdir = sharep / 'test_create_dir'
        tdir.mkdir(exist_ok=True)
        try:
            contents = sharep.listdir()
            assert 'test_create_dir' in contents
        finally:
            tdir.rmdir()


@pytest.mark.default
def test_create_file(smb_cfg, share_name):
    with smbutil.connection(smb_cfg, share_name) as sharep:
        fname = sharep / 'file1.dat'
        fname.write_text('HELLO WORLD\n')
        try:
            contents = sharep.listdir()
            assert 'file1.dat' in contents

            txt = fname.read_text()
            assert txt == 'HELLO WORLD\n'
        finally:
            fname.unlink()
