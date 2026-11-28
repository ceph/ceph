import pytest
import logging
from ceph_volume.tests.conftest import Factory
from ceph_volume import process


@pytest.fixture
def mock_call(monkeypatch):
    """
    Monkeypatches process.call, so that a caller can add behavior to the response
    """
    def apply(stdout=None, stderr=None, returncode=0):
        return_value = Factory(
            returncode=returncode,
            communicate=lambda x: (stdout, stderr),
        )

        monkeypatch.setattr(
            'ceph_volume.process.subprocess.Popen',
            lambda *a, **kw: return_value)

    return apply


class TestCall(object):

    def test_stderr_terminal_and_logfile(self, mock_call, caplog, capsys):
        caplog.set_level(logging.DEBUG)
        mock_call(stdout='stdout\n', stderr='some stderr message\n')
        process.call(['ls'], terminal_verbose=True)
        out, err = capsys.readouterr()
        log_lines = [line[-1] for line in caplog.record_tuples]
        assert 'Running command: ' in log_lines[0]
        assert 'ls' in log_lines[0]
        assert 'stderr some stderr message' in log_lines[-1]
        assert 'some stderr message' in err

    def test_stderr_terminal_and_logfile_off(self, mock_call, caplog, capsys):
        caplog.set_level(logging.DEBUG)
        mock_call(stdout='stdout\n', stderr='some stderr message\n')
        process.call(['ls'], terminal_verbose=False)
        out, err = capsys.readouterr()
        log_lines = [line[-1] for line in caplog.record_tuples]
        assert 'Running command: ' in log_lines[0]
        assert 'ls' in log_lines[0]
        assert 'stderr some stderr message' in log_lines[-1]
        assert out == ''

    def test_success_not_logged_at_info(self, mock_call, caplog):
        caplog.set_level(logging.INFO)
        mock_call(stdout='stdout\n', stderr='some stderr message\n')
        process.call(['ls'], terminal_verbose=False)
        process_records = [
            r for r in caplog.records
            if r.name == 'ceph_volume.process' and r.levelno >= logging.INFO
        ]
        assert not process_records

    def test_verbose_on_failure(self, mock_call, caplog, capsys):
        caplog.set_level(logging.INFO)
        mock_call(stdout='stdout\n', stderr='stderr\n', returncode=1)
        process.call(['ls'], terminal_verbose=False, logfile_verbose=False)
        out, err = capsys.readouterr()
        log_lines = '\n'.join([line[-1] for line in caplog.record_tuples])
        assert 'Running command: ' in log_lines
        assert 'ls' in log_lines
        assert 'stderr' in log_lines
        assert 'stdout: stdout' in err
        assert out == ''

    def test_obfuscate_redacts_logged_command_only(self, caplog, monkeypatch):
        caplog.set_level(logging.DEBUG)
        captured = {}
        secret = 'synthetic-lockbox-secret'

        def popen(cmd, **kw):
            captured['cmd'] = list(cmd)
            stdout_stream = Factory(read=lambda: b'')
            stderr_stream = Factory(read=lambda: b'')
            return Factory(
                stdout=stdout_stream,
                stderr=stderr_stream,
                wait=lambda: 0,
                communicate=lambda x: (b'', b''),
            )

        monkeypatch.setattr(process.subprocess, 'Popen', popen)
        process.call(
            ['echo', '--deltag', 'tag=%s' % secret, '--addtag', 'tag=%s' % secret],
            obfuscate=['--deltag', '--addtag'],
        )
        logged = '\n'.join(record.getMessage() for record in caplog.records)
        assert secret not in logged
        assert secret in ' '.join(captured['cmd'])

    def test_silent_verbose_on_failure(self, mock_call, caplog, capsys):
        caplog.set_level(logging.INFO)
        mock_call(stdout='stdout\n', stderr='stderr\n', returncode=1)
        process.call(['ls'], verbose_on_failure=False)
        out, err = capsys.readouterr()
        log_lines = '\n'.join([line[-1] for line in caplog.record_tuples])
        assert 'Running command: ' in log_lines
        assert 'ls' in log_lines
        assert 'stderr' in log_lines
        assert out == ''


class TestFunctionalCall(object):

    def test_stdin(self):
        process.call(['xargs', 'ls'], stdin="echo '/'")

    def test_unicode_encoding(self):
        process.call(['echo', u'\xd0'])

    def test_unicode_encoding_stdin(self):
        process.call(['echo'], stdin=u'\xd0'.encode('utf-8'))


class TestFunctionalRun(object):

    def test_log_descriptors(self):
        process.run(['ls', '-l'])
