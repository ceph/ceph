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
        stdout_stream = Factory(read=lambda: stdout)
        stderr_stream = Factory(read=lambda: stderr)
        return_value = Factory(
            stdout=stdout_stream,
            stderr=stderr_stream,
            wait=lambda: returncode,
            communicate=lambda x: (stdout, stderr, returncode)
        )

        monkeypatch.setattr(
            'ceph_volume.process.subprocess.Popen',
            lambda *a, **kw: return_value)

    return apply


class TestCall(object):

    def test_stderr_terminal_and_logfile(self, mock_call, caplog, capsys):
        caplog.set_level(logging.INFO)
        mock_call(stdout='stdout\n', stderr='some stderr message\n')
        process.call(['ls'], terminal_verbose=True)
        out, err = capsys.readouterr()
        log_lines = [line[-1] for line in caplog.record_tuples]
        assert 'Running command: ' in log_lines[0]
        assert 'ls' in log_lines[0]
        assert 'stderr some stderr message' in log_lines[-1]
        assert 'some stderr message' in err

    def test_stderr_terminal_and_logfile_off(self, mock_call, caplog, capsys):
        caplog.set_level(logging.INFO)
        mock_call(stdout='stdout\n', stderr='some stderr message\n')
        process.call(['ls'], terminal_verbose=False)
        out, err = capsys.readouterr()
        log_lines = [line[-1] for line in caplog.record_tuples]
        assert 'Running command: ' in log_lines[0]
        assert 'ls' in log_lines[0]
        assert 'stderr some stderr message' in log_lines[-1]
        assert out == ''

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
