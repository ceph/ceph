from teuthology import timer

from unittest.mock import MagicMock, patch, mock_open
from time import time


class TestTimer(object):
    def test_data_empty(self):
        self.timer = timer.Timer()
        assert self.timer.data == dict()

    def test_data_one_mark(self):
        self.timer = timer.Timer()
        # Avoid failing if ~1ms elapses between these two calls
        self.timer.precision = 2
        self.timer.mark()
        assert len(self.timer.data['marks']) == 1
        assert self.timer.data['marks'][0]['interval'] == 0
        assert self.timer.data['marks'][0]['message'] == ''

    def test_data_five_marks(self):
        self.timer = timer.Timer()
        for i in range(5):
            self.timer.mark(str(i))
        assert len(self.timer.data['marks']) == 5
        assert [m['message'] for m in self.timer.data['marks']] == \
            ['0', '1', '2', '3', '4']

    def test_intervals(self):
        fake_time = MagicMock()
        with patch('teuthology.timer.time.time', fake_time):
            self.timer = timer.Timer()
            now = start_time = fake_time.return_value = time()
            intervals = [0, 1, 1, 2, 3, 5, 8]
            for i in intervals:
                now += i
                fake_time.return_value = now
                self.timer.mark(str(i))

        summed_intervals = [sum(intervals[:x + 1]) for x in range(len(intervals))]
        result_intervals = [m['interval'] for m in self.timer.data['marks']]
        assert result_intervals == summed_intervals
        assert self.timer.data['start'] == \
            self.timer.get_datetime_string(start_time)
        assert self.timer.data['end'] == \
            self.timer.get_datetime_string(start_time + summed_intervals[-1])
        assert [m['message'] for m in self.timer.data['marks']] == \
            [str(i) for i in intervals]
        assert self.timer.data['elapsed'] == summed_intervals[-1]

    def test_write(self):
        _path = '/path'
        _safe_dump = MagicMock(name='safe_dump')
        with patch('teuthology.timer.yaml.safe_dump', _safe_dump):
            with patch('teuthology.timer.open', mock_open(), create=True) as _open:
                self.timer = timer.Timer(path=_path)
                assert self.timer.path == _path
                self.timer.write()
                _open.assert_called_once_with(_path, 'w')
                _safe_dump.assert_called_once_with(
                    dict(),
                    _open.return_value.__enter__.return_value,
                    default_flow_style=False,
                )

    def test_sync(self):
        _path = '/path'
        _safe_dump = MagicMock(name='safe_dump')
        with patch('teuthology.timer.yaml.safe_dump', _safe_dump):
            with patch('teuthology.timer.open', mock_open(), create=True) as _open:
                self.timer = timer.Timer(path=_path, sync=True)
                assert self.timer.path == _path
                assert self.timer.sync is True
                self.timer.mark()
                _open.assert_called_once_with(_path, 'w')
                _safe_dump.assert_called_once_with(
                    self.timer.data,
                    _open.return_value.__enter__.return_value,
                    default_flow_style=False,
                )
