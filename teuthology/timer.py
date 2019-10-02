import logging
import time
import yaml

from datetime import datetime

log = logging.getLogger(__name__)


class Timer(object):
    """
    A class that records timing data.

    It was created in order to record time intervals between the execution of
    different tasks' enter and exit methods.
    """
    # How many decimal places to use for time intervals
    precision = 3
    # The format to use for date-time strings
    datetime_format = '%Y-%m-%d_%H:%M:%S'

    def __init__(self, path=None, sync=False):
        """
        :param path:       A path to a file to be written when self.write() is
                           called. The file will contain self.data in yaml
                           format.
        :param sync:       Whether or not to call self.write() from within
                           self.mark()
        """
        if sync and not path:
            raise ValueError(
                "When providing sync=True, a path must be specified!")
        self.path = path
        self.sync = sync
        self.marks = list()
        self.start_time = None
        self.start_string = None

    def mark(self, message=''):
        """
        Create a time mark

        If necessary, call self._mark_start() to begin time-keeping. Then,
        create a new entry in self.marks with the message provided, along with
        the time elapsed in seconds since time-keeping began.
        """
        if self.start_time is None:
            self._mark_start(message)
        interval = round(time.time() - self.start_time, self.precision)
        mark = dict(
            interval=interval,
            message=message,
        )
        self.marks.append(mark)
        if self.sync:
            self.write()

    def _mark_start(self, message):
        """
        Create the initial time mark
        """
        self.start_time = time.time()
        self.start_string = self.get_datetime_string(self.start_time)

    def get_datetime_string(self, time):
        """
        Return a human-readable timestamp in UTC

        :param time: Time in seconds; like from time.time()
        """
        _datetime = datetime.utcfromtimestamp(time)
        return datetime.strftime(
            _datetime,
            self.datetime_format,
        )

    @property
    def data(self):
        """
        Return an object similar to::

            {'start': '2016-02-02_23:19:51',
             'elapsed': 10.65,
             'end': '2016-02-02_23:20:01',
             'marks': [
                 {'message': 'event 1', 'interval': 0.0},
                 {'message': 'event 2', 'interval': 8.58},
                 {'message': 'event 3', 'interval': 10.65}
             ],
             }

        'start' and 'end' times are in UTC.
        """
        if not self.start_string:
            return dict()
        if len(self.marks) <= 1:
            end_interval = 0
        else:
            end_interval = self.marks[-1]['interval']
        end_time = self.start_time + end_interval
        result = dict(
            start=self.start_string,
            marks=self.marks,
            end=self.get_datetime_string(end_time),
            elapsed=end_interval,
        )
        return result

    def write(self):
        try:
            with open(self.path, 'w') as f:
                yaml.safe_dump(self.data, f, default_flow_style=False)
        except Exception:
            log.exception("Failed to write timing.yaml !")
