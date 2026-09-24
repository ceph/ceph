from time import sleep as time_sleep
from logging import getLogger
from threading import Thread, Event, excepthook


log = getLogger(__name__)


class GenIoLoad:
    '''
    Generate IO load by running a writer thread in background through a given
    mount, on a given path by writing zeros in 1 KB files until caller signals
    to stop.
    '''

    def __init__(self, mount_x, path, timeout=60*60*15, sleep=0,
                 raise_on_thread_crash=True):
        self.mount_x = mount_x
        self.path = path
        self.raise_on_thread_crash = raise_on_thread_crash
        self.timeout = timeout
        self.sleep = sleep

        self.stop_w_thread = Event()
        self.should_stop = lambda: self.stop_w_thread.is_set()

        self.file_count = 0
        self.w_thread = None
        self.w_thread_crashed = False

    def _handle_thread_crash(self):
        self.w_thread_crashed = True
        if self.raise_on_thread_crash:
           msg = (f'writer thread running crashed. mount = {self.mount_x} '
                  f'path = {self.path}')
           log.info(msg)
           raise RuntimeError(msg)

    # TODO implement self.timeout
    def write_files(self):
        self.file_count = 1

        while True:
            if self.should_stop():
                break

            file_name = f'file-{self.file_count}'
            if self.path == '/':
                file_path = f'./{file_name}'
            else:
                file_path = f'{self.path}/{file_name}'

            self.mount_x.run_shell(f'dd if=/dev/zero of={file_path} bs=1K '
                                    'count=1')
            time_sleep(self.sleep)
            self.file_count += 1

    def start(self):
        excepthook = self._handle_thread_crash
        self.w_thread = Thread(target=self.write_files)
        self.w_thread.start()

    def stop(self):
        if self.stop_w_thread.is_set():
            raise RuntimeError('stop_writer flag was already set')
        if not self.w_thread.is_alive():
            raise RuntimeError('writer thread is dead')

        self.stop_w_thread.set()
        log.info('giving 5 seconds of background threads to stop...')
        time_sleep(5)
        assert not self.is_alive()
        self.file_count -= 1

    def is_alive(self):
        return self.w_thread.is_alive()

    def verify_num_of_files_written(self):
        file_count = self.mount_x.get_shell_stdout('find ./ -type f | wc -l')
        file_count = int(file_count.strip())

        # diff of zero or one is okay
        diff = file_count - self.file_count
        if diff not in (0, 1):
            raise AssertionError(f'file_count = {file_count} self.file_count = '
                                 f'{self.file_count}')
