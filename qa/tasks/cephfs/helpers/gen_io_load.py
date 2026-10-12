from logging import getLogger
from time import sleep
import threading


log = getLogger(__name__)


class GenIoLoad:
    '''
    Generate IO load by running a writer thread in background through a given
    mount, on a given path by writing zeros in 1 KB files until caller signals
    to stop.
    '''

    def __init__(self, path, mount, client_id=None, data_pool_name=None,
                 initial_wait=0, writer_sleep=0, writer_timeout=60*60*15,
                 raise_on_writer_crash=True):
        '''
        :param path: path to dir where IO load would be generated.
        :param mount: mount object to be used for generating IO load.
        :param client_id: client_id to be used for generating IO load.
        :param data_pool_name: CephFS data pool name where IO load will be
                               generated; used for creating caps
        :param initial_wait: period to wait for right after IO has begun,
                             useful for running IO without interruptions.
        :param writer_sleep: sleep duration for writer_thread between writing
                               files.
        :param writer_timeout: duration after which writer thread should be
                                 killed.
        '''
        self.path = path
        self.mount = mount
        self.client_id = client_id
        self.data_pool_name = data_pool_name

        self.initial_wait = initial_wait
        self.writer_sleep = writer_sleep
        self.writer_timeout = writer_timeout

        self.raise_on_writer_crash = raise_on_writer_crash

        self.stop_writer = threading.Event()
        self.should_stop = lambda: self.stop_writer.is_set()

        self.file_count = 0
        self.writer = None
        self.writer_crashed = False

    def _handle_thread_crash(self, args):
        self.writer_crashed = True
        if self.raise_on_writer_crash:
           msg = (f'writer thread running crashed. mount = {self.mount} '
                  f'path = {self.path} args.exc_type = {args.exc_type} '
                  f'args.exc_value = {args.exc_value} '
                  f'args.thread = {args.thread}')
           log.info(msg)
           raise RuntimeError(msg)

    def create_client_and_remount(self):
        if self.client_id and self.client_id == self.mount.client_id:
            return
        assert 'client.' not in self.client_id

        if self.path[0] != '/':
            self.path = '/' + self.path

        client_name = f'client.{self.client_id}'
        self.mount.run_ceph_cmd(f'auth add {client_name} '
                                'mon "allow r" '
                                f'osd "allow rw pool={self.data_pool_name}" '
                                f'mds "allow rw path={self.path}"')
        keyring = self.mount.get_ceph_cmd_stdout(f'auth get {client_name}')

        self.mount.remount(client_id=self.client_id, client_keyring=keyring,
                           cephfs_mntpt=self.path)

    # TODO implement self.writer_timeout
    def write_files(self):
        self.file_count = 1

        while True:
            if self.should_stop():
                break

            file_path = f'./file-{self.file_count}'
            self.mount.run_shell(f'dd if=/dev/zero of={file_path} bs=1K count=1')
            sleep(self.writer_sleep)
            self.file_count += 1

    def start(self):
        self.create_client_and_remount()

        threading.excepthook = self._handle_thread_crash
        self.writer = threading.Thread(target=self.write_files)
        self.writer.start()

        sleep(2)
        msg = 'writer thread died as soon as they had begun'
        log.info(msg)
        assert self.writer.is_alive(), msg

        log.info(f'waiting for {self.initial_wait} seconds for the writer '
                 'thread to run in background without interruptions')
        sleep(self.initial_wait)
        assert self.writer.is_alive(), \
                'writer thread crashed unexpectedly'

    def stop(self):
        if self.stop_writer.is_set():
            raise RuntimeError('stop_writer flag was already set')
        if not self.writer.is_alive():
            raise RuntimeError('writer thread is dead')

        self.stop_writer.set()
        log.info('giving 5 seconds of background threads to stop...')
        sleep(5)
        assert not self.is_alive()
        self.file_count -= 1

    def is_alive(self):
        return self.writer.is_alive()

    def verify_num_of_files_written(self):
        file_count = self.mount.get_shell_stdout('find ./ -type f | wc -l')
        file_count = int(file_count.strip())

        # diff of zero or one is okay
        diff = file_count - self.file_count
        if diff not in (0, 1):
            raise AssertionError(f'file_count = {file_count} self.file_count = '
                                 f'{self.file_count}')
