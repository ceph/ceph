import logging


from teuthology import misc
from teuthology.orchestra import run
from teuthology.task import Task

log = logging.getLogger(__name__)

class DENcoder(Task):
    """
    This task is used to test dencoder on the data on the given device.
    The task is expected to be run on a remote host.
    The task will run the DENcoder binary on the remote host
    """
    
    def __init__(self, ctx, config):
        super(DENcoder, self).__init__(ctx, config)
        self.ctx = ctx
        self.config = config
        self.testdir = misc.get_testdir(ctx)
        self.branch_N = config.get('branch_N', 'main')
        self.branch_N_2 = config.get('branch_N-2', 'quincy')
        self.log = log
        self.log.info('Starting DENcoder task...')

    def setup(self):
        """
        cloning the ceph repository on the remote host
        and submodules including the ceph-object-corpus
        that way we will have the readable.sh script available
        """
        super(DENcoder, self).setup()
        self.first_mon = next(iter(self.ctx.cluster.only(misc.get_first_mon(self.ctx, self.config)).remotes.keys()))
        self.first_mon.run(
                args=[
                    'git', 'clone', '-b', self.branch_N,
                    'https://github.com/ceph/ceph.git',
                    '{tdir}/ceph'.format(tdir=self.testdir)
                ]
        )
        self.ceph_dir = '{tdir}/ceph'.format(tdir=self.testdir)

        self.first_mon.run(
                args=[
                    'cd', '{tdir}/ceph'.format(tdir=self.testdir),
                    run.Raw('&&'),
                    'git', 'submodule', 'update', '--init', '--recursive'
                ]
        )
        self.corpus_dir = '{ceph_dir}/ceph-object-corpus'.format(ceph_dir=self.ceph_dir)

    def begin(self):
        """
        Run the dencoder readable.sh script on the remote host
        find any errors in the output
        """
        super(DENcoder, self).begin()
        self.log.info('Running DENcoder task...')
        self.log.info('Running DENcoder on the remote host...')
        # print ceph-dencoder version
        self.first_mon.run(
            args=[
                'cd', self.ceph_dir,
                run.Raw('&&'),
                'ceph-dencoder', 'version'
            ]
        )
        # run first check for type ceph-dencoder type MonMap
        self.first_mon.run(
            args=[
                'ceph-dencoder', 'type', 'MonMap'
            ]
        )

        # run the readable.sh script
        self.first_mon.run(
            args=[
                'CEPH_ROOT={ceph_dir}'.format(ceph_dir=self.ceph_dir),
                'CEPH_BUILD_DIR={ceph_dir}'.format(ceph_dir=self.ceph_dir),
                'CEPH_BIN=/usr/bin',
                'CEPH_LIB=/usr/lib',
                'src/test/encoding/readable.sh','ceph-dencoder'
            ]
        )
        # check for errors in the output
        
        self.log.info('DENcoder task completed...')

    def end(self):
        super(DENcoder, self).end()
        self.log.info('DENcoder task ended...')
        
task = DENcoder
