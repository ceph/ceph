"""
Perform rados IO along with checksum
this task creates objects and maintains checksum.
This task also performs read, write, deletes and
checks checksum ar random intervals.
"""
import logging
import hashlib
import random
from StringIO import StringIO
import json

from teuthology import misc as teuthology
from util.rados import rados

log = logging.getLogger(__name__)


class IO():
    '''
    This class implements a base class for different type of IOs
    Object IO: using rados put
    FS IO: Using cephFS
    RGW IO
    '''
    def __init__(self, io_type='object', pool='default', until='migrate'):
        self.csum = 0
        self.pool = pool
        # sepcial variable to differentiate
        # when to stop IO, becasuse if we are using this task
        # in case of fs to bs migration we may want to stop
        # after all the osds are converted to bs
        # if until="migrate" then till migration
        # if until="50" it will be time
        self.until = until
        self.timeout = 0
        try:
            if type(int(until)) == int:
                self.timeout = int(until)
        except ValueError:
            pass
        self.io_type = io_type

    def read(self):
        ''' implement in child class'''
        pass

    def write(self):
        '''implement in child class '''
        pass

    def csum_calc(self):
        '''implement in child class'''
        pass

    def rm_obj(self):
        ''' implement in child class'''
        pass


class RadosObj(IO):
    '''
    This class implements IO specific to rados objects
    we use rados put, get and rm operations
    '''
    def __init__(self, ctx, mon, manager=None, pool='default12', maxobj=1000):
        self.nobj = 0
        self.maxobj = maxobj
        IO.__init__(self, pool=pool)
        self.total = 0  # Total objects of this type in the cluster
        self.obj_prefix = "robj_"
        self.ctx = ctx
        self.mon = mon
        self.manager = manager
        self.ref_obj = "/tmp/reffile"
        self.prev_max = 1  # Used to randomly select the different object range
        self.size = 1024 * 1024
        self.poolcreate()

    def poolcreate(self):
        cmd = [
            'sudo',
            'ceph',
            'osd',
            'pool',
            'create',
            self.pool,
            '1',
            '1',
        ]
        proc = self.mon.run(
            args=cmd,
            stdout=StringIO(),
        )
        out = proc.stdout.getvalue()
        log.info(out)


    def find_range(self):
        '''
        returns (start, stop) range for ops like read, calc_csum
        '''
        nobj = random.randint(1, self.total)
        if (self.prev_max + self.nobj) < self.total:
            start = self.prev_max
            self.prev_max = self.prev_max + nobj
        else:
            self.prev_max = nobj
            start = 1

        if (start + nobj) > self.total:
            stop = self.total
        else:
            stop = start + nobj
        return (start, stop)

    def read(self):
        '''
        This function reads random objects
        Number of objects to read will be randomly chosen per iteration
        '''
        if (self.total) == 0:
            log.info("Nothing to read as no objects in the cluster")
            return
        (start, stop) = self.find_range()

        for i in range(start, stop):
            cmd = [
                'sudo',
                'rados',
                '-p',
                self.pool,
                'get',
                self.obj_prefix + str(i),
                '/tmp/null',
            ]
            proc = self.mon.run(
                args=cmd,
                stdout=StringIO(),
            )
            out = proc.stdout.getvalue()
            log.info(out)

    def calc_csum(self):
        '''
        This function calculates checksum of one object
        and copy of same objects will be place all over the cluster
        so that it will be easy to perform expected csum
        validation
        '''
        if (self.total) == 0:
            log.info("Nothing to read as no objects in the cluster")
            return

        rfile = "/tmp/tmpobj"

        (start, stop) = self.find_range()
        for i in range(start, stop):
            cmd = [
                'sudo',
                'rados',
                '-p',
                self.pool,
                'get',
                self.obj_prefix + str(i),
                rfile,
            ]
            proc = self.mon.run(
                args=cmd,
                stdout=StringIO(),
            )
            out = proc.stdout.getvalue()
            log.info(out)

            data = teuthology.get_file(self.mon, rfile)
            t_csum = hashlib.sha1(data).hexdigest()
            if self.csum != t_csum:
                log.error("csum mismatch for {}".format(
                    self.obj_prefix + str(i)))
                assert False
            else:
                log.info("csum matched !!!")

    def write(self):
        '''
        create random number of objects with given name,content
        of the object will be as same as reference object
        '''
        nobj = random.randint(1, self.total+10)
        start = self.total + 1
        stop = start + nobj

        for i in range(start, stop+1):
            cmd = [
                'sudo',
                'rados',
                '-p',
                self.pool,
                'put',
                self.obj_prefix + str(i),
                self.ref_obj,
            ]

            try:
                proc = self.mon.run(
                    args=cmd,
                    stdout=StringIO(),
                )
                out = proc.stdout.getvalue()
                log.info(out)
                self.total = self.total + 1
            except:
                log.error("Failed to put {}".format(self.obj_prefix + str(i)))
                assert False

    def rm_obj(self):
        '''
        Remove random number of objects from the pool
        '''
        if (self.total) == 0:
            log.info("Nothing to read as no objects in the cluster")
            return
        (start, stop) = self.find_range()
        for i in range(start, stop):
            try:
                rados(self.ctx, self.mon, ['-p', self.pool, 'rm',
                                           self.obj_prefix + str(i)])
                self.total = self.total - 1
            except:
                log.error("Unable to  rm {}".format(self.obj_prefix + str(i)))
                assert False

    def check_all_bluestore(self):
        '''
        return True  if all osds are on bluestore
        else False
        '''
        osd_cnt = teuthology.num_instances_of_type(self.ctx.cluster, 'osd')
        log.info("Total osds = {}".format(osd_cnt))
        fcount = 0

        store_cnt = [
            'sudo',
            'ceph',
            'osd',
            'count-metadata',
            'osd_objectstore',
        ]

        proc = self.mon.run(
            args=store_cnt,
            stdout=StringIO(),
        )
        out = proc.stdout.getvalue()
        log.info(out)

        dout = json.loads(out)
        log.info(dout)
        if 'bluestore' in dout:
            fcount = dout['bluestore']

        if fcount != osd_cnt:
            log.error("Not all osds are on bluestore")
            return False

        return True

    def check_timeout(self):
        if self.timeout == 0:
            return True
        else:
            self.timeout = self.timeout - 1
            return False

    def time_to_stop(self):
        '''
        This function decides whether stop IOs or not
        '''
        if self.until == 'migrate':
            return self.check_all_bluestore()
        else:
            try:
                if type(int(self.until)) == int:
                    return self.check_timeout()
            except ValueError:
                pass


    def create_ref_data(self):
        refpath = "/tmp/reffile"
        refbuf = "HELLOCEPH" * (self.size / len("HELLOCEPH"))
        self.csum = hashlib.sha1(refbuf).hexdigest()
        with open(refpath, "w") as ref:
            ref.write(refbuf)
        self.mon.put_file(refpath, self.ref_obj)

    def runner(self):
        '''
        This function randomly call read, write and calc_sum function
        This function also check the condition 'until' to determine
        when to terminate the IOs.
        '''
        log.info("Inside runner")
        self.create_ref_data()
        while not self.time_to_stop():
            op = {1: self.write, 2: self.read, 3: self.calc_csum, 4: self.rm_obj}
            rand_op = op[random.randint(1, 3)]
            rand_op()


def task(ctx, config):
    """
    Task for handling IO with checksum
    """
    log.info("Inside io task")
    if config is None:
        config = {}
        assert isinstance(config, dict), \
            'IO with csum task accepts a dict for config'

    # manager = ctx.managers['ceph']
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    # TODO instantiate IO class and call runner
    # as of now hard setting the 'until' and 'io_type'
    # TODO get it from config if any new cases added
    IO = RadosObj(ctx, mon)
    IO.runner()
