"""
Locking tests
"""
import logging
import os

from ..orchestra import run
from teuthology import misc as teuthology
import time
import gevent


log = logging.getLogger(__name__)

def task(ctx, config):
    """
    This task is designed to test locking. It runs an executable
    for each lock attempt you specify, at 0.01 second intervals (to
    preserve ordering of the locks).
    You can also introduce longer intervals by setting an entry
    as a number of seconds, rather than the lock dictionary.
    The config is a list of dictionaries. For each entry in the list, you
    must name the "client" to run on, the "file" to lock, and
    the "holdtime" to hold the lock.
    Optional entries are the "offset" and "length" of the lock. You can also specify a
    "maxwait" timeout period which fails if the executable takes longer
    to complete, and an "expectfail".
    An example:
    tasks:
    - ceph:
    - ceph-fuse: [client.0, client.1]
    - lockfile:
      [{client:client.0, file:testfile, holdtime:10},
      {client:client.1, file:testfile, holdtime:0, maxwait:0, expectfail:true},
      {client:client.1, file:testfile, holdtime:0, maxwait:15, expectfail:false},
      10,
      {client: client.1, lockfile: testfile, holdtime: 5},
      {client: client.2, lockfile: testfile, holdtime: 5, maxwait: 1, expectfail: True}]

      
    In the past this test would have failed; there was a bug where waitlocks weren't
    cleaned up if the process failed. More involved scenarios are also possible.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Starting lockfile')
    try:
        assert isinstance(config, list), \
            "task lockfile got invalid config"
	
        log.info("building executable on each host")
        buildprocs = list()
        # build the locker executable on each client
        clients = list()
        files = list()
        for op in config:
            if not isinstance(op, dict):
                continue
            log.info("got an op")
            log.info("op['client'] = %s", op['client'])
            clients.append(op['client'])
            files.append(op['lockfile'])
            if not "expectfail" in op:
                op["expectfail"] = False
            badconfig = False
            if not "client" in op:
                badconfig = True
            if not "lockfile" in op:
                badconfig = True
            if not "holdtime" in op:
                badconfig = True
            if badconfig:
                raise KeyError("bad config {op_}".format(op_=op))
        
        testdir = teuthology.get_testdir(ctx)
        clients = set(clients)
        files = set(files)
        lock_procs = list()
        for client in clients:
            (client_remote,) = ctx.cluster.only(client).remotes.iterkeys()
            log.info("got a client remote")
            (_, _, client_id) = client.partition('.')
            filepath = os.path.join(testdir, 'mnt.{id}'.format(id=client_id), op["lockfile"])
            
            proc = client_remote.run(
                args=[
                    'mkdir', '-p', '{tdir}/archive/lockfile'.format(tdir=testdir),
                    run.Raw('&&'),
                    'mkdir', '-p', '{tdir}/lockfile'.format(tdir=testdir),
                    run.Raw('&&'),
                    'wget',
                    '-nv',
                    '--no-check-certificate',
                    'https://raw.github.com/gregsfortytwo/FileLocker/master/sclockandhold.cpp',
                    '-O', '{tdir}/lockfile/sclockandhold.cpp'.format(tdir=testdir),
                    run.Raw('&&'),
                    'g++', '{tdir}/lockfile/sclockandhold.cpp'.format(tdir=testdir),
                    '-o', '{tdir}/lockfile/sclockandhold'.format(tdir=testdir)
                    ],
                logger=log.getChild('lockfile_client.{id}'.format(id=client_id)),
                wait=False
                )	
            log.info('building sclockandhold on client{id}'.format(id=client_id))
            buildprocs.append(proc)
            
        # wait for builds to finish
        run.wait(buildprocs)
        log.info('finished building sclockandhold on all clients')
            
        # create the files to run these locks on
        client = clients.pop()
        clients.add(client)
        (client_remote,) = ctx.cluster.only(client).remotes.iterkeys()
        (_, _, client_id) = client.partition('.')
        file_procs = list()
        for lockfile in files:
            filepath = os.path.join(testdir, 'mnt.{id}'.format(id=client_id), lockfile)
            proc = client_remote.run(
                args=[
                    'sudo',
                    'touch',
                    filepath,
                    ],
                logger=log.getChild('lockfile_createfile'),
                wait=False
                )
            file_procs.append(proc)
        run.wait(file_procs)
        file_procs = list()
        for lockfile in files:
            filepath = os.path.join(testdir, 'mnt.{id}'.format(id=client_id), lockfile)
            proc = client_remote.run(
                args=[
                    'sudo', 'chown', 'ubuntu.ubuntu', filepath
                    ],
                logger=log.getChild('lockfile_createfile'),
                wait=False
                )
            file_procs.append(proc)
        run.wait(file_procs)
        log.debug('created files to lock')

        # now actually run the locktests
        for op in config:
            if not isinstance(op, dict):
                assert isinstance(op, int) or isinstance(op, float)
                log.info("sleeping for {sleep} seconds".format(sleep=op))
                time.sleep(op)
                continue
            greenlet = gevent.spawn(lock_one, op, ctx)
            lock_procs.append((greenlet, op))
            time.sleep(0.1) # to provide proper ordering
        #for op in config
        
        for (greenlet, op) in lock_procs:
            log.debug('checking lock for op {op_}'.format(op_=op))
            result = greenlet.get()
            if not result:
                raise Exception("Got wrong result for op {op_}".format(op_=op))
        # for (greenlet, op) in lock_procs

    finally:
        #cleanup!
        if lock_procs:
            for (greenlet, op) in lock_procs:
                log.debug('closing proc for op {op_}'.format(op_=op))
                greenlet.kill(block=True)

        for client in clients:
            (client_remote,)  = ctx.cluster.only(client).remotes.iterkeys()
            (_, _, client_id) = client.partition('.')
            filepath = os.path.join(testdir, 'mnt.{id}'.format(id=client_id), op["lockfile"])
            proc = client_remote.run(
                args=[
                    'rm', '-rf', '{tdir}/lockfile'.format(tdir=testdir),
                    run.Raw(';'),
                    'sudo', 'rm', '-rf', filepath
                    ],
                wait=True
                ) #proc
    #done!
# task

def lock_one(op, ctx):
    """
    Perform the individual lock
    """
    log.debug('spinning up locker with op={op_}'.format(op_=op))
    timeout = None
    proc = None
    result = None
    (client_remote,)  = ctx.cluster.only(op['client']).remotes.iterkeys()
    (_, _, client_id) = op['client'].partition('.')
    testdir = teuthology.get_testdir(ctx)
    filepath = os.path.join(testdir, 'mnt.{id}'.format(id=client_id), op["lockfile"])

    if "maxwait" in op:
        timeout = gevent.Timeout(seconds=float(op["maxwait"]))
        timeout.start()
    try:
        proc = client_remote.run(
            args=[
                'adjust-ulimits',
                'ceph-coverage',
                '{tdir}/archive/coverage'.format(tdir=testdir),
                'daemon-helper',
                'kill',
                '{tdir}/lockfile/sclockandhold'.format(tdir=testdir),
                filepath,
                '{holdtime}'.format(holdtime=op["holdtime"]),
                '{offset}'.format(offset=op.get("offset", '0')),
                '{length}'.format(length=op.get("length", '1')),
                ],
            logger=log.getChild('lockfile_client.{id}'.format(id=client_id)),
            wait=False,
            stdin=run.PIPE,
            check_status=False
            )
        result = proc.exitstatus.get()
    except gevent.Timeout as tout:
        if tout is not timeout:
            raise
        if bool(op["expectfail"]):
            result = 1
        if result is 1:
            if bool(op["expectfail"]):
                log.info("failed as expected for op {op_}".format(op_=op))
            else:
                raise Exception("Unexpectedly failed to lock {op_} within given timeout!".format(op_=op))
    finally: #clean up proc
        if timeout is not None:
            timeout.cancel()
        if proc is not None:
            proc.stdin.close()

    ret = (result == 0 and not bool(op["expectfail"])) or (result == 1 and bool(op["expectfail"]))

    return ret  #we made it through
