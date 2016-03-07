"""
 Long running fio tests on rbd mapped devices for format/features provided in config
 Many fio parameters can be configured so that this task can be used along with thrash/power-cut tests
 and exercise IO on full disk for all format/features
  - This test should not be run on VM due to heavy use of resource

"""
import contextlib
import logging
import StringIO
import re

from teuthology.parallel import parallel
from teuthology import misc as teuthology
from tempfile import NamedTemporaryFile
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    client.0:
       fio-io-size: 100g or 80% or 100m
       fio-version: 2.2.9
       formats: [2]
       features: [[layering],[striping],[layering,exclusive-lock,object-map]]
       test-clone-io: 1  #remove this option to not run create rbd clone and not run io on clone
       io-engine: "sync or rbd or any io-engine"
       rw: randrw
    client.1:
       fio-io-size: 100g
       fio-version: 2.2.9
       rw: read
       image-size:20480

or
    all:
       fio-io-size: 400g
       rw: randrw
       formats: [2]
       features: [[layering],[striping]]
       io-engine: libaio

    Create rbd image + device and exercise IO for format/features provided in config file
    Config can be per client or one config can be used for all clients, fio jobs are run in parallel for client provided

    """
    if config.get('all'):
        client_config = config['all']
    clients = ctx.cluster.only(teuthology.is_type('client'))
    rbd_test_dir = teuthology.get_testdir(ctx) + "/rbd_fio_test"
    for remote,role in clients.remotes.iteritems():
        if 'client_config' in locals():
           with parallel() as p:
               p.spawn(run_fio, remote, client_config, rbd_test_dir)
        else:
           for client_config in config:
              if client_config in role:
                 with parallel() as p:
                     p.spawn(run_fio, remote, config[client_config], rbd_test_dir)

    yield


def run_fio(remote, config, rbd_test_dir):
    """
    create fio config file with options based on above config
    get the fio from github, generate binary, and use it to run on
    the generated fio config file
    """
    fio_config=NamedTemporaryFile(prefix='fio_rbd_', dir='/tmp/', delete=False)
    fio_config.write('[global]\n')
    if config.get('io-engine'):
        ioengine=config['io-engine']
        fio_config.write('ioengine={ioe}\n'.format(ioe=ioengine))
    else:
        fio_config.write('ioengine=sync\n')
    if config.get('bs'):
        bs=config['bs']
        fio_config.write('bs={bs}\n'.format(bs=bs))
    else:
        fio_config.write('bs=4k\n')
    fio_config.write('iodepth=2\n')
    if config.get('fio-io-size'):
        size=config['fio-io-size']
        fio_config.write('size={size}\n'.format(size=size))
    else:
        fio_config.write('size=100m\n')

    fio_config.write('time_based\n')
    if config.get('runtime'):
        runtime=config['runtime']
        fio_config.write('runtime={runtime}\n'.format(runtime=runtime))
    else:
        fio_config.write('runtime=1800\n')
    fio_config.write('allow_file_create=0\n')
    image_size=10240
    if config.get('image_size'):
        image_size=config['image_size']

    formats=[1,2]
    features=[['layering'],['striping'],['exclusive-lock','object-map']]
    fio_version='2.2.11'
    if config.get('formats'):
        formats=config['formats']
    if config.get('features'):
        features=config['features']
    if config.get('fio-version'):
        fio_version=config['fio-version']

    fio_config.write('norandommap\n')
    if ioengine == 'rbd':
        fio_config.write('invalidate=0\n')
    #handle package required for librbd engine
    sn=remote.shortname
    system_type= teuthology.get_system_type(remote)
    if system_type == 'rpm' and ioengine == 'rbd':
        log.info("Installing librbd1 devel package on {sn}".format(sn=sn))
        remote.run(args=['sudo', 'yum' , 'install', 'librbd1-devel', '-y'])
    elif ioengine == 'rbd':
        log.info("Installing librbd devel package on {sn}".format(sn=sn))
        remote.run(args=['sudo', 'apt-get', '-y',
                         '--force-yes',
                         'install', 'librbd-dev'])
    if ioengine == 'rbd':
        fio_config.write('clientname=admin\n')
        fio_config.write('pool=rbd\n')
    for frmt in formats:
        for feature in features:
           log.info("Creating rbd images on {sn}".format(sn=sn))
           feature_name = '-'.join(feature)
           rbd_name = 'i{i}f{f}{sn}'.format(i=frmt,f=feature_name,sn=sn)
           rbd_snap_name = 'i{i}f{f}{sn}@i{i}f{f}{sn}Snap'.format(i=frmt,f=feature_name,sn=sn)
           rbd_clone_name = 'i{i}f{f}{sn}Clone'.format(i=frmt,f=feature_name,sn=sn)
           create_args=['sudo', 'rbd', 'create',
                        '--size', '{size}'.format(size=image_size),
                        '--image', rbd_name,
                        '--image-format', '{f}'.format(f=frmt)]
           map(lambda x: create_args.extend(['--image-feature', x]), feature)
           remote.run(args=create_args)
           remote.run(args=['sudo', 'rbd', 'info', rbd_name])
           if ioengine != 'rbd':
               out=StringIO.StringIO()
               remote.run(args=['sudo', 'rbd', 'map', rbd_name ],stdout=out)
               dev=re.search(r'(/dev/rbd\d+)',out.getvalue())
               rbd_dev=dev.group(1)
               if config.get('test-clone-io'):
                    log.info("Testing clones using fio")
                    remote.run(args=['sudo', 'rbd', 'snap', 'create', rbd_snap_name])
                    remote.run(args=['sudo', 'rbd', 'snap', 'protect', rbd_snap_name])
                    remote.run(args=['sudo', 'rbd', 'clone', rbd_snap_name, rbd_clone_name])
                    remote.run(args=['sudo', 'rbd', 'map', rbd_clone_name], stdout=out)
                    dev=re.search(r'(/dev/rbd\d+)',out.getvalue())
                    rbd_clone_dev=dev.group(1)
               fio_config.write('[{rbd_dev}]\n'.format(rbd_dev=rbd_dev))
               if config.get('rw'):
                   rw=config['rw']
                   fio_config.write('rw={rw}\n'.format(rw=rw))
               else:
                   fio_config .write('rw=randrw\n')
               fio_config.write('filename={rbd_dev}\n'.format(rbd_dev=rbd_dev))
               if config.get('test-clone-io'):
                   fio_config.write('[{rbd_clone_dev}]\n'.format(rbd_clone_dev=rbd_clone_dev))
                   fio_config.write('rw={rw}\n'.format(rw=rw))
                   fio_config.write('filename={rbd_clone_dev}\n'.format(rbd_clone_dev=rbd_clone_dev))
           else:
               if config.get('test-clone-io'):
                    log.info("Testing clones using fio")
                    remote.run(args=['sudo', 'rbd', 'snap', 'create', rbd_snap_name])
                    remote.run(args=['sudo', 'rbd', 'snap', 'protect', rbd_snap_name])
                    remote.run(args=['sudo', 'rbd', 'clone', rbd_snap_name, rbd_clone_name])
               fio_config.write('[{img_name}]\n'.format(img_name=rbd_name))
               if config.get('rw'):
                   rw=config['rw']
                   fio_config.write('rw={rw}\n'.format(rw=rw))
               else:
                   fio_config.write('rw=randrw\n')
               fio_config.write('rbdname={img_name}\n'.format(img_name=rbd_name))
               if config.get('test-clone-io'):
                   fio_config.write('[{clone_img_name}]\n'.format(clone_img_name=rbd_clone_name))
                   fio_config.write('rw={rw}\n'.format(rw=rw))
                   fio_config.write('rbdname={clone_img_name}\n'.format(clone_img_name=rbd_clone_name))


    fio_config.close()
    remote.put_file(fio_config.name,fio_config.name)
    try:
        log.info("Running rbd feature - fio test on {sn}".format(sn=sn))
        fio = "https://github.com/axboe/fio/archive/fio-" + fio_version + ".tar.gz"
        remote.run(args=['mkdir', run.Raw(rbd_test_dir),])
        remote.run(args=['cd' , run.Raw(rbd_test_dir),
                         run.Raw(';'), 'wget' , fio , run.Raw(';'), run.Raw('tar -xvf fio*tar.gz'), run.Raw(';'),
                         run.Raw('cd fio-fio*'), 'configure', run.Raw(';') ,'make'])
        remote.run(args=['sudo', 'ceph', '-s'])
        remote.run(args=['sudo', run.Raw('{tdir}/fio-fio-{v}/fio {f}'.format(tdir=rbd_test_dir,v=fio_version,f=fio_config.name))])
        remote.run(args=['sudo', 'ceph', '-s'])
    finally:
        log.info("Cleaning up fio install")
        remote.run(args=['rm','-rf', run.Raw(rbd_test_dir)])
        if system_type == 'rpm' and ioengine == 'rbd':
            log.info("Uninstall librbd1 devel package on {sn}".format(sn=sn))
            remote.run(args=['sudo', 'yum' , 'remove', 'librbd1-devel', '-y'])
        elif ioengine == 'rbd':
            log.info("Uninstall librbd devel package on {sn}".format(sn=sn))
            remote.run(args=['sudo', 'apt-get', '-y', 'remove', 'librbd-dev'])
