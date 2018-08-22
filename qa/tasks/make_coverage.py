'''
Compiling ceph from source on each node
for code coverage runs
'''
import contextlib
import logging
import os
import re
import shlex
import subprocess

from cStringIO import StringIO
from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.orchestra import run
from teuthology.config import config as teuth_config

log = logging.getLogger(__name__)
COVERAGE_LOGS = "/a/code_coverage_logs/"


def get_sourcedir(ctx, config, remote, rdir):
	'''
	get source code directory along with gcno files
        on the machine where we want to generate report
	'''
	ldir = "/tmp/build/{}/".format(os.path.basename(os.path.dirname(ctx.archive)))
	if not os.path.exists(ldir):
		os.makedirs(ldir)
#o	teuthology.pull_directory(remote, rdir, ldir)
	cmd="scp -r {rhost}:{rpath} {lpath}".format(\
		rhost=remote.name, rpath=rdir, \
		lpath=ldir)
	log.info(cmd)

	p=subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
	p_stdout=p.communicate()[0]
	log.info(p_stdout)


def copy_compile_utils(ctx, config, remote):
	FILES=['update-cmakelists', 'update-docmake']
	destdir = '/usr/bin'
	for filename in FILES:
		log.info('Shipping %r...', filename)
		src = os.path.join(os.path.dirname(__file__), "util/"+filename)
		dst = os.path.join(destdir, filename)
		with file(src, 'rb') as f:
			teuthology.sudo_write_file(
				remote=remote,
				path=dst,
				data=f,
			)
			remote.run(
				args=[
				    'sudo',
				    'chmod',
				    'a=rx',
                                    '--',
                                    dst,
                                ]
 			)


def copy_utils(ctx, config, remote):
	FILES = ['daemon-helper', 'adjust-ulimits']
	destdir = '/usr/bin'
	for filename in FILES:
		log.info('Shipping %r...', filename)
		src = os.path.join(os.path.dirname(teuthology.__file__), "task/install/"+filename)
		dst = os.path.join(destdir, filename)
		with file(src, 'rb') as f:
		    teuthology.sudo_write_file(
			remote=remote,
			path=dst,
			data=f,
		    )
		    f.seek(0)
		    remote.run(
			args=[
			    'sudo',
			    'chmod',
			    'a=rx',
			    '--',
			    dst,
			]
		    )


def get_dependencies(ctx, config, remote):
	#1. enable repos
	#2. get dependencies
	cmd='sudo subscription-manager --force register \
		--serverurl=subscription.rhsm.stage.redhat.com:443/subscription \
		--baseurl=https://cdn.stage.redhat.com --username=qa@redhat.com \
		--password=redhatqa --auto-attach'
	r=remote.run(args=cmd)
	cmd='sudo subscription-manager repos --enable=rhel-7-server-extras-rpms'
	r=remote.run(args=cmd)
	cmd='sudo subscription-manager repos --enable=rhel-7-server-optional-rpms'
	r=remote.run(args=cmd)
	cmd='sudo sed -i -- \'s/enabled=0/enabled=1/g\' /etc/yum.repos.d/epel.repo'
	r=remote.run(args=cmd)
	cmd='sudo yum repolist'
	r=remote.run(args=cmd)

	cmd = "sudo bash -c '(cd /sourcebuild/ceph && exec git checkout luminous)'"
	r=remote.run(args=cmd)
	cmd="sudo bash -c '(cd /sourcebuild/ceph && exec  git submodule update --init --recursive)'"
	r=remote.run(args=cmd)
	cmd='sudo sed -i -- \'s/yum install subscription-manager/yum install \
		-y subscription-manager/g\' /sourcebuild/ceph/install-deps.sh'
	r=remote.run(args=cmd)
	cmd="sudo bash -c '(cd /sourcebuild/ceph && exec  ./install-deps.sh)'"
	r=remote.run(args=cmd)
	cmd="sudo bash -c '(sudo  yum -y install lcov)'"
	r=remote.run(args=cmd)

	copy_compile_utils(ctx, config, remote)

	#edit CMakefiles.txt and do_cmake.sh according to gcov need
	cmd="sudo update-cmakelists " + "/sourcebuild/ceph/CMakeLists.txt"
	r=remote.run(args=cmd)
	#1. set INSTALL_PREFIX=/usr in do_cmake
	cmd="sudo update-docmake " + "/sourcebuild/ceph/do_cmake.sh"
	r=remote.run(args=cmd)

	#cmd="sudo bash -c '(cd /sourcebuild/ceph && source scl_source enable devtoolset-7 && exec ./do_cmake.sh)'"
	cmd="sudo bash -c '(cd /sourcebuild/ceph && exec ./do_cmake.sh)'"
	r=remote.run(args=cmd)
	cmd="sudo bash -c '(cd /sourcebuild/ceph/build && exec make -j4)'"
	r=remote.run(args=cmd)
	cmd="sudo bash -c '(cd /sourcebuild/ceph/build && exec make install)'"
	r=remote.run(args=cmd)
	#cmd="sudo bash -c '(cp -ar  /usr/local/lib64/python2.7/site-packages/* /usr/lib64/python2.7/site-packages/)'"
	#r=remote.run(args=cmd)
	cmd="sudo mkdir /etc/ceph"
	r=remote.run(args=cmd)
	cmd="sudo mkdir /var/log/ceph"
	r=remote.run(args=cmd)
	cmd="sudo mkdir /var/lib/ceph"
	r=remote.run(args=cmd)

	''' copy adjust-ulimits, daemon-helper scripts '''
	copy_utils(ctx, config, remote)

	#update permissions for testdir so that
	# teuthology can delete archive/coverage/gcda files
	test_dir=teuthology.get_testdir(ctx)
	cmd="sudo chmod -R 0777 {tdir}".format(tdir=test_dir)
	r=remote.run(args=cmd)

	#before CEPH task
	#- ceph:
	    #conf:
	     # osd:
	     #   osd max object name len : 400
	     #   osd max object namespace len : 64
	#TODO
	#gather gcda files
	#aggreagate
	#publish report


def start_compile(ctx, config, remote, builddir):
	#1. edit cmakefile for gcov entry
	#2.compile
	pass


def compile_with_gcov(ctx, config, grepo, remote, builddir):
	''' please use variables once task working'''
	try:
		r = remote.run(args=['sudo', 'mkdir', '/sourcebuild'], \
				)
	except:
		pass

	r = remote.run (args=['sudo', 'git','config',\
				'--global','http.sslVerify', 'false']
				)
#	r = remote.run(args=['sudo', 'git', 'clone',\
#			'https://gitlab.cee.redhat.com/ceph/ceph.git','/sourcebuild/ceph'], timeout=3600,\
#			check_status=False, wait=True)

	r = remote.run(args=['sudo', 'git', 'clone',\
			'https://github.com/ceph/ceph.git','/sourcebuild/ceph'], timeout=3600,\
			check_status=False, wait=True)

	get_dependencies(ctx, config, remote)
	#start_compile()




@contextlib.contextmanager
def task(ctx, config):
	"""
	This will replace Install task for the tests.
	fetch source from a git repo on all the nodes and compile
  	them locally. this is because we need to preserve .gcno files
	for gcov report generation purpose.

	ex:
	- tasks:
	    make-coverage:
	    ceph:
	    radosbench:
	"""
	''' Hard coding for POC purpose'''

	grepo="https://gitlab.cee.redhat.com/ceph/ceph.git"
	builddir="/sourcebuild/ceph/"

	with parallel() as ptask:
		for remote, roles in ctx.cluster.remotes.iteritems():
			ptask.spawn(compile_with_gcov, ctx, config, grepo, remote, builddir)


	'''
	Transfer source dir which has gcno files and source
	only from one node
	'''

	log.info("Transferring the source directory")
	remote=next(iter(ctx.cluster.remotes))
	get_sourcedir(ctx, config, remote, builddir)

	'''create dir with run name in COVERAGE_LOGS directory'''
	rundir=COVERAGE_LOGS+os.path.basename(os.path.dirname(ctx.archive))
	log.info("rundir = {}".format(rundir))
	try:
		os.mkdir(rundir)
	except:
		log.error("Failed to create rundir")

	yield
	#TODO: cleanup still pending


