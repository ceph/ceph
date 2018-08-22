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

from subprocess import Popen
from cStringIO import StringIO
from teuthology import misc as teuthology
from teuthology.parallel import parallel
from teuthology.orchestra import run, remote
from teuthology.config import config as teuth_config

log = logging.getLogger(__name__)
COVERAGE_LOGS="/a/code_coverage_logs/"
run_dir=""

def aggregate_info(ctx, tdir, curator, ilist, run_dir):
	'''
	Aggregate the results from all the nodes to generate
    	a single .info file
	'''

	coverdir="/tmp/coverage"
	JID_PREFIX=os.path.basename(ctx.archive)
	cmd = "mkdir -p {}".format(coverdir)
	r=curator.run(args=cmd)
	dst=""

	mergefile=coverdir+"/coverage_merged.info"

	cmd="lcov --rc  lcov_branch_coverage=1 "
	for ent in ilist:
		addstr=" -a "+tdir+ent
		tstr=" -t "+ent.split(".")[0]
		cmd = cmd + addstr+tstr
	cmd = cmd +" -o "+mergefile
	log.info(cmd)

	r=curator.run(args=cmd)

	'''
	  rename file with JID_PREFIX
	'''
	new_mergefile=coverdir+"/"+JID_PREFIX+"_coverage_merged.info"
	cmd = "mv "+mergefile+" "+new_mergefile
	log.info("renaming {src} to {dst}".format(src=mergefile,\
			dst=new_mergefile))
	log.info(cmd)
	r=curator.run(args=cmd)

	'''
	  transfer file to teuthology master node
	'''
	assert(os.path.exists(run_dir))
	dst=curator.get_file(new_mergefile, False, run_dir)
	assert(os.path.exists(dst))
	log.info("xfrd file {} to {}".format(new_mergefile, dst))


	'''
	cmd="genhtml -o {archive_dir} {coverfile}".format( \
		archive_dir=coverdir, coverfile=mergefile)
	log.info(cmd)
	r=curator.run(args=cmd)
	teuthology.pull_directory(curator, coverdir, ctx.archive)
	'''

def _gather_data(ctx, remote, rpath, curator):
	tdir="/tmp/cov/"+ctx.archive
	lpath=""
	if remote != curator:
		if not os.path.exists(tdir):
			os.makedirs(tdir)
		lpath=remote.get_file(rpath, False, tdir)
		assert(os.path.exists(lpath))
		log.info("Transfered file {} to {} ".format(rpath, lpath))
		#pass the file to curator
		# direct copying from remote->remote not working
		fn=os.path.basename(rpath)
		src=tdir+"/{fn}".format(fn=fn)

		with file(src, 'rb') as f:
			teuthology.sudo_write_file(
				remote=curator,
				path=rpath,
				data=f,
			)
			remote.run(
				args=[
					'sudo',
					'chmod',
					'a=rx',
					'--',
					rpath,
				]
			)



def gather_data(ctx, config, remote, srcdir, curator):
	this_infofile="{hostname}.info".format(hostname=remote.shortname)
	''' Generate .info file on respective nodes and copy it to
            local node
	'''
	cmd="sudo bash -c '(cd /sourcebuild/ceph && \
		exec lcov --rc lcov_branch_coverage=1 \
		--capture -d . --output-file {nodename})'"\
			.format(nodename=this_infofile)
	log.info(cmd)
	r=remote.run(args=cmd)

	_gather_data(ctx, remote, srcdir+this_infofile, curator)

@contextlib.contextmanager
def task(ctx, config):
	"""
	generate reports on respective nodes but fetch all of them to local
	so that we can merge the reports
	"""
	global run_dir
	srcdir="/sourcebuild/ceph/"
	dstdir="{ctxdir}/".format(ctxdir=ctx.archive)
	run_dir=COVERAGE_LOGS+os.path.basename(os.path.dirname(ctx.archive))+"/"


	if os.path.exists(dstdir):
		print 'coverage directory present'
	else:
		os.makedirs(dstdir)
	curator=ctx.cluster.remotes.keys()[0]

	with parallel() as ptask:
		for remote, roles in ctx.cluster.remotes.iteritems():
			ptask.spawn(gather_data, ctx, config, remote, srcdir, curator)
	ilist=[]
	for remote, roles in ctx.cluster.remotes.iteritems():
		node=remote.shortname+".info"
		ilist.append(node)

	aggregate_info(ctx, srcdir, curator, ilist, run_dir)
	yield

	log.info("DONE WITH COLLECTION")


