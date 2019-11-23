import gzip
import logging
import os
import shutil
import time

import teuthology
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)


# If we see this in any directory, we do not prune it
PRESERVE_FILE = '.preserve'


def main(args):
    """
    Main function; parses args and calls prune_archive()
    """
    verbose = args['--verbose']
    if verbose:
        teuthology.log.setLevel(logging.DEBUG)
    archive_dir = args['--archive']
    dry_run = args['--dry-run']
    pass_days = int(args['--pass'])
    fail_days = int(args['--fail'])
    remotes_days = int(args['--remotes'])
    compress_days = int(args['--compress'])

    prune_archive(
        archive_dir, pass_days, fail_days, remotes_days, compress_days, dry_run
    )


def prune_archive(
        archive_dir,
        pass_days,
        fail_days,
        remotes_days,
        compress_days,
        dry_run=False,
):
    """
    Walk through the archive_dir, calling the cleanup functions to process
    directories that might be old enough
    """
    min_days = min(filter(
        lambda n: n >= 0, [pass_days, fail_days, remotes_days]))
    log.debug("Archive {archive} has {count} children".format(
        archive=archive_dir, count=len(os.listdir(archive_dir))))
    # Use full paths
    children = [os.path.join(archive_dir, p) for p in listdir(archive_dir)]
    run_dirs = list()
    for child in children:
        # Ensure that the path is not a symlink, is a directory, and is old
        # enough to process
        if (not os.path.islink(child) and os.path.isdir(child) and
                is_old_enough(child, min_days)):
            run_dirs.append(child)
    run_dirs.sort(key=lambda p: os.path.getctime(p), reverse=True)
    for run_dir in run_dirs:
        log.debug("Processing %s ..." % run_dir)
        maybe_remove_jobs(run_dir, pass_days, fail_days, dry_run)
        maybe_remove_remotes(run_dir, remotes_days, dry_run)
        maybe_compress_logs(run_dir, compress_days, dry_run)


def listdir(path):
    with safe_while(sleep=1, increment=1, tries=10) as proceed:
        while proceed():
            try:
                return os.listdir(path)
            except OSError:
                log.exception("Failed to list %s !" % path)


def should_preserve(dir_name):
    """
    Should the directory be preserved?

    :returns: True if the directory contains a file named '.preserve'; False
              otherwise
    """
    preserve_path = os.path.join(dir_name, PRESERVE_FILE)
    if os.path.isdir(dir_name) and os.path.exists(preserve_path):
        return True
    return False


def is_old_enough(file_name, days):
    """
    :returns: True if the file's modification date is earlier than the amount
              of days specified
    """
    if days < 0:
        return False
    now = time.time()
    secs_to_days = lambda s: s / (60 * 60 * 24)
    age = now - os.path.getmtime(file_name)
    if secs_to_days(age) > days:
        return True
    return False


def remove(path):
    """
    Attempt to recursively remove a directory. If an OSError is encountered,
    log it and continue.
    """
    try:
        shutil.rmtree(path)
    except OSError:
        log.exception("Failed to remove %s !" % path)


def maybe_remove_jobs(run_dir, pass_days, fail_days, dry_run=False):
    """
    Remove entire job log directories if they are old enough and the job passed
    """
    if pass_days < 0 and fail_days < 0:
        return
    contents = listdir(run_dir)
    if PRESERVE_FILE in contents:
        return
    for child in contents:
        job_path = os.path.join(run_dir, child)
        # Ensure the path isn't marked for preservation and that it is a
        # directory
        if should_preserve(job_path) or not os.path.isdir(job_path):
            continue
        # Is it a job dir?
        summary_path = os.path.join(job_path, 'summary.yaml')
        if not os.path.exists(summary_path):
            continue
        # Depending on whether it passed or failed, we have a different age
        # threshold
        summary_lines = [line.strip() for line in
                         open(summary_path).readlines()]
        if 'success: true' in summary_lines:
            status = 'passed'
            days = pass_days
        elif 'success: false' in summary_lines:
            status = 'failed'
            days = fail_days
        else:
            continue
        # Ensure the directory is old enough to remove
        if not is_old_enough(summary_path, days):
            continue
        log.info("{job} is a {days}-day old {status} job; removing".format(
            job=job_path, days=days, status=status))
        if not dry_run:
            remove(job_path)


def maybe_remove_remotes(run_dir, days, dry_run=False):
    """
    Remove remote logs (not teuthology logs) from job directories if they are
    old enough
    """
    if days < 0:
        return
    contents = listdir(run_dir)
    subdirs = dict(
        remote='remote logs',
        data='mon data',
    )
    if PRESERVE_FILE in contents:
        return
    for child in contents:
        item = os.path.join(run_dir, child)
        # Ensure the path isn't marked for preservation, that it is a
        # directory, and that it is old enough
        if (should_preserve(item) or not os.path.isdir(item) or not
                is_old_enough(item, days)):
            continue
        for (subdir, description) in subdirs.items():
            _maybe_remove_subdir(item, subdir, days, description, dry_run)


def _maybe_remove_subdir(job_dir, subdir, days, description, dry_run=False):
    # Does the subdir exist?
    subdir_path = os.path.join(job_dir, subdir)
    if not os.path.isdir(subdir_path):
        return
    log.info("{job} is {days} days old; removing {desc}".format(
        job=job_dir,
        days=days,
        desc=description,
    ))
    if not dry_run:
        remove(subdir_path)


def maybe_compress_logs(run_dir, days, dry_run=False):
    if days < 0:
        return
    contents = listdir(run_dir)
    if PRESERVE_FILE in contents:
        return
    for child in contents:
        item = os.path.join(run_dir, child)
        # Ensure the path isn't marked for preservation, that it is a
        # directory, and that it is old enough
        if (should_preserve(item) or not os.path.isdir(item) or not
                is_old_enough(item, days)):
            continue
        log_name = 'teuthology.log'
        log_path = os.path.join(item, log_name)
        if not os.path.exists(log_path):
            continue
        log.info("{job} is {days} days old; compressing {name}".format(
            job=item,
            days=days,
            name=log_name,
        ))
        if dry_run:
            continue
        zlog_path = log_path + '.gz'
        try:
            _compress(log_path, zlog_path)
        except Exception:
            log.exception("Failed to compress %s", log_path)
            os.remove(zlog_path)
        else:
            os.remove(log_path)


def _compress(in_path, out_path):
    """
    Compresses a file using gzip, preserving the original permissions, atime,
    and mtime.  Does not remove the original.
    """
    with open(in_path, 'rb') as src, gzip.open(out_path, 'wb') as dest:
        shutil.copyfileobj(src, dest)
        shutil.copystat(in_path, out_path)
