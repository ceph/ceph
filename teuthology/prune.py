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
    remotes_days = int(args['--remotes'])

    prune_archive(archive_dir, pass_days, remotes_days, dry_run)


def prune_archive(archive_dir, pass_days, remotes_days, dry_run=False):
    """
    Walk through the archive_dir, calling the cleanup functions to process
    directories that might be old enough
    """
    max_days = max(pass_days, remotes_days)
    log.debug("Archive {archive} has {count} children".format(
        archive=archive_dir, count=len(os.listdir(archive_dir))))
    # Use full paths
    children = map(
        lambda p: os.path.join(archive_dir, p),
        listdir(archive_dir)
    )
    run_dirs = list()
    for child in children:
        # Ensure that the path is not a symlink, is a directory, and is old
        # enough to process
        if (not os.path.islink(child) and os.path.isdir(child) and
                is_old_enough(child, max_days)):
            run_dirs.append(child)
    run_dirs.sort(key=lambda p: os.path.getctime(p), reverse=True)
    for run_dir in run_dirs:
        log.debug("Processing %s ..." % run_dir)
        maybe_remove_passes(run_dir, pass_days, dry_run)
        maybe_remove_remotes(run_dir, remotes_days, dry_run)


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


def maybe_remove_passes(run_dir, days, dry_run=False):
    """
    Remove entire job log directories if they are old enough and the job passed
    """
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
        # Is it a job dir?
        summary_path = os.path.join(item, 'summary.yaml')
        if not os.path.exists(summary_path):
            continue
        # Is it a passed job?
        summary_lines = [line.strip() for line in
                         file(summary_path).readlines()]
        if 'success: true' in summary_lines:
            log.info("{job} is a {days}-day old passed job; removing".format(
                job=item, days=days))
            if not dry_run:
                remove(item)


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
        for (subdir, description) in subdirs.iteritems():
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
