# Origin: https://github.com/jcsp/scrape/blob/master/scrape.py
# Author: John Spray (github.com/jcsp)

import difflib
from errno import ENOENT
from gzip import GzipFile
import sys
import os
import yaml
from collections import defaultdict
import re
import logging
import subprocess

import six

log = logging.getLogger('scrape')
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)


class Reason(object):
    def get_description(self):
        return self.description

    def get_detail(self):
        return None


def grep(path, expr):
    """
    Call out to native grep rather than feeding massive log files through python line by line
    """
    p = subprocess.Popen(["grep", expr, path], stdout=subprocess.PIPE)
    p.wait()
    out, err = p.communicate()
    if p.returncode == 0:
        return six.ensure_str(out).split("\n")
    else:
        return []


class GenericReason(Reason):
    """
    A reason inferred from a Job: matches Jobs with an apparently-similar failure
    """

    def __init__(self, job, description=None):
        self.failure_reason = job.get_failure_reason()
        self.description = description

        self.backtrace = job.get_backtrace()
        if self.backtrace:
            log.debug("Found a backtrace!\n{0}".format(self.backtrace))

    def get_detail(self):
        return self.backtrace

    def get_description(self):
        if self.description:
            return self.description
        else:
            if self.backtrace:
                return "Crash: {0}".format(self.failure_reason)
            else:
                return "Failure: {0}".format(self.failure_reason)

    def match(self, job):
        # I never match dead jobs
        if job.get_failure_reason() is None:
            return False

        # If one has a backtrace but the other doesn't, we're a different thing even if the official
        # failure_reason is the same
        if (self.backtrace is None) != (job.get_backtrace() is None):
            return False

        # If we have the same backtrace, we're a match even if the teuthology failure_reason
        # doesn't match (a crash is a crash, it can have different symptoms)
        if self.backtrace:
            ratio = difflib.SequenceMatcher(None, self.backtrace, job.get_backtrace()).ratio()
            return ratio > 0.5
        else:
            if "Test failure:" in self.failure_reason:
                return self.failure_reason == job.get_failure_reason()
            elif re.search("workunit test (.*)\) on ", self.failure_reason):
                workunit_name = re.search("workunit test (.*)\) on ", self.failure_reason).group(1)
                other_match = re.search("workunit test (.*)\) on ", job.get_failure_reason())
                return other_match is not None and workunit_name == other_match.group(1)
            else:
                reason_ratio = difflib.SequenceMatcher(None, self.failure_reason, job.get_failure_reason()).ratio()
                return reason_ratio > 0.5


class RegexReason(Reason):
    """
    A known reason matching a particular regex to failure reason
    """

    def __init__(self, regexes, description):
        self.description = description
        if isinstance(regexes, list):
            self.regexes = regexes
        else:
            self.regexes = [regexes]

    def match(self, job):
        # I never match dead jobs
        if job.get_failure_reason() is None:
            return False

        for regex in self.regexes:
            if re.match(regex, job.get_failure_reason()):
                return True

        return False


class AssertionReason(Reason):
    def __init__(self, job):
        self.assertion = job.get_assertion()
        self.backtrace = job.get_backtrace()

    def get_description(self):
        return "Assertion: {0}".format(self.assertion)

    def get_detail(self):
        return self.backtrace

    @classmethod
    def could_be(cls, job):
        return job.get_assertion() is not None

    def match(self, job):
        return self.assertion == job.get_assertion()


class LockdepReason(AssertionReason):
    """
    Different to a normal assertion, because matches should not only
    have the same assertion but the same backtrace (don't want to glob
    all lockdep failures together if they are really being tripped in
    different places)
    """
    @classmethod
    def could_be(cls, job):
        if not super(LockdepReason, cls).could_be(job):
            return False

        return "common/lockdep" in job.get_assertion()

    def get_description(self):
        return "Lockdep: {0}".format(self.assertion)

    def match(self, job):
        if not super(LockdepReason, self).match(job):
            return False

        if self.backtrace:
            if job.get_backtrace():
                ratio = difflib.SequenceMatcher(None, self.backtrace, job.get_backtrace()).ratio()
                return ratio > 0.5
            else:
                return False
        else:
            # No backtrace to compare about, allow matches based purely on assertion
            return True


class DeadReason(Reason):
    """
    A reason for picking up jobs with no summary.yaml
    """
    def __init__(self, job):
        self.description = "Dead"
        self.last_tlog_line = job.get_last_tlog_line()
        self.backtrace = job.get_backtrace()

    def get_description(self):
        return "Dead: {0}".format(self.last_tlog_line)

    def get_detail(self):
        return self.backtrace

    @classmethod
    def could_be(cls, job):
        return job.summary_data is None

    def match(self, job):
        if job.summary_data:
            return False

        if self.backtrace:
            if job.get_backtrace():
                # We both have backtrace: use that to decide if we're the same
                ratio = difflib.SequenceMatcher(None, self.backtrace, job.get_backtrace()).ratio()
                return ratio > 0.5
            else:
                # I have BT but he doesn't, so we're different
                return False

        if self.last_tlog_line or job.get_last_tlog_line():
            ratio = difflib.SequenceMatcher(None, self.last_tlog_line,
                                            job.get_last_tlog_line()).ratio()
            return ratio > 0.5
        return True


class TimeoutReason(Reason):
    def __init__(self, job):
        self.timeout, self.command = self.get_timeout(job)

    def get_description(self):
        return "Timeout {0} running {1}".format(
            self.timeout, self.command
        )

    @classmethod
    def could_be(cls, job):
        return cls.get_timeout(job) is not None

    @classmethod
    def get_timeout(cls, job):
        if job.get_failure_reason() is None:
            return None

        match = re.search("status 124:.* timeout ([^ ]+) ([^']+)'", job.get_failure_reason())
        if not match:
            return

        timeout, bin_path = match.groups()

        # Given a path like /home/ubuntu/cephtest/workunit.client.0/cephtool/test.sh
        # ... strip it down to cephtool/test.sh
        parts = bin_path.split(os.path.sep)
        parts.reverse()
        rparts = []
        for p in parts:
            if 'workunit.' in p or 'cephtest' in p:
                break
            else:
                rparts.append(p)
        rparts.reverse()
        command = os.path.sep.join(rparts)

        return timeout, command

    def match(self, job):
        return self.get_timeout(job) == (self.timeout, self.command)

MAX_TEUTHOLOGY_LOG = 1024 * 1024 * 100
MAX_SVC_LOG = 100 * 1024 * 1024
MAX_BT_LINES = 100


class Job(object):
    def __init__(self, path, job_id):
        self.path = path
        self.job_id = job_id

        try:
            self.config = yaml.safe_load(open(os.path.join(self.path, "config.yaml"), 'r'))
            self.description = self.config['description']
            assert self.description
        except IOError:
            self.config = None
            self.description = None

        summary_path = os.path.join(self.path, "summary.yaml")
        try:
            self.summary_data = yaml.safe_load(open(summary_path, 'r'))
        except IOError:
            self.summary_data = None

        self.backtrace = None
        self.assertion = None
        self.populated = False

    def get_success(self):
        if self.summary_data:
            return self.summary_data['success']
        else:
            return False

    def get_failure_reason(self):
        if self.summary_data:
            return self.summary_data['failure_reason']
        else:
            return None

    def get_last_tlog_line(self):
        t_path = os.path.join(self.path, "teuthology.log")
        if not os.path.exists(t_path):
            return None
        else:
            out, err = subprocess.Popen(["tail", "-n", "1", t_path], stdout=subprocess.PIPE).communicate()
            return out.strip()

    def _search_backtrace(self, file_obj):
        bt_lines = []
        assertion = None
        for line in file_obj:
            # Log prefix from teuthology.log
            if ".stderr:" in line:
                line = line.split(".stderr:")[1]

            if "FAILED assert" in line:
                assertion = line.strip()

            if line.startswith(" ceph version"):
                # The start of a backtrace!
                bt_lines = [line]
            elif line.startswith(" NOTE: a copy of the executable"):
                # The backtrace terminated, if we have a buffer return it
                if len(bt_lines):
                    return ("".join(bt_lines)).strip(), assertion
                else:
                    log.warning("Saw end of BT but not start")
            elif bt_lines:
                # We're in a backtrace, push the line onto the list
                if len(bt_lines) > MAX_BT_LINES:
                    # Something wrong with our parsing, drop it
                    log.warning("Ignoring malparsed backtrace: {0}".format(
                        ", ".join(bt_lines[0:3])
                    ))
                    bt_lines = []
                bt_lines.append(line)

        return None, assertion

    def get_assertion(self):
        if not self.populated:
            self._populate_backtrace()
        return self.assertion

    def get_backtrace(self):
        if not self.populated:
            self._populate_backtrace()
        return self.backtrace

    def _populate_backtrace(self):
        tlog_path = os.path.join(self.path, "teuthology.log")
        try:
            s = os.stat(tlog_path)
        except OSError:
            log.warning("Missing teuthology log {0}".format(tlog_path))
            return None
        size = s.st_size
        if size > MAX_TEUTHOLOGY_LOG:
            log.debug("Ignoring teuthology log for job {0}, it is {1} bytes".format(self.job_id, size))
            return None

        self.backtrace, self.assertion = self._search_backtrace(open(tlog_path))
        if self.backtrace:
            return

        for line in grep(tlog_path, "command crashed with signal"):
            log.debug("Found a crash indication: {0}".format(line))
            # tasks.ceph.osd.1.plana82.stderr
            match = re.search("tasks.ceph.([^\.]+).([^\.]+).([^\.]+).stderr", line)
            if not match:
                log.warning("Not-understood crash indication {0}".format(line))
                continue
            svc, svc_id, hostname = match.groups()
            gzipped_log_path = os.path.join(
                self.path, "remote", hostname, "log", "ceph-{0}.{1}.log.gz".format(svc, svc_id))

            try:
                s = os.stat(gzipped_log_path)
            except OSError as e:
                if e.errno == ENOENT:
                    log.warning("Missing log {0}".format(gzipped_log_path))
                    continue
                else:
                    raise

            size = s.st_size
            if size > MAX_SVC_LOG:
                log.warning("Not checking for backtrace from {0}:{1}.{2} log, too large ({3})".format(
                    hostname, svc, svc_id, size
                ))
                continue

            bt, ass = self._search_backtrace(GzipFile(gzipped_log_path))
            if ass and not self.assertion:
                self.assertion = ass
            if bt:
                self.backtrace = bt
                return

        return None


class ValgrindReason(Reason):
    def __init__(self, job):
        assert self.could_be(job)
        self.service_types = self._get_service_types(job)

    def _get_service_types(self, job):
        """
        Get dict mapping service type 'osd' etc to sorted list of violation types 'Leak_PossiblyLost' etc
        """

        result = defaultdict(list)
        # Lines like:
        # 2014-08-22T20:07:18.668 ERROR:tasks.ceph:saw valgrind issue   <kind>Leak_DefinitelyLost</kind> in /var/log/ceph/valgrind/osd.3.log.gz
        for line in grep(os.path.join(job.path, "teuthology.log"), "</kind> in "):
            match = re.search("<kind>(.+)</kind> in .+/(.+)", line)
            if not match:
                log.warning("Misunderstood line: {0}".format(line))
                continue
            err_typ, log_basename = match.groups()
            svc_typ = six.ensure_str(log_basename).split(".")[0]
            if err_typ not in result[svc_typ]:
                result[svc_typ].append(err_typ)
                result[svc_typ] = sorted(result[svc_typ])

        return dict(result)

    def get_description(self):
        desc_bits = []
        for service, types in list(self.service_types.items()):
            desc_bits.append("{0} ({1})".format(service, ", ".join(types)))
        return "Valgrind: " + ", ".join(desc_bits)

    @classmethod
    def could_be(cls, job):
        return job.get_failure_reason() is not None and "saw valgrind issues" in job.get_failure_reason()

    def match(self, job):
        return self._get_service_types(job) == self.service_types


known_reasons = [
    # If the failure reason indicates no packages found...
    RegexReason(["Failed to fetch package version from http://",
                 "Command failed on .* with status 100: 'sudo apt-get update"]
                , "Missing packages"),
]


def give_me_a_reason(job):
    """
    If no existing reasons match the job, generate the most specific reason we can
    """

    # Note: because we match known reasons, including GenericReasons, before any of
    # the Timeout/Valgrind whatever, even if a run is a timeout or a valgrind failure,
    # it will get matched up with a backtrace or assertion if one is there, hiding
    # the valgrind/timeout aspect.

    for r in known_reasons:
        if r.match(job):
            return r

    # NB ordering matters, LockdepReason must come before AssertionReason
    for klass in [DeadReason, LockdepReason, AssertionReason, TimeoutReason, ValgrindReason]:
        if klass.could_be(job):
            return klass(job)

    return GenericReason(job)


class Scraper(object):
    def __init__(self, target_dir):
        self.target_dir = target_dir
        log.addHandler(logging.FileHandler(os.path.join(target_dir,
                                                     "scrape.log")))

    def analyze(self):
        entries = os.listdir(self.target_dir)
        jobs = []
        for entry in entries:
            job_dir = os.path.join(self.target_dir, entry)
            if os.path.isdir(job_dir):
                jobs.append(Job(job_dir, entry))

        log.info("Found {0} jobs".format(len(jobs)))

        passes = []
        reasons = defaultdict(list)

        for job in jobs:
            if job.get_success():
                passes.append(job)
                continue

            matched = False
            for reason, reason_jobs in reasons.items():
                if reason.match(job):
                    reason_jobs.append(job)
                    matched = True
                    break

            if not matched:
                reasons[give_me_a_reason(job)].append(job)

        log.info("Found {0} distinct failure reasons".format(len(reasons)))
        for reason, jobs in list(reasons.items()):
            job_spec = "{0} jobs: {1}".format(len(jobs), [j.job_id for j in jobs]) if len(jobs) < 30 else "{0} jobs".format(len(jobs))
            log.info(reason.get_description())
            detail = reason.get_detail()
            if detail:
                log.info(detail)
            log.info(job_spec)
            suites = [set(j.description.split()) for j in jobs if j.description != None]
            if len(suites) > 1:
                log.info("suites intersection: {0}".format(sorted(set.intersection(*suites))))
                log.info("suites union: {0}".format(sorted(set.union(*suites))))
            elif len(suites) == 1:
                log.info("suites: {0}".format(sorted(suites[0])))
            log.info("")

if __name__ == '__main__':
    Scraper(sys.argv[1]).analyze()
