import os
import sys
import time
import logging
import subprocess
from textwrap import dedent
from textwrap import fill

import teuthology
from teuthology.config import config
from teuthology import misc
from .report import ResultsReporter

log = logging.getLogger(__name__)

UNFINISHED_STATUSES = ('queued', 'running', 'waiting')


def main(args):

    log = logging.getLogger(__name__)
    if args['--verbose']:
        teuthology.log.setLevel(logging.DEBUG)

    if not args['--dry-run']:
        log_path = os.path.join(args['--archive-dir'], 'results.log')
        teuthology.setup_log_file(log_path)

    try:
        results(args['--archive-dir'], args['--name'], args['--email'],
                int(args['--timeout']), args['--dry-run'])
    except Exception:
        log.exception('error generating results')
        raise


def results(archive_dir, name, email, timeout, dry_run):
    starttime = time.time()

    if timeout:
        log.info('Waiting up to %d seconds for tests to finish...', timeout)

    reporter = ResultsReporter()
    while timeout > 0:
        if time.time() - starttime > timeout:
            log.warn('test(s) did not finish before timeout of %d seconds',
                     timeout)
            break
        jobs = reporter.get_jobs(name, fields=['job_id', 'status'])
        unfinished_jobs = [job for job in jobs if job['status'] in
                           UNFINISHED_STATUSES]
        if not unfinished_jobs:
            log.info('Tests finished! gathering results...')
            break
        time.sleep(10)

    (subject, body) = build_email_body(name)

    try:
        if email and dry_run:
            print "From: %s" % (config.results_sending_email or 'teuthology')
            print "To: %s" % email
            print "Subject: %s" % subject
            print body
        elif email:
            email_results(
                subject=subject,
                from_=(config.results_sending_email or 'teuthology'),
                to=email,
                body=body,
            )
    finally:
        generate_coverage(archive_dir, name)


def generate_coverage(archive_dir, name):
    coverage_config_keys = ('coverage_output_dir', 'coverage_html_dir',
                            'coverage_tools_dir')
    for key in coverage_config_keys:
        if key not in config.to_dict():
            log.warn(
                "'%s' not in teuthology config; skipping coverage report",
                key)
            return
    log.info('starting coverage generation')
    subprocess.Popen(
        args=[
            os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-coverage'),
            '-v',
            '-o',
            os.path.join(config.coverage_output_dir, name),
            '--html-output',
            os.path.join(config.coverage_html_dir, name),
            '--cov-tools-dir',
            config.coverage_tools_dir,
            archive_dir,
        ],
    )


def email_results(subject, from_, to, body):
    log.info('Sending results to {to}: {body}'.format(to=to, body=body))
    import smtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_
    msg['To'] = to
    log.debug('sending email %s', msg.as_string())
    smtp = smtplib.SMTP('localhost')
    smtp.sendmail(msg['From'], [msg['To']], msg.as_string())
    smtp.quit()


def build_email_body(name, _reporter=None):
    failed = {}
    dead = {}
    running = {}
    waiting = {}
    queued = {}
    passed = {}
    reporter = _reporter or ResultsReporter()
    fields = ('job_id', 'status', 'description', 'duration', 'failure_reason',
              'sentry_event', 'log_href')
    jobs = reporter.get_jobs(name, fields=fields)
    jobs.sort(key=lambda job: job['job_id'])

    for job in jobs:
        job_id = job['job_id']
        status = job['status']
        description = job['description']
        duration = int(job['duration'] or 0)

        # Every job gets a link to e.g. pulpito's pages
        info_url = misc.get_results_url(name, job_id)
        if info_url:
            info_line = email_templates['info_url_templ'].format(info=info_url)
        else:
            info_line = ''

        if status in UNFINISHED_STATUSES:
            format_args = dict(
                job_id=job_id,
                desc=description,
                time=duration,
                info_line=info_line,
            )
            if status == 'running':
                running[job_id] = email_templates['running_templ'].format(
                    **format_args)
            elif status == 'waiting':
                waiting[job_id] = email_templates['running_templ'].format(
                    **format_args)
            elif status == 'queued':
                queued[job_id] = email_templates['running_templ'].format(
                    **format_args)
            continue

        if status == 'pass':
            passed[job_id] = email_templates['pass_templ'].format(
                job_id=job_id,
                desc=description,
                time=duration,
                info_line=info_line,
            )
        else:
            log_dir_url = job['log_href'].rstrip('teuthology.yaml')
            if log_dir_url:
                log_line = email_templates['fail_log_templ'].format(
                    log=log_dir_url)
            else:
                log_line = ''
            sentry_event = job.get('sentry_event')
            if sentry_event:
                sentry_line = email_templates['fail_sentry_templ'].format(
                    sentry_event=sentry_event)
            else:
                sentry_line = ''

            if job['failure_reason']:
                # 'fill' is from the textwrap module and it collapses a given
                # string into multiple lines of a maximum width as specified.
                # We want 75 characters here so that when we indent by 4 on the
                # next line, we have 79-character exception paragraphs.
                reason = fill(job['failure_reason'] or '', 75)
                reason = \
                    '\n'.join(('    ') + line for line in reason.splitlines())
                reason_lines = email_templates['fail_reason_templ'].format(
                    reason=reason)
            else:
                reason_lines = ''

            format_args = dict(
                job_id=job_id,
                desc=description,
                time=duration,
                info_line=info_line,
                log_line=log_line,
                sentry_line=sentry_line,
                reason_lines=reason_lines,
            )
            if status == 'fail':
                failed[job_id] = email_templates['fail_templ'].format(
                    **format_args)
            elif status == 'dead':
                dead[job_id] = email_templates['fail_templ'].format(
                    **format_args)

    maybe_comma = lambda s: ', ' if s else ' '

    subject = ''
    fail_sect = ''
    dead_sect = ''
    running_sect = ''
    waiting_sect = ''
    queued_sect = ''
    pass_sect = ''
    if failed:
        subject += '{num_failed} failed{sep}'.format(
            num_failed=len(failed),
            sep=maybe_comma(dead or running or waiting or queued or passed)
        )
        fail_sect = email_templates['sect_templ'].format(
            title='Failed',
            jobs=''.join(failed.values())
        )
    if dead:
        subject += '{num_dead} dead{sep}'.format(
            num_dead=len(dead),
            sep=maybe_comma(running or waiting or queued or passed)
        )
        dead_sect = email_templates['sect_templ'].format(
            title='Dead',
            jobs=''.join(dead.values()),
        )
    if running:
        subject += '{num_running} running{sep}'.format(
            num_running=len(running),
            sep=maybe_comma(waiting or queued or passed)
        )
        running_sect = email_templates['sect_templ'].format(
            title='Running',
            jobs=''.join(running.values()),
        )
    if waiting:
        subject += '{num_waiting} waiting{sep}'.format(
            num_waiting=len(waiting),
            sep=maybe_comma(running or waiting or queued or passed)
        )
        waiting_sect = email_templates['sect_templ'].format(
            title='Waiting',
            jobs=''.join(waiting.values()),
        )
    if queued:
        subject += '{num_queued} queued{sep}'.format(
            num_queued=len(queued),
            sep=maybe_comma(running or waiting or queued or passed)
        )
        queued_sect = email_templates['sect_templ'].format(
            title='Queued',
            jobs=''.join(queued.values()),
        )
    if passed:
        subject += '%s passed ' % len(passed)
        pass_sect = email_templates['sect_templ'].format(
            title='Passed',
            jobs=''.join(passed.values()),
        )

    if config.archive_server:
        log_root = os.path.join(config.archive_server, name, '')
    else:
        log_root = None

    body = email_templates['body_templ'].format(
        name=name,
        info_root=misc.get_results_url(name),
        log_root=log_root,
        fail_count=len(failed),
        dead_count=len(dead),
        running_count=len(running),
        waiting_count=len(waiting),
        queued_count=len(queued),
        pass_count=len(passed),
        fail_sect=fail_sect,
        dead_sect=dead_sect,
        running_sect=running_sect,
        waiting_sect=waiting_sect,
        queued_sect=queued_sect,
        pass_sect=pass_sect,
    )

    subject += 'in {suite}'.format(suite=name)
    return (subject.strip(), body.strip())

email_templates = {
    'body_templ': dedent("""\
        Test Run: {name}
        =================================================================
        info:    {info_root}
        logs:    {log_root}
        failed:  {fail_count}
        dead:    {dead_count}
        running: {running_count}
        waiting: {waiting_count}
        queued:  {queued_count}
        passed:  {pass_count}

        {fail_sect}{dead_sect}{running_sect}{waiting_sect}{queued_sect}{pass_sect}
        """),
    'sect_templ': dedent("""\
        {title}
        =================================================================
        {jobs}
        """),
    'fail_templ': dedent("""\
        [{job_id}]  {desc}
        -----------------------------------------------------------------
        time:   {time}s{info_line}{log_line}{sentry_line}{reason_lines}

        """),
    'info_url_templ': "\ninfo:   {info}",
    'fail_log_templ': "\nlog:    {log}",
    'fail_sentry_templ': "\nsentry: {sentry_event}",
    'fail_reason_templ': "\n\n{reason}\n",
    'running_templ': dedent("""\
        [{job_id}] {desc}{info_line}
        """),
    'pass_templ': dedent("""\
        [{job_id}] {desc}
        time:   {time}s{info_line}

        """),
}
