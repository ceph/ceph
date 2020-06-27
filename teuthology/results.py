import os
import time
import logging
from collections import OrderedDict
from textwrap import dedent
from textwrap import fill

import teuthology
from teuthology.config import config
from teuthology import misc
from teuthology.report import ResultsReporter
from teuthology.scrape import Scraper

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
        if args['--seed']:
            note_rerun_params(args['--subset'], args['--seed'])
        else:
            results(args['--archive-dir'], args['--name'], args['--email'],
                    int(args['--timeout']), args['--dry-run'])
    except Exception:
        log.exception('error generating memo/results')
        raise


def note_rerun_params(subset, seed):
    if subset:
        log.info('subset: %r', subset)
    if seed:
        log.info('seed: %r', seed)


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
        time.sleep(60)

    (subject, body) = build_email_body(name)

    Scraper(archive_dir).analyze()
    if email and dry_run:
        print("From: %s" % (config.results_sending_email or 'teuthology'))
        print("To: %s" % email)
        print("Subject: %s" % subject)
        print(body)
    elif email:
        email_results(
            subject=subject,
            from_=(config.results_sending_email or 'teuthology'),
            to=email,
            body=body,
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
    stanzas = OrderedDict([
        ('fail', dict()),
        ('dead', dict()),
        ('running', dict()),
        ('waiting', dict()),
        ('queued', dict()),
        ('pass', dict()),
    ])
    reporter = _reporter or ResultsReporter()
    fields = ('job_id', 'status', 'description', 'duration', 'failure_reason',
              'sentry_event', 'log_href')
    jobs = reporter.get_jobs(name, fields=fields)
    jobs.sort(key=lambda job: job['job_id'])

    for job in jobs:
        job_stanza = format_job(name, job)
        stanzas[job['status']][job['job_id']] = job_stanza

    sections = OrderedDict.fromkeys(stanzas.keys(), '')
    subject_fragments = []
    for status in sections.keys():
        stanza = stanzas[status]
        if stanza:
            subject_fragments.append('%s %s' % (len(stanza), status))
            sections[status] = email_templates['sect_templ'].format(
                title=status.title(),
                jobs=''.join(stanza.values()),
            )
    subject = ', '.join(subject_fragments) + ' '

    if config.archive_server:
        log_root = os.path.join(config.archive_server, name, '')
    else:
        log_root = None

    body = email_templates['body_templ'].format(
        name=name,
        info_root=misc.get_results_url(name),
        log_root=log_root,
        fail_count=len(stanzas['fail']),
        dead_count=len(stanzas['dead']),
        running_count=len(stanzas['running']),
        waiting_count=len(stanzas['waiting']),
        queued_count=len(stanzas['queued']),
        pass_count=len(stanzas['pass']),
        fail_sect=sections['fail'],
        dead_sect=sections['dead'],
        running_sect=sections['running'],
        waiting_sect=sections['waiting'],
        queued_sect=sections['queued'],
        pass_sect=sections['pass'],
    )

    subject += 'in {suite}'.format(suite=name)
    return (subject.strip(), body.strip())


def format_job(run_name, job):
    job_id = job['job_id']
    status = job['status']
    description = job['description']
    duration = seconds_to_hms(int(job['duration'] or 0))

    # Every job gets a link to e.g. pulpito's pages
    info_url = misc.get_results_url(run_name, job_id)
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
        return email_templates['running_templ'].format(**format_args)

    if status == 'pass':
        return email_templates['pass_templ'].format(
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
                reason=reason).rstrip()
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
        return email_templates['fail_templ'].format(**format_args)


def seconds_to_hms(seconds):
    (minutes, seconds) = divmod(seconds, 60)
    (hours, minutes) = divmod(minutes, 60)
    return "%02d:%02d:%02d" % (hours, minutes, seconds)


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
        time:   {time}{info_line}{log_line}{sentry_line}{reason_lines}

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
        time:   {time}{info_line}

        """),
}
