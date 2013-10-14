import os
import sys
import time
import yaml
import logging
import subprocess
from textwrap import dedent
from textwrap import fill

import teuthology
from teuthology import misc
from teuthology import suite

log = logging.getLogger(__name__)


def main(args):

    log = logging.getLogger(__name__)
    if args.verbose:
        teuthology.log.setLevel(logging.DEBUG)

    misc.read_config(args)

    handler = logging.FileHandler(
        filename=os.path.join(args.archive_dir, 'results.log'),
    )
    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d %(levelname)s:%(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
    )
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)

    try:
        results(args)
    except Exception:
        log.exception('error generating results')
        raise


def results(args):
    running_tests = [
        f for f in sorted(os.listdir(args.archive_dir))
        if not f.startswith('.')
        and os.path.isdir(os.path.join(args.archive_dir, f))
        and not os.path.exists(os.path.join(
            args.archive_dir, f, 'summary.yaml'))
    ]
    starttime = time.time()
    log.info('Waiting up to %d seconds for tests to finish...', args.timeout)
    while running_tests and args.timeout > 0:
        if os.path.exists(os.path.join(
                args.archive_dir,
                running_tests[-1], 'summary.yaml')):
            running_tests.pop()
        else:
            if time.time() - starttime > args.timeout:
                log.warn('test(s) did not finish before timeout of %d seconds',
                         args.timeout)
                break
            time.sleep(10)
    log.info('Tests finished! gathering results...')

    (subject, body) = build_email_body(args.name, args.archive_dir,
                                       args.timeout)

    try:
        if args.email:
            email_results(
                subject=subject,
                from_=args.teuthology_config['results_sending_email'],
                to=args.email,
                body=body,
            )
    finally:
        generate_coverage(args)


def generate_coverage(args):
    log.info('starting coverage generation')
    subprocess.Popen(
        args=[
            os.path.join(os.path.dirname(sys.argv[0]), 'teuthology-coverage'),
            '-v',
            '-o',
            os.path.join(args.teuthology_config[
                         'coverage_output_dir'], args.name),
            '--html-output',
            os.path.join(args.teuthology_config[
                         'coverage_html_dir'], args.name),
            '--cov-tools-dir',
            args.teuthology_config['coverage_tools_dir'],
            args.archive_dir,
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


def build_email_body(name, archive_dir, timeout):
    failed = {}
    hung = {}
    passed = {}

    for job in suite.get_jobs(archive_dir):
        job_dir = os.path.join(archive_dir, job)
        summary_file = os.path.join(job_dir, 'summary.yaml')

        # Unfinished jobs will have no summary.yaml
        if not os.path.exists(summary_file):
            info_file = os.path.join(job_dir, 'info.yaml')

            desc = ''
            if os.path.exists(info_file):
                with file(info_file) as f:
                    info = yaml.safe_load(f)
                    desc = info['description']

            hung[job] = email_templates['hung_templ'].format(
                job_id=job,
                desc=desc,
            )
            continue

        with file(summary_file) as f:
            summary = yaml.safe_load(f)

        if summary['success']:
            passed[job] = email_templates['pass_templ'].format(
                job_id=job,
                desc=summary.get('description'),
                time=int(summary.get('duration', 0)),
            )
        else:
            log = misc.get_http_log_path(archive_dir, job)
            if log:
                log_line = email_templates['fail_log_templ'].format(log=log)
            else:
                log_line = ''
            sentry_events = summary.get('sentry_events')
            if sentry_events:
                sentry_line = email_templates['fail_sentry_templ'].format(
                    sentries='\n        '.join(sentry_events))
            else:
                sentry_line = ''

            # 'fill' is from the textwrap module and it collapses a given
            # string into multiple lines of a maximum width as specified. We
            # want 75 characters here so that when we indent by 4 on the next
            # line, we have 79-character exception paragraphs.
            reason = fill(summary.get('failure_reason'), 75)
            reason = '\n'.join(('    ') + line for line in reason.splitlines())

            failed[job] = email_templates['fail_templ'].format(
                job_id=job,
                desc=summary.get('description'),
                time=int(summary.get('duration', 0)),
                reason=reason,
                log_line=log_line,
                sentry_line=sentry_line,
            )

    maybe_comma = lambda s: ', ' if s else ' '

    subject = ''
    fail_sect = ''
    hung_sect = ''
    pass_sect = ''
    if failed:
        subject += '{num_failed} failed{sep}'.format(
            num_failed=len(failed),
            sep=maybe_comma(hung or passed)
        )
        fail_sect = email_templates['sect_templ'].format(
            title='Failed',
            jobs=''.join(failed.values())
        )
    if hung:
        subject += '{num_hung} hung{sep}'.format(
            num_hung=len(hung),
            sep=maybe_comma(passed),
        )
        hung_sect = email_templates['sect_templ'].format(
            title='Hung',
            jobs=''.join(hung.values()),
        )
    if passed:
        subject += '%s passed ' % len(passed)
        pass_sect = email_templates['sect_templ'].format(
            title='Passed',
            jobs=''.join(passed.values()),
        )

    body = email_templates['body_templ'].format(
        name=name,
        log_root=misc.get_http_log_path(archive_dir),
        fail_count=len(failed),
        hung_count=len(hung),
        pass_count=len(passed),
        fail_sect=fail_sect,
        hung_sect=hung_sect,
        pass_sect=pass_sect,
    )

    subject += 'in {suite}'.format(suite=name)
    return (subject.strip(), body.strip())

email_templates = {
    'body_templ': dedent("""\
        Test Run: {name}
        =================================================================
        logs:   {log_root}
        failed: {fail_count}
        hung:   {hung_count}
        passed: {pass_count}

        {fail_sect}{hung_sect}{pass_sect}
        """),
    'sect_templ': dedent("""\
        {title}
        =================================================================
        {jobs}
        """),
    'fail_templ': dedent("""\
        [{job_id}]  {desc}
        -----------------------------------------------------------------
        time:   {time}s{log_line}{sentry_line}

        {reason}

        """),
    'fail_log_templ': "\nlog:    {log}",
    'fail_sentry_templ': "\nsentry: {sentries}",
    'hung_templ': dedent("""\
        [{job_id}] {desc}
        """),
    'pass_templ': dedent("""\
        [{job_id}] {desc}
        time:    {time}s

        """),
}
