"""
Aver wrapper task
"""
import contextlib
import logging
from subprocess import check_call, Popen, PIPE

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Execute an aver assertion

    Parameters:

        input: file containing data referred to by the assertions. File name is
               relative to the job's archive path
        validations: list of validations in the Aver language

    Example:
    - aver:
        input: bench_output.csv
        validations:
        - expect performance(alg='ceph') > performance(alg='raw')
        - for size > 3 expect avg_throughput > 2000
    """
    log.info('Beginning aver...')
    assert isinstance(config, dict), 'expecting dictionary for configuration'

    if 'input' not in config:
        raise Exception("Expecting 'input' option")
    if len(config.get('validations', [])) < 1:
        raise Exception("Expecting at least one entry in 'validations'")

    url = ('https://github.com/ivotron/aver/releases/download/'
           'v0.3.0/aver-linux-amd64.tar.bz2')

    aver_path = ctx.archive + '/aver'

    # download binary
    check_call(['wget', '-O', aver_path + '.tbz', url])
    check_call(['tar', 'xfj', aver_path + '.tbz', '-C', ctx.archive])

    # print version
    process = Popen([aver_path, '-v'], stdout=PIPE)
    log.info(process.communicate()[0])

    # validate
    for validation in config['validations']:
        cmd = (aver_path + ' -s -i ' + (ctx.archive + '/' + config['input']) +
               ' "' + validation + '"')
        log.info("executing: " + cmd)
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        (stdout, stderr) = process.communicate()
        if stderr:
            log.info('aver stderr: ' + stderr)
        log.info('aver result: ' + stdout)
        if stdout.strip(' \t\n\r') != 'true':
            raise Exception('Failed validation: ' + validation)

    try:
        yield
    finally:
        log.info('Removing aver binary...')
        check_call(['rm', aver_path, aver_path + '.tbz'])
