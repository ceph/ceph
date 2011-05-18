import gevent
import pipes


def write_to_log(logger, f):
    # i can't seem to get fudge to fake an iterable, so using this old
    # api for now
    for line in f.xreadlines():
        line = line.rstrip()
        logger.info(line)


def run_and_log(client, logger, args):
    cmd = ' '.join(pipes.quote(a) for a in args)
    (in_, out, err) = client.exec_command(cmd)
    in_.close()
    log_out = logger.getChild('out')
    log_err = logger.getChild('err')
    g_err = gevent.spawn(write_to_log, log_err, err)
    write_to_log(log_out, out)
    g_err.get()
    # -1 on connection loss *and* signals; map to more pythonic None
    status = out.channel.recv_exit_status()
    if status == -1:
        status = None
    return status
