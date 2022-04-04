from teuthology.orchestra import run
from teuthology.exceptions import (CommandCrashedError, CommandFailedError,
                                   ConnectionLostError)

def _checking_raise_for_status(
    client, args,
    stdin=None, stdout=None, stderr=None,
    logger=None,
    check_status=True,
    wait=True,
    name=None,
    label=None,
    quiet=False,
    timeout=None,
    cwd=None,
    omit_sudo=False
):
   
    try:
        transport = client.get_transport()
        if transport:
            (host, port) = transport.getpeername()[0:2]
        else:
            raise ConnectionLostError(command=quote(args), node=name)
    except socket.error:
        raise ConnectionLostError(command=quote(args), node=name)

    if name is None:
        name = host

    if timeout:
        log.info("Running command with timeout %d", timeout)
    r = RemoteProcess(client, args, check_status=check_status, hostname=name,
                      label=label, timeout=timeout, wait=wait, logger=logger,
                      cwd=cwd)
    
    # For testing taking return code 
    # to be any value other than zero
    r.returncode = 1

    try:
        r._raise_for_status()
    except CommandFailedError:
        print("Error generated")
    else:
        print("Error not generated")
