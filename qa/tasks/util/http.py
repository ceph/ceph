from io import StringIO

def wait_for_http_endpoint(url, remote):
    """ poll the given url until it starts accepting connections

    add_daemon() doesn't wait until the daemon finishes startup, so this is used
    to avoid racing with later tasks that expect it to be up and listening
    """
    remote.run(
        args=['curl', url, '--retry-connrefused', '--retry', '8'],
        check_status=True,
        stdout=StringIO(),
        stderr=StringIO(),
        stdin=StringIO(),
        )
