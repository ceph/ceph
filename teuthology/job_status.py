def get_status(summary):
    """
    :param summary: The job summary dict. Normally ctx.summary
    :returns:       A status string like 'pass', 'fail', or 'dead'
    """
    status = summary.get('status')
    if status is not None:
        return status

    success = summary.get('success')
    if success is True:
        status = 'pass'
    elif success is False:
        status = 'fail'
    else:
        status = None
    return status


def set_status(summary, status):
    """
    Sets summary['status'] to status, and summary['success'] to True if status
    is 'pass'. If status is not 'pass', then 'success' is False.

    If status is None, do nothing.

    :param summary: The job summary dict. Normally ctx.summary
    :param status: The job status, e.g. 'pass', 'fail', 'dead'
    """
    if status is None:
        return

    summary['status'] = status
    if status == 'pass':
        summary['success'] = True
    else:
        summary['success'] = False

