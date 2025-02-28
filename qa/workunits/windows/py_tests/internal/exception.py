# Copyright (C) 2023 Cloudbase Solutions
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation (see LICENSE).

class CephTestException(Exception):
    msg_fmt = "An exception has been encountered."

    def __init__(self, message: str = '', **kwargs):
        self.kwargs = kwargs
        if not message:
            message = self.msg_fmt % kwargs
        self.message = message
        super(CephTestException, self).__init__(message)


class CommandFailed(CephTestException):
    msg_fmt = (
        "Command failed: %(command)s. "
        "Return code: %(returncode)s. "
        "Stdout: %(stdout)s. Stderr: %(stderr)s.")


class CephTestTimeout(CephTestException):
    msg_fmt = "Operation timeout."
