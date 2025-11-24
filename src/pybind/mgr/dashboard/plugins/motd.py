# -*- coding: utf-8 -*-

import hashlib
import json
from enum import Enum
from typing import Dict, NamedTuple, Optional

from ceph.utils import datetime_now, datetime_to_str, parse_timedelta, str_to_datetime

from ..cli import DBCLICommand
from . import PLUGIN_MANAGER as PM
from .plugin import SimplePlugin as SP


class MotdSeverity(Enum):
    INFO = 'info'
    WARNING = 'warning'
    DANGER = 'danger'


class MotdData(NamedTuple):
    message: str
    md5: str  # The MD5 of the message.
    severity: MotdSeverity
    expires: str  # The expiration date in ISO 8601. Does not expire if empty.


@PM.add_plugin  # pylint: disable=too-many-ancestors
class Motd(SP):
    NAME = 'motd'

    OPTIONS = [
        SP.Option(
            name=NAME,
            default='',
            type='str',
            desc='The message of the day'
        )
    ]

    @PM.add_hook
    def register_commands(self):
        @DBCLICommand("dashboard {name} get".format(name=self.NAME))
        def _get(_):
            stdout: str
            value: str = self.get_option(self.NAME)
            if not value:
                stdout = 'No message of the day has been set.'
            else:
                data = json.loads(value)
                if not data['expires']:
                    data['expires'] = "Never"
                stdout = 'Message="{message}", severity="{severity}", ' \
                         'expires="{expires}"'.format(**data)
            return 0, stdout, ''

        @DBCLICommand("dashboard {name} set".format(name=self.NAME))
        def _set(_, severity: MotdSeverity, expires: str, message: str):
            if expires != '0':
                delta = parse_timedelta(expires)
                if not delta:
                    return 1, '', 'Invalid expires format, use "2h", "10d" or "30s"'
                expires = datetime_to_str(datetime_now() + delta)
            else:
                expires = ''
            value: str = json.dumps({
                'message': message,
                'md5': hashlib.md5(message.encode()).hexdigest(),
                'severity': severity.value,
                'expires': expires
            })
            self.set_option(self.NAME, value)
            return 0, 'Message of the day has been set.', ''

        @DBCLICommand("dashboard {name} clear".format(name=self.NAME))
        def _clear(_):
            self.set_option(self.NAME, '')
            return 0, 'Message of the day has been cleared.', ''

    @PM.add_hook
    def get_controllers(self):
        from ..controllers import RESTController, UIRouter

        @UIRouter('/motd')
        class MessageOfTheDay(RESTController):
            def list(_) -> Optional[Dict]:  # pylint: disable=no-self-argument
                value: str = self.get_option(self.NAME)
                if not value:
                    return None
                data: MotdData = MotdData(**json.loads(value))
                # Check if the MOTD has been expired.
                if data.expires:
                    expires = str_to_datetime(data.expires)
                    if expires < datetime_now():
                        return None
                return data._asdict()

        return [MessageOfTheDay]
