
"""
A hello world module

See doc/mgr/hello.rst for more info.
"""

from mgr_module import CLIReadCommand, HandleCommandResult, MgrModule, Option
from threading import Event
from typing import cast, Any, Optional, TYPE_CHECKING
import errno


class Hello(MgrModule):
    # These are module options we understand.  These can be set with
    #
    #   ceph config set global mgr/hello/<name> <value>
    #
    # e.g.,
    #
    #   ceph config set global mgr/hello/place Earth
    #
    MODULE_OPTIONS = [
        Option(name='place',
               default='world',
               desc='a place in the world',
               runtime=True),   # can be updated at runtime (no mgr restart)
        Option(name='emphatic',
               type='bool',
               desc='whether to say it loudly',
               default=True,
               runtime=True),
        Option(name='foo',
               type='str',
               enum_allowed=['a', 'b', 'c'],
               default='a',
               runtime=True)
    ]

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS = [
        'mgr_tick_period',
    ]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = Event()

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

        # for mypy which does not run the code
        if TYPE_CHECKING:
            self.mgr_tick_period = 0

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # Do the same for the native options.
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))

    # there are CLI commands we implement
    @CLIReadCommand('hello')
    def hello(self, person_name: Optional[str] = None) -> HandleCommandResult:
        """
        Say hello
        """
        if person_name is None:
            who = cast(str, self.get_module_option('place'))
        else:
            who = person_name
        fin = '!' if self.get_module_option('emphatic') else ''
        return HandleCommandResult(stdout=f'Hello, {who}{fin}')

    @CLIReadCommand('count')
    def count(self, num: int) -> HandleCommandResult:
        """
        Do some counting
        """
        ret = 0
        out = ''
        err = ''
        if num < 1:
            err = 'That\'s too small a number'
            ret = -errno.EINVAL
        elif num > 10:
            err = 'That\'s too big a number'
            ret = -errno.EINVAL
        else:
            out = 'Hello, I am the count!\n'
            out += ', '.join([str(x) for x in range(1, num + 1)]) + '!'
        return HandleCommandResult(retval=ret,
                                   stdout=out,
                                   stderr=err)

    def serve(self) -> None:
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        while self.run:
            # Do some useful background work here.

            # Use mgr_tick_period (default: 2) here just to illustrate
            # consuming native ceph options.  Any real background work
            # would presumably have some more appropriate frequency.
            sleep_interval = self.mgr_tick_period
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
