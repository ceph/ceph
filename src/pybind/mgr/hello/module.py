
"""
A hello world module

See doc/mgr/hello.rst for more info.
"""

from mgr_module import MgrModule, HandleCommandResult
from threading import Event


class Hello(MgrModule):
    COMMANDS = [
        {
            "cmd": "hello "
                   "name=person_name,type=CephString,req=false",
            "desc": "Prints hello world to mgr.x.log",
            "perm": "r"
        },
    ]

    def __init__(self, *args, **kwargs):
        super(Hello, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown
        self.run = True
        self.event = Event()

    def handle_command(self, inbuf, cmd):
        self.log.info("hello_world_info")
        self.log.debug("hello_world_debug")
        self.log.error("hello_world_error")

        status_code = 0
        output_buffer = "Output buffer is for data results"
        output_string = "Output string is for informative text"
        message = "hello world!"

        if 'person_name' in cmd:
            message = "hello, " + cmd['person_name'] + "!"

        return HandleCommandResult(retval=status_code, stdout=output_buffer,
                                   stderr=message + "\n" + output_string)


    def serve(self):
        """
        This method is called by the mgr when the module starts and can be
        used for any background activity.
        """
        self.log.info("Starting")
        while self.run:
            sleep_interval = 5
            self.log.debug('Sleeping for %d seconds', sleep_interval)
            ret = self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self):
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit.
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
