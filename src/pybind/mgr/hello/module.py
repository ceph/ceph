
"""
A hello world module

See doc/mgr/hello.rst for more info.
"""

from mgr_module import MgrModule


class Hello(MgrModule):
    COMMANDS = [
        {
            "cmd": "hello "
                   "name=person_name,type=CephString,req=false",
            "desc": "Prints hello world to mgr.x.log",
            "perm": "r"
        },
    ]

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

        return status_code, output_buffer, message + "\n" + output_string
