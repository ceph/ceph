
"""
An email module

See doc/mgr/hello.rst for more info.
"""

from mgr_module import MgrModule
import smtplib

class Email(MgrModule):
    COMMANDS = [
        {
            "cmd": "Email "
                   "name=port,type=CephInt,req=false "
                   "name=host,type=CephString,req=false "
                   "name=sender_id,type=CephString,req=true "
                   "name=target_id,type=CephString,req=true "
                   "name=msg,type=CephString,req=true",
            "desc": "Sends email",
            "perm": "rw"
        },
    ]

    def handle_command(self, inbuf, cmd):
        self.log.info("Email_info")
        self.log.debug("Email_debug")
        self.log.error("Email_error")

        status_code = 0
        output_buffer = "Output buffer is for data results"
        output_string = "Output string is for informative text"

        sender = cmd['sender_id']
        receivers = cmd['target_id']

        email_message = cmd['msg']

        if 'host' in cmd:
            host = cmd['host']
        else:
            host = 'localhost'

        port = cmd.get('port', 465)

        try:
            smtpObj = smtplib.SMTP(host, port)
            smtpObj.sendmail(sender, receivers, email_message)         
            message = "Successfully sent email"
        except SMTPException:
            message = "Error: unable to send email"

        return status_code, output_buffer, message + "\n" + output_string
