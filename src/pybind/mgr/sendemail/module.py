
"""
A hello world module

See doc/mgr/hello.rst for more info.
"""
from mgr_module import MgrModule
import smtplib
from email.mime.text import MIMEText



class SendEmail(MgrModule):

    COMMANDS = [
        {
            "cmd": "sendemail ",       
            "desc": "send email",
            "perm": "rw"
        },
    ]

    def handle_command(self, inbuf, cmd):

        self.log.error("hello_world_error")
        smtp_ssl_host = 'smtp.gmail.com'  # smtp.mail.yahoo.com
	smtp_ssl_port = 465
	sender = 'add Your Email here'
	password = 'add Your Password here'
	target = 'add the target email here'

	msg = MIMEText('Hi, how are you today?')   
	msg['Subject'] = 'Hello'
	msg['From'] = sender
	msg['To'] = target

	server = smtplib.SMTP_SSL(smtp_ssl_host, smtp_ssl_port)
	server.login(sender, password)
	server.sendmail(sender, target, msg.as_string())
	server.quit()

	status_code = 0
        output_buffer = "Output buffer is for data results"
        output_string = "Output string is for informative text"
        message = "Email Sent!"
        

	return status_code, output_buffer, message + "\n" + output_string


