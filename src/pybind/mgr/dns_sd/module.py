
"""
blah
"""
import socketserver
import logging
import threading

import dns.message
import dns.opcode
import dns.rcode
import dns.rdtypes
import dns.rrset
import dns.rdataclass
import dns.name
import dns.namedict
import dns.rdata
import dns.reversename

from mgr_module import MgrModule


HOST = "0.0.0.0"
UDP_PORT = 53
TTL = 5

NAME_DICT = dns.namedict.NameDict()

NAME_DICT[dns.name.from_text('mgr.local.')] = {
    dns.rdatatype.A: ['172.20.0.2'],
    dns.rdatatype.AAAA: ['2600:3c02:e000:58::2']
    }

NAME_DICT[dns.name.from_text('grafana.local.')] = {
    dns.rdatatype.A: ['172.20.0.3'],
    dns.rdatatype.AAAA: ['2600:3c02:e000:58::3']
}

NAME_DICT[dns.reversename.from_address('172.20.0.3')] = {
    dns.rdatatype.PTR: ['grafana.local.'],
}

NAME_DICT[dns.reversename.from_address('2600:3c02:e000:58::3')] = {
    dns.rdatatype.PTR: ['grafana.local.'],
}

NAME_DICT[dns.name.from_text('prometheus.local.')] = {
    dns.rdatatype.A: ['172.20.0.3'],
    dns.rdatatype.AAAA: ['2600:3c02:e000:58::3']
}

NAME_DICT[dns.name.from_text('alertmanager.local.')] = {
    dns.rdatatype.A: ['172.20.0.3'],
    dns.rdatatype.AAAA: ['2600:3c02:e000:58::3']
}



logger = logging.getLogger(__name__)


class DNSResolver(socketserver.BaseRequestHandler):
    def handle(self):
        data, socket = self.request
        query = dns.message.from_wire(data, question_only=True)
        logger.debug('Request received from {}: {} '.format(socket, query.to_text().replace('\n', ' \\n ')))

        response = dns.message.make_response(query)
        response.set_rcode(dns.rcode.NOERROR)

        if query.opcode() == dns.opcode.QUERY:
            for question in query.question:
                try:
                    names = NAME_DICT.get_deepest_match(question.name)[1][question.rdtype]
                    question.add(dns.rdata.from_text(question.rdclass, question.rdtype, names[0]), ttl=TTL)
                    response.answer.append(question)
                    logger.warning('Response: {} '.format(response.to_text().replace('\n', ' \\n ')))
                except KeyError:
                    # Not Found Here
                    pass #response.set_rcode(dns.rcode.NOTZONE)

        else:    
            response.set_rcode(dns.rcode.NOTIMP)
        socket.sendto(response.to_wire(), self.client_address)


class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer): pass


class DNS_SD(MgrModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logger.warning('Initializing')
        self.server = ThreadedUDPServer((HOST, UDP_PORT), DNSResolver)

    def serve(self):
        logger.warning('Starting')
        self.server_thread = threading.Thread(target=self.server.serve_forever())
        self.server_thread.start()
        logger.warning('Started')

    def shutdown(self):
        logger.warning('Stopping')
        self.server.shudown()
        self.sever_thread.join()
        logger.warning('Stopped')

