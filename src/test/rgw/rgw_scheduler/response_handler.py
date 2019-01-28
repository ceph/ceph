import abc
from collections import Counter, defaultdict

import urllib.parse
import pprint

class ResponseHandler(abc.ABC):

    @abc.abstractmethod
    def handle_response(self, response, response_data):
        pass

    def needs_data(self):
        return False

class CounterHandler(ResponseHandler):
    '''
    A very basic request status counter, aggregating the total requests per
    status code:
    Final output is just a simple printout of status: aggr count
    '''

    def __init__(self):
        self.counter=Counter()

    def handle_response(self, response, *_):
        self.counter[response.status] += 1

    def print_stats(self):
        for k,v in self.counter.items():
            print(k,': ',v)

class ReqTypeCounterHandler(ResponseHandler):
    '''
    For the lack of a better name!
    '''
    def __init__(self, req_list):
        #TODO: req dict -> type

        c = defaultdict(dict)
        for req in req_list:
            path = urllib.parse.urlparse(req["req_url"]).path
            c[req["req_type"]][path] = Counter()

        self.counter = c

    def handle_response(self, response, *_):
        self.counter[response.method][response.url.path][response.status] += 1

    def print_stats(self):
        # TODO implement me!
        pprint.pprint(self.counter)


def make_response_handler(ctx):
    if ctx.response_handler == 'simple':
        return CounterHandler()
    # default to reqtype counter
    return ReqTypeCounterHandler(ctx.arg_list)
