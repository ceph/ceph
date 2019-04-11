from collections import Counter, defaultdict

import urllib.parse
import pprint

from .async_client import ResponseHandler

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

    def aggr_stats(self):
        d = {'total': 0,
             'success': 0,
             'error':0,
             'denied':0,
             'unknown':0}

        for req_status, req_count in self.counter.items():
            d['total'] += req_count
            if req_status >= 200 and req_status <= 300:
                d['success'] += req_count
            elif req_status >= 400 and req_status <= 400 :
                d['error'] += req_count
            elif req_status >= 500 and req_status <= 510 :
                d['denied'] += req_count

        return d

class ReqTypeCounterHandler(ResponseHandler):
    '''
    A response handler that aggregates by req type and then by path

    '''
    def __init__(self, req_list):
        #TODO: handle per req urls
        c = defaultdict(lambda : defaultdict(Counter))
        self.counter = c

    def handle_response(self, response, *_):
        self.counter[response.method][response.url.path][response.status] += 1

    def print_stats(self):
        # TODO implement me!
        pprint.pprint(self.counter)

class PathCountHandler(ResponseHandler):

    def __init__(self):
        self.counter = defaultdict(int)

    def handle_response(self, response, *_):
        self.counter[response.url.path] += 1

    def needs_data(self):
        return True

def make_response_handler_from_str(handler_str, *args):
    if handler_str == 'req_type':
        return ReqTypeCounterHandler(*args)
    elif handler_str == 'path':
        return PathCountHandler()
    # default to counter
    return CounterHandler()

def make_response_handler(c):
    return make_response_handler_from_str(c.response_handler, c.arg_list)
