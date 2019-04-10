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

class PathCountHandler(ResponseHandler):

    def __init__(self):
        #TODO: req dict -> type

        self.counter = defaultdict(int)


    def handle_response(self, response, *_):
        self.counter[response.url.path] += 1

    def needs_data(self):
        return True

def make_response_handler_from_str(handler_str):
    if handler_str == 'simple':
        return CounterHandler()
    elif handler_str == 'path':
        return PathCountHandler()
    # default to reqtype counter
    return ReqTypeCounterHandler(ctx.arg_list)

def make_response_handler(ctx):
    return make_response_handler_from_str(ctx.response_handler)
