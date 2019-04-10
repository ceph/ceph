import abc
import asyncio
import logging
from aiohttp import ClientSession, TCPConnector, ClientTimeout

from botocore.auth import S3SigV4Auth
from botocore.credentials import Credentials
from botocore.awsrequest import AWSRequest

def get_s3_auth_headers(creds, url, method, data=None):
    # TODO: blocking function call
    # When there is data this function might recalc hashes
    req = AWSRequest(method = method, url = url, data=data)
    sig = S3SigV4Auth(Credentials(**creds),
                      's3','us-east-1')
    sig.headers_to_sign(req)
    sig.add_auth(req)
    return dict(req.headers.items())

class ResponseHandler(abc.ABC):
    """
    An abstract class that specifies the requirements of a response handler
    for AsyncClient
    """
    @abc.abstractmethod
    def handle_response(self, response, response_data):
        pass

    def needs_data(self):
        return False

class AsyncClient():
    """A simple async client that can make a specified count of authenticated
    requests.

    Requests are a simple aiohttp request under the hood, where authentication
    headers are passed on.

    Responses are currently not handled directly by the
    client and need a response handler class which must implement a
    handle_response function that decides what to do with the response data and
    headers. The handler must also decide whether it needs data or ignores it.
    Please refer to the abstract class ResponseHandler for the requirements.

    """

    def __init__(self, response_handler,
                 auth_type=None, auth_creds=None, limit=None):
        self.handler = response_handler
        self.auth_type = auth_type
        self.auth_creds = auth_creds
        self.logger = logging.getLogger('async-client')

    def get_auth_headers(self, auth_type, *args):
        if auth_type is None:
            return dict()
        elif auth_type == 's3':
            return get_s3_auth_headers(*args)
        else:
            raise NotImplementedError("the selected auth_type isn't implemented")


    def get_final_headers(self, req_type, req_url, headers, req_data=None):
        auth_headers = self.get_auth_headers(self.auth_type, self.auth_creds,
                                             req_url, req_type, req_data)
        headers.update(auth_headers)
        return headers

    async def make_request(self, req_type, req_url, session, req_count=1, headers={}, **req_params):
        # TODO: this doesn't handle the default ~300s expiry window of auth
        # requests, which will manifest itself in large request counts in the
        # range of 5000 where the window will be exceeded, we're not
        # regenerating the auth headers for every request For that it would be
        # best if we hand over auth generation to something that keeps track of
        # time and regenerates them
        headers = self.get_final_headers(req_type, req_url, headers, req_params.get('data',None))
        resp_data = None
        async with session.request(req_type, req_url, headers=headers, **req_params) as resp:
            # this is pure evil for GET requests
            if self.handler.needs_data():
                self.logger.debug('reading data')
                resp_data = await resp.read()

        self.logger.debug('Finished req %s %s: %d' % (req_type, req_url, req_count))
        self.handler.handle_response(resp, resp_data)

    async def run(self, req_type, req_url, req_count=100, per_req_path=False, **req_params):
        """
        run a req_count number of requests with the specified method and url.
        per_req_path allows for appending a count number at the end of url
        which is useful for eg putting objects in a seq. Note that every
        request is fired off as a coroutine so ordering is not guaranteed
        """
        reqs = []
        timeout = ClientTimeout(total=600)
        # TODO: figure out why we get 104s at connection counts > 4000
        async with ClientSession(connector=TCPConnector(keepalive_timeout=3000),timeout=timeout) as s:
            for i in range(int(req_count)):
                url = req_url
                if per_req_path:
                    url += str(i)
                self.logger.debug('making request: %d' % i)
                reqs.append(asyncio.ensure_future(
                    self.make_request(req_type, url, s, req_count=i, **req_params)
                ))

            responses = await asyncio.gather(*reqs)
