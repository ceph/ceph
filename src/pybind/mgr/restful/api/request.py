from pecan import expose, request, response
from pecan.rest import RestController

from restful import context
from restful.decorators import auth, lock, paginate


class RequestId(RestController):
    def __init__(self, request_id):
        self.request_id = request_id


    @expose(template='json')
    @auth
    def get(self, **kwargs):
        """
        Show the information for the request id
        """
        request = [x for x in context.instance.requests
                   if x.id == self.request_id]
        if len(request) != 1:
            response.status = 500
            return {'message': 'Unknown request id "{}"'.format(self.request_id)}
        return request[0]


    @expose(template='json')
    @auth
    @lock
    def delete(self, **kwargs):
        """
        Remove the request id from the database
        """
        for index in range(len(context.instance.requests)):
            if context.instance.requests[index].id == self.request_id:
                return context.instance.requests.pop(index)

        # Failed to find the job to cancel
        response.status = 500
        return {'message': 'No such request id'}



class Request(RestController):
    @expose(template='json')
    @paginate
    @auth
    def get(self, **kwargs):
        """
        List all the available requests
        """
        return context.instance.requests


    @expose(template='json')
    @auth
    @lock
    def delete(self, **kwargs):
        """
        Remove all the finished requests
        """
        num_requests = len(context.instance.requests)

        context.instance.requests = [x for x in context.instance.requests
                                     if not x.is_finished()]
        remaining = len(context.instance.requests)
        # Return the job statistics
        return {
            'cleaned': num_requests - remaining,
            'remaining': remaining,
        }


    @expose(template='json')
    @auth
    def post(self, **kwargs):
        """
        Pass through method to create any request
        """
        if isinstance(request.json, list):
            if all(isinstance(element, list) for element in request.json):
                return context.instance.submit_request(request.json, **kwargs)

            # The request.json has wrong format
            response.status = 500
            return {'message': 'The request format should be [[{c1},{c2}]]'}

        return context.instance.submit_request([[request.json]], **kwargs)


    @expose()
    def _lookup(self, request_id, *remainder):
        return RequestId(request_id), remainder
