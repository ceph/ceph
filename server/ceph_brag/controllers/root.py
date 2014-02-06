from pecan import expose, request, abort, response
from webob.exc import status_map
from pecan.rest import RestController
from ceph_brag.model.db import put_new_version
import sys, traceback

class RootController(RestController):
    @expose()
    def get(self, *args, **kwargs):
        return ""

    @expose()
    def put(self, *args, **kwargs):
        try:
          put_new_version(request.body)
        except Exception as e:
          print >> sys.stderr, "Got an exception " + str(e)
          traceback.print_exc()
          abort(status_code=500, comment="Internal Server Error")
        
        print >> sys.stderr, "Successfully completed the new version"
        response.status_int = 201
        return "Created"
    
    @expose()
    def delete(self):
        return ""
