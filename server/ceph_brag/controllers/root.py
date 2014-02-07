from pecan import expose, request, abort, response
from webob.exc import status_map
from pecan.rest import RestController
from ceph_brag.model import db
import sys, traceback

class RootController(RestController):
    @expose('json')
    def get(self, *args, **kwargs):
        if len(args) is 0:
            #return the list of uuids
            try:
                result = db.get_uuids()
            except Exception as e:
                traceback.print_exc()
                abort(500, comment="Internal Server Error")
        elif len(args) is 1 or len(args) is 2 and args[1] == '':
            #/uuid
            try:
                result = db.get_versions(args[0])
            except Exception as e:
                traceback.print_exc()
                abort(status_code=500, comment="Internal Server Error")
           
            if result is None:
                abort(400, comment="Invalid UUID")
        elif len(args) is 2 or len(args) is 3 and args[2] == '':
            #/uuid/version_number
            try:
                result = db.get_brag(args[0], args[1])
            except Exception as e:
                traceback.print_exc()
                abort(status_code=500, comment="Internal Server Error")

            if result is None:
                abort(status_code=400, comment="Invalid UUID,version combination")
        else: 
            abort(status_code=400, comment="Invalid args")
        
        return result

    @expose(content_type='application/json')
    def put(self, *args, **kwargs):
        try:
            db.put_new_version(request.body)
        except Exception as e:
            traceback.print_exc()
            abort(status_code=500, comment="Internal Server Error")
       
        response.status = 201
        return "CREATED"
    
    @expose()
    def delete(self, *args, **kwargs):
        uuid = kwargs['uuid']
        if uuid is None:
            abort(status_code=400, comment="Required uuid parameter")

        try:
            status = db.delete_uuid(uuid)
        except Exception as e:
            traceback.print_exc()
            abort(status_code=500, comment="Internal Server Error")
        
        if status is not None:
            abort(status_code=status['status'], comment=status['comment'])
    
        response.status=200
        return "DELETED"
