from pecan import expose, request, abort, response
from webob import exc
from pecan.rest import RestController
from ceph_brag.model import db
import sys, traceback

class RootController(RestController):
    def fail(self, status_code=200, msg="OK"):
        response.status = status_code
        return msg

    @expose('json')
    def get(self, *args, **kwargs):
        if len(args) is 0:
            #return the list of uuids
            try:
                result = db.get_uuids()
            except Exception as e:
                return self.fail(500, msg="Internal Server Error")
        elif len(args) is 1 or len(args) is 2 and args[1] == '':
            #/uuid
            try:
                result = db.get_versions(args[0])
            except Exception as e:
                return self.fail(status_code=500, msg="Internal Server Error")
           
            if result is None:
                return self.fail(400, msg="Invalid UUID")
        elif len(args) is 2 or len(args) is 3 and args[2] == '':
            #/uuid/version_number
            try:
                result = db.get_brag(args[0], args[1])
            except Exception as e:
                return self.fail(status_code=500, msg="Internal Server Error")

            if result is None:
                return self.fail(status_code=400, msg="Invalid UUID,version combination")
        else:
            return self.fail(status_code=400, msg="Invalid args")

        return result

    @expose(content_type='application/json')
    def put(self, *args, **kwargs):
        try:
            db.put_new_version(request.body)
        except ValueError as ve:
            return self.fail(status_code=422, msg="Improper payload : " + str(ve))
        except KeyError as ke:
            msg = "Payload not as expected, some keys are missing : " + str(ke)
            return self.fail(status_code=422, msg=msg)
        except Exception as e:
            return self.fail(status_code=500, msg="Internal Server Error : " + str(e))
       
        response.status = 201
        return "CREATED"
    
    @expose()
    def delete(self, *args, **kwargs):
        if 'uuid' not in kwargs:
            return self.fail(status_code=400, msg="Required uuid parameter")
        
        uuid = kwargs['uuid']
        try:
            status = db.delete_uuid(uuid)
        except Exception as e:
            return self.fail(status_code=500, msg="Internal Server Error")
        
        if status is not None:
            return self.fail(status_code=status['status'], msg=status['msg'])
    
        response.status=200
        return "DELETED"
