from pecan import expose
from pecan.rest import RestController

from restful import module

import restful


class Doc(RestController):
    @expose(template='json')
    def get(self, **kwargs):
        """
        Show documentation information
        """
        return module.instance.get_doc_api(restful.api.Root)
