from pecan import expose
from pecan.rest import RestController

from restful import context

import restful


class Doc(RestController):
    @expose(template='json')
    def get(self, **kwargs):
        """
        Show documentation information
        """
        return context.instance.get_doc_api(restful.api.Root)
