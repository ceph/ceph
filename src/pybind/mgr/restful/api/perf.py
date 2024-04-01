from pecan import expose, request, response
from pecan.rest import RestController

from restful import context
from restful.decorators import auth, lock, paginate

import re

class Perf(RestController):
    @expose(template='json')
    @paginate
    @auth
    def get(self, **kwargs):
        """
        List all the available performance counters

        Options:
         - 'daemon' -- filter by daemon, accepts Python regexp
        """

        counters = context.instance.get_unlabeled_perf_counters()

        if 'daemon' in kwargs:
            _re = re.compile(kwargs['daemon'])
            counters = {k: v for k, v in counters.items() if _re.match(k)}

        return counters
