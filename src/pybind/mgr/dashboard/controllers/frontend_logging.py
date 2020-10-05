from __future__ import absolute_import

import logging

from . import BaseController, Endpoint, UiApiController

logger = logging.getLogger('frontend.error')


@UiApiController('/logging', secure=False)
class FrontendLogging(BaseController):

    @Endpoint('POST', path='js-error')
    def jsError(self, url, message, stack=None):  # noqa: N802
        logger.error('(%s): %s\n %s\n', url, message, stack)
