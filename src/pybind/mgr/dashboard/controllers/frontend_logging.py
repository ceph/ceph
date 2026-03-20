import logging

from . import BaseController, Endpoint, UIRouter

logger = logging.getLogger(__name__)


@UIRouter('/logging', secure=False)
class FrontendLogging(BaseController):

    @Endpoint('POST', path='js-error')
    def jsError(self, url, message, stack=None):  # noqa: N802
        logger.error('(%s): %s\n %s\n', url, message, stack)
