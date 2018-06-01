from . import UiApiController, BaseController, Endpoint
from .. import logger


@UiApiController('/logging')
class Logging(BaseController):

    @Endpoint('POST', path='js-error')
    def jsError(self, url, message, stack):
        logger.error('frontend error (%s): %s\n %s\n', url, message, stack)
