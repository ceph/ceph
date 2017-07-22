import logging

# Inherit teuthology's log level
teuthology_log = logging.getLogger('teuthology')
log = logging.getLogger(__name__)
log.setLevel(teuthology_log.level)
