class MirrorException(Exception):
    def __init__(self, error_code, error_msg=''):
        super().__init__(error_code, error_msg)
