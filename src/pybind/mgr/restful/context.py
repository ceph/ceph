# Global instance to share
try:
    from restful.module import Module
except ImportError:
    pass  # only for type checking
instance = None  # type: Module
