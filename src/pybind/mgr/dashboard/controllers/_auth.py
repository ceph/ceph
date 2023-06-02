import cherrypy

from .. import mgr

class ControllerAuthMixin:
    @staticmethod
    def _delete_token_cookie(token):
        cherrypy.response.cookie['token'] = token
        cherrypy.response.cookie['token']['expires'] = 0
        cherrypy.response.cookie['token']['max-age'] = 0

    @staticmethod
    def _set_token_cookie(url_prefix, token):
        allow_embedding_url = mgr.get_module_option('allow_embedding_url', '')
        cherrypy.response.cookie['token'] = token
        if url_prefix == 'https':
            cherrypy.response.cookie['token']['secure'] = True
        cherrypy.response.cookie['token']['HttpOnly'] = True
        cherrypy.response.cookie['token']['path'] = '/'
        cherrypy.response.cookie['token']['SameSite'] = 'Strict'
        if url_prefix == 'https' and allow_embedding_url and allow_embedding_url != 'None':
            cherrypy.response.cookie['token']['SameSite'] = 'None'
