import cherrypy


class ControllerAuthMixin:
    @staticmethod
    def _delete_token_cookie(token):
        cherrypy.response.cookie['token'] = token
        cherrypy.response.cookie['token']['expires'] = 0
        cherrypy.response.cookie['token']['max-age'] = 0

    @staticmethod
    def _set_token_cookie(url_prefix, token):
        cherrypy.response.cookie['token'] = token
        if url_prefix == 'https':
            cherrypy.response.cookie['token']['secure'] = True
        cherrypy.response.cookie['token']['HttpOnly'] = True
        cherrypy.response.cookie['token']['path'] = '/'
        cherrypy.response.cookie['token']['SameSite'] = 'Strict'

    @staticmethod
    def _delete_cookie(name, value):
        cherrypy.response.cookie[name] = value
        cherrypy.response.cookie[name]['expires'] = 0
        cherrypy.response.cookie[name]['max-age'] = 0
