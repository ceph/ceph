# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from ..tools import ApiController, AuthRequired, RESTController


@ApiController('ping')
@AuthRequired()
class Ping(object):
    @cherrypy.expose
    def default(self):
        return 'pong'


@ApiController('echo1')
class EchoArgs(RESTController):
    @RESTController.args_from_json
    def create(self, msg):
        return {'echo': msg}


@ApiController('echo2')
class Echo(RESTController):
    def create(self, data):
        return {'echo': data['msg']}
