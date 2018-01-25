# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from ..restresource import RESTResource
from ..tools import ApiController, AuthRequired


@ApiController('ping')
@AuthRequired()
class Ping(object):
    @cherrypy.expose
    def default(self, *args):
        return "pong"


@ApiController('echo1')
class EchoArgs(RESTResource):
    @RESTResource.args_from_json
    def create(self, msg):
        return {'echo': msg}


@ApiController('echo2')
class Echo(RESTResource):
    def create(self, data):
        return {'echo': data['msg']}
