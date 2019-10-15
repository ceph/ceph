# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import re
import json
try:
    from functools import lru_cache
except ImportError:
    from ..plugins.lru_cache import lru_cache

import cherrypy
from cherrypy.lib.static import serve_file

from . import Controller, UiApiController, BaseController, Proxy, Endpoint
from .. import mgr, logger


LANGUAGES = {f for f in os.listdir(mgr.get_frontend_path())
             if os.path.isdir(os.path.join(mgr.get_frontend_path(), f))}
LANGUAGES_PATH_MAP = {f.lower(): {'lang': f, 'path': os.path.join(mgr.get_frontend_path(), f)}
                      for f in LANGUAGES}
# pre-populating with the primary language subtag
for _lang in list(LANGUAGES_PATH_MAP.keys()):
    if '-' in _lang:
        LANGUAGES_PATH_MAP[_lang.split('-')[0]] = {
            'lang': LANGUAGES_PATH_MAP[_lang]['lang'], 'path': LANGUAGES_PATH_MAP[_lang]['path']}


def _get_default_language():
    with open("{}/../package.json".format(mgr.get_frontend_path()), "r") as f:
        config = json.load(f)
    return config['config']['locale']


DEFAULT_LANGUAGE = _get_default_language()
DEFAULT_LANGUAGE_PATH = os.path.join(mgr.get_frontend_path(), DEFAULT_LANGUAGE)


@Controller("/", secure=False)
class HomeController(BaseController):
    LANG_TAG_SEQ_RE = re.compile(r'\s*([^,]+)\s*,?\s*')
    LANG_TAG_RE = re.compile(
        r'^(?P<locale>[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8})?)(;q=(?P<weight>[01]\.\d{0,3}))?$')
    MAX_ACCEPTED_LANGS = 10

    @lru_cache()
    def _parse_accept_language(self, accept_lang_header):
        result = []
        for i, m in enumerate(self.LANG_TAG_SEQ_RE.finditer(accept_lang_header)):
            if i >= self.MAX_ACCEPTED_LANGS:
                logger.debug("reached max accepted languages, skipping remaining")
                break

            tag_match = self.LANG_TAG_RE.match(m.group(1))
            if tag_match is None:
                raise cherrypy.HTTPError(400, "Malformed 'Accept-Language' header")
            locale = tag_match.group('locale').lower()
            weight = tag_match.group('weight')
            if weight:
                try:
                    ratio = float(weight)
                except ValueError:
                    raise cherrypy.HTTPError(400, "Malformed 'Accept-Language' header")
            else:
                ratio = 1.0
            result.append((locale, ratio))

        result.sort(key=lambda l: l[0])
        result.sort(key=lambda l: l[1], reverse=True)
        logger.debug("language preference: %s", result)
        return [l[0] for l in result]

    def _language_dir(self, langs):
        for lang in langs:
            if lang in LANGUAGES_PATH_MAP:
                logger.debug("found directory for language '%s'", lang)
                cherrypy.response.headers['Content-Language'] = LANGUAGES_PATH_MAP[lang]['lang']
                return LANGUAGES_PATH_MAP[lang]['path']

        logger.debug("Languages '%s' not available, falling back to %s", langs, DEFAULT_LANGUAGE)
        cherrypy.response.headers['Content-Language'] = DEFAULT_LANGUAGE
        return DEFAULT_LANGUAGE_PATH

    @Proxy()
    def __call__(self, path, **params):
        if not path:
            path = "index.html"

        if 'cd-lang' in cherrypy.request.cookie:
            langs = [cherrypy.request.cookie['cd-lang'].value.lower()]
            logger.debug("frontend language from cookie: %s", langs)
        else:
            if 'Accept-Language' in cherrypy.request.headers:
                accept_lang_header = cherrypy.request.headers['Accept-Language']
                langs = self._parse_accept_language(accept_lang_header)
            else:
                langs = [DEFAULT_LANGUAGE.lower()]
            logger.debug("frontend language from headers: %s", langs)

        base_dir = self._language_dir(langs)
        full_path = os.path.join(base_dir, path)
        logger.debug("serving static content: %s", full_path)
        if 'Vary' in cherrypy.response.headers:
            cherrypy.response.headers['Vary'] = "{}, Accept-Language"
        else:
            cherrypy.response.headers['Vary'] = "Accept-Language"
        return serve_file(full_path)


@UiApiController("/langs", secure=False)
class LangsController(BaseController):
    @Endpoint('GET')
    def __call__(self):
        return list(LANGUAGES)
