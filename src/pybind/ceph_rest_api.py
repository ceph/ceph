#!/usr/bin/python
# vim: ts=4 sw=4 smarttab expandtab

import os
import collections
import ConfigParser
import json
import logging
import logging.handlers
import rados
import textwrap
import xml.etree.ElementTree
import xml.sax.saxutils

import flask
from ceph_argparse import *

#
# Globals
#

APPNAME = '__main__'
DEFAULT_BASEURL = '/api/v0.1'
DEFAULT_ADDR = '0.0.0.0:5000'
DEFAULT_LOG_LEVEL = 'warning'
DEFAULT_CLIENTNAME = 'client.restapi'
DEFAULT_LOG_FILE = '/var/log/ceph/' + DEFAULT_CLIENTNAME + '.log'

app = flask.Flask(APPNAME)

LOGLEVELS = {
    'critical':logging.CRITICAL,
    'error':logging.ERROR,
    'warning':logging.WARNING,
    'info':logging.INFO,
    'debug':logging.DEBUG,
}

# my globals, in a named tuple for usage clarity

glob = collections.namedtuple('gvars', 'cluster urls sigdict baseurl')
glob.cluster = None
glob.urls = {}
glob.sigdict = {}
glob.baseurl = ''

def load_conf(clustername='ceph', conffile=None):
    import contextlib


    class _TrimIndentFile(object):
        def __init__(self, fp):
            self.fp = fp

        def readline(self):
            line = self.fp.readline()
            return line.lstrip(' \t')


    def _optionxform(s):
        s = s.replace('_', ' ')
        s = '_'.join(s.split())
        return s


    def parse(fp):
        cfg = ConfigParser.RawConfigParser()
        cfg.optionxform = _optionxform
        ifp = _TrimIndentFile(fp)
        cfg.readfp(ifp)
        return cfg


    def load(path):
        f = file(path)
        with contextlib.closing(f):
            return parse(f)

    if conffile:
        # from CEPH_CONF
        return load(conffile)
    else:
        for path in [
            '/etc/ceph/{0}.conf'.format(clustername),
            os.path.expanduser('~/.ceph/{0}.conf'.format(clustername)),
            '{0}.conf'.format(clustername),
        ]:
            if os.path.exists(path):
                return load(path)

    raise EnvironmentError('No conf file found for "{0}"'.format(clustername))

def get_conf(cfg, clientname, key):
    try:
        return cfg.get(clientname, 'restapi_' + key)
    except ConfigParser.NoOptionError:
        return None

# XXX this is done globally, and cluster connection kept open; there
# are facilities to pass around global info to requests and to
# tear down connections between requests if it becomes important

def api_setup():
    """
    Initialize the running instance.  Open the cluster, get the command
    signatures, module, perms, and help; stuff them away in the glob.urls
    dict.
    """

    conffile = os.environ.get('CEPH_CONF', '')
    clustername = os.environ.get('CEPH_CLUSTER_NAME', 'ceph')
    clientname = os.environ.get('CEPH_NAME', DEFAULT_CLIENTNAME)
    try:
        err = ''
        cfg = load_conf(clustername, conffile)
    except Exception as e:
        err = "Can't load Ceph conf file: " + str(e)
        app.logger.critical(err)
        app.logger.critical("CEPH_CONF: %s", conffile)
        app.logger.critical("CEPH_CLUSTER_NAME: %s", clustername)
        raise EnvironmentError(err)

    client_logfile = '/var/log/ceph' + clientname + '.log'

    glob.cluster = rados.Rados(name=clientname, conffile=conffile)
    glob.cluster.connect()

    glob.baseurl = get_conf(cfg, clientname, 'base_url') or DEFAULT_BASEURL
    if glob.baseurl.endswith('/'):
        glob.baseurl
    addr = get_conf(cfg, clientname, 'public_addr') or DEFAULT_ADDR
    addrport = addr.rsplit(':', 1)
    addr = addrport[0]
    if len(addrport) > 1:
        port = addrport[1]
    else:
        port = DEFAULT_ADDR.rsplit(':', 1)
    port = int(port)

    loglevel = get_conf(cfg, clientname, 'log_level') or DEFAULT_LOG_LEVEL
    logfile = get_conf(cfg, clientname, 'log_file') or client_logfile
    app.logger.addHandler(logging.handlers.WatchedFileHandler(logfile))
    app.logger.setLevel(LOGLEVELS[loglevel.lower()])
    for h in app.logger.handlers:
        h.setFormatter(logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s: %(message)s'))

    ret, outbuf, outs = json_command(glob.cluster,
                                     prefix='get_command_descriptions')
    if ret:
        err = "Can't contact cluster for command descriptions: {0}".format(outs)
        app.logger.error(err)
        raise EnvironmentError(ret, err)

    try:
        glob.sigdict = parse_json_funcsigs(outbuf, 'rest')
    except Exception as e:
        err = "Can't parse command descriptions: {}".format(e)
        app.logger.error(err)
        raise EnvironmentError(err)

    # glob.sigdict maps "cmdNNN" to a dict containing:
    # 'sig', an array of argdescs
    # 'help', the helptext
    # 'module', the Ceph module this command relates to
    # 'perm', a 'rwx*' string representing required permissions, and also
    #    a hint as to whether this is a GET or POST/PUT operation
    # 'avail', a comma-separated list of strings of consumers that should
    #    display this command (filtered by parse_json_funcsigs() above)
    glob.urls = {}
    for cmdnum, cmddict in glob.sigdict.iteritems():
        cmdsig = cmddict['sig']
        url, params = generate_url_and_params(cmdsig)
        if url in glob.urls:
            continue
        else:
            perm = cmddict['perm']
            urldict = {'paramsig':params,
                       'help':cmddict['help'],
                       'module':cmddict['module'],
                       'perm':perm,
                      }
            method_dict = {'r':['GET'],
                       'w':['PUT', 'DELETE']}
            for k in method_dict.iterkeys():
                if k in perm:
                    methods = method_dict[k]
            app.add_url_rule(url, url, handler, methods=methods)
            glob.urls[url] = urldict

            url += '.<fmt>'
            app.add_url_rule(url, url, handler, methods=methods)
            glob.urls[url] = urldict
    app.logger.debug("urls added: %d", len(glob.urls))

    app.add_url_rule('/<path:catchall_path>', '/<path:catchall_path>',
                     handler, methods=['GET', 'PUT'])
    return addr, port


def generate_url_and_params(sig):
    """
    Digest command signature from cluster; generate an absolute
    (including glob.baseurl) endpoint from all the prefix words,
    and a dictionary of non-prefix parameters
    """

    url = ''
    params = []
    for desc in sig:
        if desc.t == CephPrefix:
            url += '/' + desc.instance.prefix
        elif desc.t == CephChoices and \
             len(desc.instance.strings) == 1 and \
             desc.req and \
             not str(desc.instance).startswith('--'):
                url += '/' + str(desc.instance)
        else:
            params.append(desc)
    return glob.baseurl + url, params


def concise_sig_for_uri(sig):
    """
    Return a generic description of how one would send a REST request for sig
    """
    prefix = []
    args = []
    for d in sig:
        if d.t == CephPrefix:
            prefix.append(d.instance.prefix)
        else:
            args.append(d.name + '=' + str(d))
    sig = '/'.join(prefix)
    if args:
        sig += '?' + '&'.join(args)
    return sig

def show_human_help(prefix):
    """
    Dump table showing commands matching prefix
    """
    # XXX this really needs to be a template
    #s = '<html><body><style>.colhalf { width: 50%;} body{word-wrap:break-word;}</style>'
    #s += '<table border=1><col class=colhalf /><col class=colhalf />'
    #s += '<th>Possible commands:</th>'
    # XXX the above mucking with css doesn't cause sensible columns.
    s = '<html><body><table border=1><th>Possible commands:</th><th>Method</th><th>Description</th>'

    possible = []
    permmap = {'r':'GET', 'rw':'PUT'}
    line = ''
    for cmdsig in sorted(glob.sigdict.itervalues(), cmp=descsort):
        concise = concise_sig(cmdsig['sig'])
        if concise.startswith(prefix):
            line = ['<tr><td>']
            wrapped_sig = textwrap.wrap(concise_sig_for_uri(cmdsig['sig']), 40)
            for sigline in wrapped_sig:
                line.append(flask.escape(sigline) + '\n')
            line.append('</td><td>')
            line.append(permmap[cmdsig['perm']])
            line.append('</td><td>')
            line.append(flask.escape(cmdsig['help']))
            line.append('</td></tr>\n')
            s += ''.join(line)

    s += '</table></body></html>'
    if line:
        return s
    else:
        return ''

@app.before_request
def log_request():
    """
    For every request, log it.  XXX Probably overkill for production
    """
    app.logger.info(flask.request.url + " from " + flask.request.remote_addr + " " + flask.request.user_agent.string)
    app.logger.debug("Accept: %s", flask.request.accept_mimetypes.values())


@app.route('/')
def root_redir():
    return flask.redirect(glob.baseurl)

def make_response(fmt, output, statusmsg, errorcode):
    """
    If formatted output, cobble up a response object that contains the
    output and status wrapped in enclosing objects; if nonformatted, just
    use output.  Return HTTP status errorcode in any event.
    """
    response = output
    if fmt:
        if 'json' in fmt:
            try:
                native_output = json.loads(output or '[]')
                response = json.dumps({"output":native_output,
                                       "status":statusmsg})
            except:
                return flask.make_response("Error decoding JSON from " +
                                           output, 500)
        elif 'xml' in fmt:
            # one is tempted to do this with xml.etree, but figuring out how
            # to 'un-XML' the XML-dumped output so it can be reassembled into
            # a piece of the tree here is beyond me right now.
            #ET = xml.etree.ElementTree
            #resp_elem = ET.Element('response')
            #o = ET.SubElement(resp_elem, 'output')
            #o.text = output
            #s = ET.SubElement(resp_elem, 'status')
            #s.text = statusmsg
            #response = ET.tostring(resp_elem)
            response = '''
<response>
  <output>
    {0}
  </output>
  <status>
    {1}
  </status>
</response>'''.format(response, xml.sax.saxutils.escape(statusmsg))

    return flask.make_response(response, errorcode)

def handler(catchall_path=None, fmt=None):
    """
    Main endpoint handler; generic for every endpoint
    """

    if (catchall_path):
        ep = catchall_path.replace('.<fmt>', '')
    else:
        ep = flask.request.endpoint.replace('.<fmt>', '')

    if ep[0] != '/':
        ep = '/' + ep

    # Extensions override Accept: headers override defaults
    if not fmt:
        if 'application/json' in flask.request.accept_mimetypes.values():
            fmt = 'json'
        elif 'application/xml' in flask.request.accept_mimetypes.values():
            fmt = 'xml'

    # demand that endpoint begin with glob.baseurl
    if not ep.startswith(glob.baseurl):
        return make_response(fmt, '', 'Page not found', 404)

    relative_endpoint = ep[len(glob.baseurl)+1:]
    prefix = ' '.join(relative_endpoint.split('/')).strip()

    # show "match as much as you gave me" help for unknown endpoints
    if not ep in glob.urls:
        helptext = show_human_help(prefix)
        if helptext:
            resp = flask.make_response(helptext, 400)
            resp.headers['Content-Type'] = 'text/html'
            return resp
        else:
            return make_response(fmt, '', 'Invalid endpoint ' + ep, 400)

    urldict = glob.urls[ep]
    paramsig = urldict['paramsig']

    # allow '?help' for any specifically-known endpoint
    if 'help' in flask.request.args:
        response = flask.make_response('{0}: {1}'.\
            format(prefix + concise_sig(paramsig), urldict['help']))
        response.headers['Content-Type'] = 'text/plain'
        return response

    # if there are parameters for this endpoint, process them
    if paramsig:
        args = {}
        for k, l in flask.request.args.iterlists():
            if len(l) == 1:
                args[k] = l[0]
            else:
                args[k] = l

        # is this a valid set of params?
        try:
            argdict = validate(args, paramsig)
        except Exception as e:
            return make_response(fmt, '', str(e) + '\n', 400)
    else:
        # no parameters for this endpoint; complain if args are supplied
        if flask.request.args:
            return make_response(fmt, '', ep + 'takes no params', 400)
        argdict = {}


    argdict['format'] = fmt or 'plain'
    argdict['module'] = urldict['module']
    argdict['perm'] = urldict['perm']

    app.logger.debug('sending command prefix %s argdict %s', prefix, argdict)
    ret, outbuf, outs = json_command(glob.cluster, prefix=prefix,
                                     inbuf=flask.request.data, argdict=argdict)
    if ret:
        return make_response(fmt, '', 'Error: {0} ({1})'.format(outs, ret), 400)

    response = make_response(fmt, outbuf, outs or 'OK', 200)
    if fmt:
        contenttype = 'application/' + fmt.replace('-pretty','')
    else:
        contenttype = 'text/plain'
    response.headers['Content-Type'] = contenttype
    return response

addr, port = api_setup()
