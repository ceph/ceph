# vim: ts=4 sw=4 smarttab expandtab

import errno
import json
import logging
import logging.handlers
import os
import rados
import textwrap
import xml.etree.ElementTree
import xml.sax.saxutils

import flask
from ceph_argparse import \
    ArgumentError, CephPgid, CephOsdName, CephChoices, CephPrefix, \
    concise_sig, descsort, parse_funcsig, parse_json_funcsigs, \
    validate, json_command

#
# Globals and defaults
#

DEFAULT_ADDR = '0.0.0.0'
DEFAULT_PORT = '5000'
DEFAULT_ID = 'restapi'

DEFAULT_BASEURL = '/api/v0.1'
DEFAULT_LOG_LEVEL = 'warning'
DEFAULT_LOGDIR = '/var/log/ceph'
# default client name will be 'client.<DEFAULT_ID>'

# 'app' must be global for decorators, etc.
APPNAME = '__main__'
app = flask.Flask(APPNAME)

LOGLEVELS = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG,
}


def find_up_osd(app):
    '''
    Find an up OSD.  Return the last one that's up.
    Returns id as an int.
    '''
    ret, outbuf, outs = json_command(app.ceph_cluster, prefix="osd dump",
                                     argdict=dict(format='json'))
    if ret:
        raise EnvironmentError(ret, 'Can\'t get osd dump output')
    try:
        osddump = json.loads(outbuf)
    except:
        raise EnvironmentError(errno.EINVAL, 'Invalid JSON back from osd dump')
    osds = [osd['osd'] for osd in osddump['osds'] if osd['up']]
    if not osds:
        return None
    return int(osds[-1])


METHOD_DICT = {'r': ['GET'], 'w': ['PUT', 'DELETE']}


def api_setup(app, conf, cluster, clientname, clientid, args):
    '''
    This is done globally, and cluster connection kept open for
    the lifetime of the daemon.  librados should assure that even
    if the cluster goes away and comes back, our connection remains.

    Initialize the running instance.  Open the cluster, get the command
    signatures, module, perms, and help; stuff them away in the app.ceph_urls
    dict.  Also save app.ceph_sigdict for help() handling.
    '''
    def get_command_descriptions(cluster, target=('mon', '')):
        ret, outbuf, outs = json_command(cluster, target,
                                         prefix='get_command_descriptions',
                                         timeout=30)
        if ret:
            err = "Can't get command descriptions: {0}".format(outs)
            app.logger.error(err)
            raise EnvironmentError(ret, err)

        try:
            sigdict = parse_json_funcsigs(outbuf, 'rest')
        except Exception as e:
            err = "Can't parse command descriptions: {}".format(e)
            app.logger.error(err)
            raise EnvironmentError(err)
        return sigdict

    app.ceph_cluster = cluster or 'ceph'
    app.ceph_urls = {}
    app.ceph_sigdict = {}
    app.ceph_baseurl = ''

    conf = conf or ''
    cluster = cluster or 'ceph'
    clientid = clientid or DEFAULT_ID
    clientname = clientname or 'client.' + clientid

    app.ceph_cluster = rados.Rados(name=clientname, conffile=conf)
    app.ceph_cluster.conf_parse_argv(args)
    app.ceph_cluster.connect()

    app.ceph_baseurl = app.ceph_cluster.conf_get('restapi_base_url') \
        or DEFAULT_BASEURL
    if app.ceph_baseurl.endswith('/'):
        app.ceph_baseurl = app.ceph_baseurl[:-1]
    addr = app.ceph_cluster.conf_get('public_addr') or DEFAULT_ADDR

    # remove any nonce from the conf value
    addr = addr.split('/')[0]
    addr, port = addr.rsplit(':', 1)
    addr = addr or DEFAULT_ADDR
    port = port or DEFAULT_PORT
    port = int(port)

    loglevel = app.ceph_cluster.conf_get('restapi_log_level') \
        or DEFAULT_LOG_LEVEL
    # ceph has a default log file for daemons only; clients (like this)
    # default to "".  Override that for this particular client.
    logfile = app.ceph_cluster.conf_get('log_file')
    if not logfile:
        logfile = os.path.join(
            DEFAULT_LOGDIR,
            '{cluster}-{clientname}.{pid}.log'.format(
                cluster=cluster,
                clientname=clientname,
                pid=os.getpid()
            )
        )
    app.logger.addHandler(logging.handlers.WatchedFileHandler(logfile))
    app.logger.setLevel(LOGLEVELS[loglevel.lower()])
    for h in app.logger.handlers:
        h.setFormatter(logging.Formatter(
            '%(asctime)s %(name)s %(levelname)s: %(message)s'))

    app.ceph_sigdict = get_command_descriptions(app.ceph_cluster)

    osdid = find_up_osd(app)
    if osdid is not None:
        osd_sigdict = get_command_descriptions(app.ceph_cluster,
                                               target=('osd', int(osdid)))

        # shift osd_sigdict keys up to fit at the end of the mon's app.ceph_sigdict
        maxkey = sorted(app.ceph_sigdict.keys())[-1]
        maxkey = int(maxkey.replace('cmd', ''))
        osdkey = maxkey + 1
        for k, v in osd_sigdict.iteritems():
            newv = v
            newv['flavor'] = 'tell'
            globk = 'cmd' + str(osdkey)
            app.ceph_sigdict[globk] = newv
            osdkey += 1

    # app.ceph_sigdict maps "cmdNNN" to a dict containing:
    # 'sig', an array of argdescs
    # 'help', the helptext
    # 'module', the Ceph module this command relates to
    # 'perm', a 'rwx*' string representing required permissions, and also
    #    a hint as to whether this is a GET or POST/PUT operation
    # 'avail', a comma-separated list of strings of consumers that should
    #    display this command (filtered by parse_json_funcsigs() above)
    app.ceph_urls = {}
    for cmdnum, cmddict in app.ceph_sigdict.iteritems():
        cmdsig = cmddict['sig']
        flavor = cmddict.get('flavor', 'mon')
        url, params = generate_url_and_params(app, cmdsig, flavor)
        perm = cmddict['perm']
        for k in METHOD_DICT.iterkeys():
            if k in perm:
                methods = METHOD_DICT[k]
        urldict = {'paramsig': params,
                   'help': cmddict['help'],
                   'module': cmddict['module'],
                   'perm': perm,
                   'flavor': flavor,
                   'methods': methods, }

        # app.ceph_urls contains a list of urldicts (usually only one long)
        if url not in app.ceph_urls:
            app.ceph_urls[url] = [urldict]
        else:
            # If more than one, need to make union of methods of all.
            # Method must be checked in handler
            methodset = set(methods)
            for old_urldict in app.ceph_urls[url]:
                methodset |= set(old_urldict['methods'])
            methods = list(methodset)
            app.ceph_urls[url].append(urldict)

        # add, or re-add, rule with all methods and urldicts
        app.add_url_rule(url, url, handler, methods=methods)
        url += '.<fmt>'
        app.add_url_rule(url, url, handler, methods=methods)

    app.logger.debug("urls added: %d", len(app.ceph_urls))

    app.add_url_rule('/<path:catchall_path>', '/<path:catchall_path>',
                     handler, methods=['GET', 'PUT'])
    return addr, port


def generate_url_and_params(app, sig, flavor):
    '''
    Digest command signature from cluster; generate an absolute
    (including app.ceph_baseurl) endpoint from all the prefix words,
    and a list of non-prefix param descs
    '''

    url = ''
    params = []
    # the OSD command descriptors don't include the 'tell <target>', so
    # tack it onto the front of sig
    if flavor == 'tell':
        tellsig = parse_funcsig(['tell',
                                {'name': 'target', 'type': 'CephOsdName'}])
        sig = tellsig + sig

    for desc in sig:
        # prefixes go in the URL path
        if desc.t == CephPrefix:
            url += '/' + desc.instance.prefix
        else:
            # tell/<target> is a weird case; the URL includes what
            # would everywhere else be a parameter
            if flavor == 'tell' and ((desc.t, desc.name) ==
               (CephOsdName, 'target')):
                url += '/<target>'
            else:
                params.append(desc)

    return app.ceph_baseurl + url, params


#
# end setup (import-time) functions, begin request-time functions
#
def concise_sig_for_uri(sig, flavor):
    '''
    Return a generic description of how one would send a REST request for sig
    '''
    prefix = []
    args = []
    ret = ''
    if flavor == 'tell':
        ret = 'tell/<osdid-or-pgid>/'
    for d in sig:
        if d.t == CephPrefix:
            prefix.append(d.instance.prefix)
        else:
            args.append(d.name + '=' + str(d))
    ret += '/'.join(prefix)
    if args:
        ret += '?' + '&'.join(args)
    return ret


def show_human_help(prefix):
    '''
    Dump table showing commands matching prefix
    '''
    # XXX There ought to be a better discovery mechanism than an HTML table
    s = '<html><body><table border=1><th>Possible commands:</th><th>Method</th><th>Description</th>'

    permmap = {'r': 'GET', 'rw': 'PUT', 'rx': 'GET', 'rwx': 'PUT'}
    line = ''
    for cmdsig in sorted(app.ceph_sigdict.itervalues(), cmp=descsort):
        concise = concise_sig(cmdsig['sig'])
        flavor = cmdsig.get('flavor', 'mon')
        if flavor == 'tell':
            concise = 'tell/<target>/' + concise
        if concise.startswith(prefix):
            line = ['<tr><td>']
            wrapped_sig = textwrap.wrap(
                concise_sig_for_uri(cmdsig['sig'], flavor), 40
            )
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
    '''
    For every request, log it.  XXX Probably overkill for production
    '''
    app.logger.info(flask.request.url + " from " + flask.request.remote_addr + " " + flask.request.user_agent.string)
    app.logger.debug("Accept: %s", flask.request.accept_mimetypes.values())


@app.route('/')
def root_redir():
    return flask.redirect(app.ceph_baseurl)


def make_response(fmt, output, statusmsg, errorcode):
    '''
    If formatted output, cobble up a response object that contains the
    output and status wrapped in enclosing objects; if nonformatted, just
    use output+status.  Return HTTP status errorcode in any event.
    '''
    response = output
    if fmt:
        if 'json' in fmt:
            try:
                native_output = json.loads(output or '[]')
                response = json.dumps({"output": native_output,
                                       "status": statusmsg})
            except:
                return flask.make_response("Error decoding JSON from " +
                                           output, 500)
        elif 'xml' in fmt:
            # XXX
            # one is tempted to do this with xml.etree, but figuring out how
            # to 'un-XML' the XML-dumped output so it can be reassembled into
            # a piece of the tree here is beyond me right now.
            # ET = xml.etree.ElementTree
            # resp_elem = ET.Element('response')
            # o = ET.SubElement(resp_elem, 'output')
            # o.text = output
            # s = ET.SubElement(resp_elem, 'status')
            # s.text = statusmsg
            # response = ET.tostring(resp_elem)
            response = '''
<response>
  <output>
    {0}
  </output>
  <status>
    {1}
  </status>
</response>'''.format(response, xml.sax.saxutils.escape(statusmsg))
    else:
        if not 200 <= errorcode < 300:
            response = response + '\n' + statusmsg + '\n'

    return flask.make_response(response, errorcode)


def handler(catchall_path=None, fmt=None, target=None):
    '''
    Main endpoint handler; generic for every endpoint, including catchall.
    Handles the catchall, anything with <.fmt>, anything with embedded
    <target>.  Partial match or ?help cause the HTML-table
    "show_human_help" output.
    '''

    ep = catchall_path or flask.request.endpoint
    ep = ep.replace('.<fmt>', '')

    if ep[0] != '/':
        ep = '/' + ep

    # demand that endpoint begin with app.ceph_baseurl
    if not ep.startswith(app.ceph_baseurl):
        return make_response(fmt, '', 'Page not found', 404)

    rel_ep = ep[len(app.ceph_baseurl) + 1:]

    # Extensions override Accept: headers override defaults
    if not fmt:
        if 'application/json' in flask.request.accept_mimetypes.values():
            fmt = 'json'
        elif 'application/xml' in flask.request.accept_mimetypes.values():
            fmt = 'xml'

    prefix = ''
    pgid = None
    cmdtarget = 'mon', ''

    if target:
        # got tell/<target>; validate osdid or pgid
        name = CephOsdName()
        pgidobj = CephPgid()
        try:
            name.valid(target)
        except ArgumentError:
            # try pgid
            try:
                pgidobj.valid(target)
            except ArgumentError:
                return flask.make_response("invalid osdid or pgid", 400)
            else:
                # it's a pgid
                pgid = pgidobj.val
                cmdtarget = 'pg', pgid
        else:
            # it's an osd
            cmdtarget = name.nametype, name.nameid

        # prefix does not include tell/<target>/
        prefix = ' '.join(rel_ep.split('/')[2:]).strip()
    else:
        # non-target command: prefix is entire path
        prefix = ' '.join(rel_ep.split('/')).strip()

    # show "match as much as you gave me" help for unknown endpoints
    if ep not in app.ceph_urls:
        helptext = show_human_help(prefix)
        if helptext:
            resp = flask.make_response(helptext, 400)
            resp.headers['Content-Type'] = 'text/html'
            return resp
        else:
            return make_response(fmt, '', 'Invalid endpoint ' + ep, 400)

    found = None
    exc = ''
    for urldict in app.ceph_urls[ep]:
        if flask.request.method not in urldict['methods']:
            continue
        paramsig = urldict['paramsig']

        # allow '?help' for any specifically-known endpoint
        if 'help' in flask.request.args:
            response = flask.make_response('{0}: {1}'.
                                           format(prefix +
                                                  concise_sig(paramsig),
                                                  urldict['help']))
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
                found = urldict
                break
            except Exception as e:
                exc += str(e)
                continue
        else:
            if flask.request.args:
                continue
            found = urldict
            argdict = {}
            break

    if not found:
        return make_response(fmt, '', exc + '\n', 400)

    argdict['format'] = fmt or 'plain'
    argdict['module'] = found['module']
    argdict['perm'] = found['perm']
    if pgid:
        argdict['pgid'] = pgid

    if not cmdtarget:
        cmdtarget = ('mon', '')

    app.logger.debug('sending command prefix %s argdict %s', prefix, argdict)
    ret, outbuf, outs = json_command(app.ceph_cluster, prefix=prefix,
                                     target=cmdtarget,
                                     inbuf=flask.request.data, argdict=argdict)
    if ret:
        return make_response(fmt, '', 'Error: {0} ({1})'.format(outs, ret), 400)

    response = make_response(fmt, outbuf, outs or 'OK', 200)
    if fmt:
        contenttype = 'application/' + fmt.replace('-pretty', '')
    else:
        contenttype = 'text/plain'
    response.headers['Content-Type'] = contenttype
    return response


#
# Main entry point from wrapper/WSGI server: call with cmdline args,
# get back the WSGI app entry point
#
def generate_app(conf, cluster, clientname, clientid, args):
    addr, port = api_setup(app, conf, cluster, clientname, clientid, args)
    app.ceph_addr = addr
    app.ceph_port = port
    return app
