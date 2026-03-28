import boto.s3.connection
import munch
import itertools
import os
import random
import string
import yaml
import re
from lxml import etree

from doctest import Example
from lxml.doctestcompare import LXMLOutputChecker

s3 = munch.Munch()
config = munch.Munch()
prefix = ''

bucket_counter = itertools.count(1)
key_counter = itertools.count(1)

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )

def nuke_bucket(bucket):
    try:
        bucket.set_canned_acl('private')
        # TODO: deleted_cnt and the while loop is a work around for rgw
        # not sending the
        deleted_cnt = 1
        while deleted_cnt:
            deleted_cnt = 0
            for key in bucket.list():
                print('Cleaning bucket {bucket} key {key}'.format(
                    bucket=bucket,
                    key=key,
                    ))
                key.set_canned_acl('private')
                key.delete()
                deleted_cnt += 1
        bucket.delete()
    except boto.exception.S3ResponseError as e:
        # TODO workaround for buggy rgw that fails to send
        # error_code, remove
        if (e.status == 403
            and e.error_code is None
            and e.body == ''):
            e.error_code = 'AccessDenied'
        if e.error_code != 'AccessDenied':
            print('GOT UNWANTED ERROR', e.error_code)
            raise
        # seems like we're not the owner of the bucket; ignore
        pass

def nuke_prefixed_buckets():
    for name, conn in list(s3.items()):
        print('Cleaning buckets from connection {name}'.format(name=name))
        for bucket in conn.get_all_buckets():
            if bucket.name.startswith(prefix):
                print('Cleaning bucket {bucket}'.format(bucket=bucket))
                nuke_bucket(bucket)

    print('Done with cleanup of test buckets.')

def read_config(fp):
    config = munch.Munch()
    g = yaml.safe_load_all(fp)
    for new in g:
        config.update(munch.Munchify(new))
    return config

def connect(conf):
    mapping = dict(
        port='port',
        host='host',
        is_secure='is_secure',
        access_key='aws_access_key_id',
        secret_key='aws_secret_access_key',
        )
    kwargs = dict((mapping[k],v) for (k,v) in conf.items() if k in mapping)
    #process calling_format argument
    calling_formats = dict(
        ordinary=boto.s3.connection.OrdinaryCallingFormat(),
        subdomain=boto.s3.connection.SubdomainCallingFormat(),
        vhost=boto.s3.connection.VHostCallingFormat(),
        )
    kwargs['calling_format'] = calling_formats['ordinary']
    if 'calling_format' in conf:
        raw_calling_format = conf['calling_format']
        try:
            kwargs['calling_format'] = calling_formats[raw_calling_format]
        except KeyError:
            raise RuntimeError(
                'calling_format unknown: %r' % raw_calling_format
                )
    # TODO test vhost calling format
    conn = boto.s3.connection.S3Connection(**kwargs)
    return conn

def setup():
    global s3, config, prefix
    s3.clear()
    config.clear()

    try:
        path = os.environ['S3TEST_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable S3TEST_CONF to a config file.',
            )
    with file(path) as f:
        config.update(read_config(f))

    # These 3 should always be present.
    if 's3' not in config:
        raise RuntimeError('Your config file is missing the s3 section!')
    if 'defaults' not in config.s3:
        raise RuntimeError('Your config file is missing the s3.defaults section!')
    if 'fixtures' not in config:
        raise RuntimeError('Your config file is missing the fixtures section!')

    template = config.fixtures.get('bucket prefix', 'test-{random}-')
    prefix = choose_bucket_prefix(template=template)
    if prefix == '':
        raise RuntimeError("Empty Prefix! Aborting!")

    defaults = config.s3.defaults
    for section in list(config.s3.keys()):
        if section == 'defaults':
            continue

        conf = {}
        conf.update(defaults)
        conf.update(config.s3[section])
        conn = connect(conf)
        s3[section] = conn

    # WARNING! we actively delete all buckets we see with the prefix
    # we've chosen! Choose your prefix with care, and don't reuse
    # credentials!

    # We also assume nobody else is going to use buckets with that
    # prefix. This is racy but given enough randomness, should not
    # really fail.
    nuke_prefixed_buckets()

def get_new_bucket(connection=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    if connection is None:
        connection = s3.main
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    # the only way for this to fail with a pre-existing bucket is if
    # someone raced us between setup nuke_prefixed_buckets and here;
    # ignore that as astronomically unlikely
    bucket = connection.create_bucket(name)
    return bucket

def teardown():
    nuke_prefixed_buckets()

def with_setup_kwargs(setup, teardown=None):
    """Decorator to add setup and/or teardown methods to a test function::

      @with_setup_args(setup, teardown)
      def test_something():
          " ... "

    The setup function should return (kwargs) which will be passed to
    test function, and teardown function.

    Note that `with_setup_kwargs` is useful *only* for test functions, not for test
    methods or inside of TestCase subclasses.
    """
    def decorate(func):
        kwargs = {}

        def test_wrapped(*args, **kwargs2):
            k2 = kwargs.copy()
            k2.update(kwargs2)
            k2['testname'] = func.__name__
            func(*args, **k2)

        test_wrapped.__name__ = func.__name__

        def setup_wrapped():
            k = setup()
            kwargs.update(k)
            if hasattr(func, 'setup'):
                func.setup()
        test_wrapped.setup = setup_wrapped

        if teardown:
            def teardown_wrapped():
                if hasattr(func, 'teardown'):
                    func.teardown()
                teardown(**kwargs)

            test_wrapped.teardown = teardown_wrapped
        else:
            if hasattr(func, 'teardown'):
                test_wrapped.teardown = func.teardown()
        return test_wrapped
    return decorate

# Demo case for the above, when you run test_gen():
# _test_gen will run twice,
# with the following stderr printing
# setup_func {'b': 2}
# testcase ('1',) {'b': 2, 'testname': '_test_gen'}
# teardown_func {'b': 2}
# setup_func {'b': 2}
# testcase () {'b': 2, 'testname': '_test_gen'}
# teardown_func {'b': 2}
# 
#def setup_func():
#    kwargs = {'b': 2}
#    print("setup_func", kwargs, file=sys.stderr)
#    return kwargs
#
#def teardown_func(**kwargs):
#    print("teardown_func", kwargs, file=sys.stderr)
#
#@with_setup_kwargs(setup=setup_func, teardown=teardown_func)
#def _test_gen(*args, **kwargs):
#    print("testcase", args, kwargs, file=sys.stderr)
#
#def test_gen():
#    yield _test_gen, '1'
#    yield _test_gen

def trim_xml(xml_str):
    p = etree.XMLParser(encoding="utf-8", remove_blank_text=True)
    xml_str = bytes(xml_str, "utf-8")
    elem = etree.XML(xml_str, parser=p)
    return etree.tostring(elem, encoding="unicode")

def normalize_xml(xml, pretty_print=True):
    if xml is None:
        return xml

    root = etree.fromstring(xml.encode(encoding='ascii'))

    for element in root.iter('*'):
        if element.text is not None and not element.text.strip():
            element.text = None
        if element.text is not None:
            element.text = element.text.strip().replace("\n", "").replace("\r", "")
        if element.tail is not None and not element.tail.strip():
            element.tail = None
        if element.tail is not None:
            element.tail = element.tail.strip().replace("\n", "").replace("\r", "")

    # Sort the elements
    for parent in root.xpath('//*[./*]'): # Search for parent elements
          parent[:] = sorted(parent,key=lambda x: x.tag)

    xmlstr = etree.tostring(root, encoding="unicode", pretty_print=pretty_print)
    # there are two different DTD URIs
    xmlstr = re.sub(r'xmlns="[^"]+"', 'xmlns="s3"', xmlstr)
    xmlstr = re.sub(r'xmlns=\'[^\']+\'', 'xmlns="s3"', xmlstr)
    for uri in ['http://doc.s3.amazonaws.com/doc/2006-03-01/', 'http://s3.amazonaws.com/doc/2006-03-01/']:
        xmlstr = xmlstr.replace(uri, 'URI-DTD')
    #xmlstr = re.sub(r'>\s+', '>', xmlstr, count=0, flags=re.MULTILINE)
    return xmlstr

def assert_xml_equal(got, want):
    assert want is not None, 'Wanted XML cannot be None'
    if got is None:
        raise AssertionError('Got input to validate was None')
    checker = LXMLOutputChecker()
    if not checker.check_output(want, got, 0):
        message = checker.output_difference(Example("", want), got, 0)
        raise AssertionError(message)
