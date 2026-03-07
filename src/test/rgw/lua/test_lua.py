import logging
import json
import tempfile
import random
import socket
import time
import threading
import subprocess
import os
import stat
import string
import pytest
import boto3

from . import(
    configfile,
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key
    )

# configure logging for the tests module
log = logging.getLogger(__name__)

num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

def bash(cmd, **kwargs):
    log.debug('running command: %s', ' '.join(cmd))
    kwargs['stdout'] = subprocess.PIPE
    kwargs["stderr"] = subprocess.PIPE
    kwargs["text"] = True
    process = subprocess.Popen(cmd, **kwargs)
    stdout, stderr = process.communicate()
    return (stdout, stderr, process.returncode)


def admin(args, **kwargs):
    """ radosgw-admin command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', 'noname'] + args
    return bash(cmd, **kwargs)


def delete_all_objects(conn, bucket_name):
    objects = []
    for key in conn.list_objects(Bucket=bucket_name)['Contents']:
        objects.append({'Key': key['Key']})
    # delete objects from the bucket
    response = conn.delete_objects(Bucket=bucket_name,
            Delete={'Objects': objects})


def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return run_prefix + '-' + str(num_buckets)


def get_ip():
    return 'localhost'


def get_ip_http():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # address should not be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def connection():
    hostname = get_config_host()
    port_no = get_config_port()
    access_key = get_access_key()
    secret_key = get_secret_key()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    client = boto3.client('s3',
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

    return client


def another_user(tenant=None):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = 'superman' + str(time.time())
    if tenant:
        _, _, result = admin(['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])
    else:
        _, _, result = admin(['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])

    assert result == 0
    hostname = get_config_host()
    port_no = get_config_port()
    if port_no == 443 or port_no == 8443:
        scheme = 'https://'
    else:
        scheme = 'http://'

    client = boto3.client('s3',
            endpoint_url=scheme+hostname+':'+str(port_no),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

    return client


def put_script(script, context, tenant=None, script_name=None):
    fp = tempfile.NamedTemporaryFile(mode='w+')
    fp.write(script)
    fp.flush()
    
    args = ['script', 'put', '--infile', fp.name, '--context', context]
    if tenant:
        args.append('--tenant')
        args.append(tenant)
    if script_name:
        args.append('--script-name')
        args.append(script_name)

    result = admin(args)
    fp.close()
    return result

class UnixSocket:
    def __init__(self, socket_path):
        self.socket_path = socket_path
        self.stop = False
        self.started = False
        self.events = []
        self.t = threading.Thread(target=self.listen_on_socket)
        self.t.start()
        while not self.started:
            print("UnixSocket: waiting for unix socket server to start")
            time.sleep(1)

    def shutdown(self):
        self.stop = True
        self.t.join()

    def get_and_reset_events(self):
        tmp = self.events
        self.events = []
        return tmp

    def listen_on_socket(self):
        self.started = True
        # remove the socket file if it already exists
        try:
            os.unlink(self.socket_path)
        except OSError:
            if os.path.exists(self.socket_path):
                raise

        # create and bind the Unix socket server
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.socket_path)

        # give permissions for anyone to write to it
        os.chmod(self.socket_path, stat.S_IWOTH|stat.S_IWGRP|stat.S_IWUSR)

        # listen for incoming connections
        server.listen(1)
        # accept timeout is 30s at the beginning
        server.settimeout(30)
        print("UnixSocket '%s' is listening for incoming connections..." % self.socket_path)

        while not self.stop:
            # accept connections
            try:
                connection, _ = server.accept()
            except Exception as e:
                print("UnixSocket: accept "+str(e))
                continue
            # after we start accept/recv timeouts are 5s
            server.settimeout(5)
            connection.settimeout(5)

            try:
                print("UnixSocket: new connection accepted")
                # receive data from the client
                while True:
                    # recv timeout is 5s
                    data = connection.recv(1024)
                    if not data:
                        break
                    event = json.loads(data.decode())
                    self.events.append(event)
            finally:
                # close the connection
                connection.close()
                print("UnixSocket: connection closed")

        # remove the socket file
        os.unlink(self.socket_path)


#####################
# lua scripting tests
#####################


@pytest.mark.basic_test
def test_script_management():
    contexts = ['prerequest', 'postauth', 'postrequest', 'background', 'getdata', 'putdata']
    scripts = {}
    for context in contexts:
        script = 'print("hello from ' + context + '")'
        result = put_script(script, context)
        assert result[2] == 0
        scripts[context] = script
    for context in contexts:
        result = admin(['script', 'get', '--context', context])
        assert result[2] == 0
        assert result[0].strip() == scripts[context]
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert result[2] == 0
    for context in contexts:
        result = admin(['script', 'get', '--context', context])
        assert result[2] == 0
        assert result[0].strip() == 'no script exists for context: ' + context

@pytest.mark.basic_test
def test_multi_script_unsupported():
    contexts = ['getdata', 'putdata']
    for context in contexts:
        result = put_script('print("hello")', context, None, 'test')
        assert result[2] != 0
        assert f"ERROR: cannot create more than one Lua script in the {context} context; remove the --script-name flag" in result[1]

@pytest.mark.basic_test
def test_multi_script_management():
    contexts = ['prerequest', 'postauth', 'postrequest', 'background'] # getdata and putdata contexts cannot use --script-name
    script_names = ["c", "b", "a"]
    scripts = {}
    for context in contexts:
        for script_name in script_names:
            script = 'print("hello from ' + '{} ({})'.format(context, script_name) + ' script")'
            result = put_script(script, context, None, script_name)
            assert result[2] == 0
            scripts.setdefault(context, {})[script_name] = script
    for context in contexts:
        for script_name in script_names:
            result = admin(['script', 'get', '--context', context, '--script-name', script_name])
            assert result[2] == 0
            assert result[0].strip() == scripts[context][script_name]
    for context in contexts:
         result = admin(['script', 'list', '--context', context])
         assert result[2] == 0
         assert result[0].strip() == "a\nb\nc"
    # remove one script and verify the others remain in sorted order
    for context in contexts:
        result = admin(['script', 'rm', '--context', context, '--script-name', 'b'])
        assert result[2] == 0
    for context in contexts:
        result = admin(['script', 'get', '--context', context, '--script-name', 'a'])
        assert result[2] == 0
        assert result[0].strip() == scripts[context]['a']
        result = admin(['script', 'get', '--context', context, '--script-name', 'c'])
        assert result[2] == 0
        assert result[0].strip() == scripts[context]['c']
        result = admin(['script', 'list', '--context', context])
        assert result[2] == 0
        assert result[0].strip() == "a\nc"
    for context in contexts:
        for script_name in ['c', 'a']:
            result = admin(['script', 'rm', '--context', context, '--script-name', script_name])
            assert result[2] == 0
    for context in contexts:
        for script_name in script_names:
            result = admin(['script', 'get', '--context', context, '--script-name', script_name])
            assert result[2] == 0
            assert result[0].strip() == "'{}' script does not exist in context: ".format(script_name) + context

@pytest.mark.basic_test
def test_script_management_with_tenant():
    tenant = 'mytenant'
    conn2 = another_user(tenant)
    contexts = ['prerequest', 'postauth', 'postrequest', 'getdata', 'putdata']
    scripts = {}
    for context in contexts:
        for t in ['', tenant]:
            script = 'print("hello from ' + context + ' and ' + tenant + '")'
            result = put_script(script, context, t)
            assert result[2] == 0
            scripts[context+t] = script
    for context in contexts:
        result = admin(['script', 'get', '--context', context])
        assert result[2] == 0
        assert result[0].strip(), scripts[context]
        result = admin(['script', 'rm', '--context', context])
        assert result[2] == 0
        result = admin(['script', 'get', '--context', context])
        assert result[2] == 0
        assert result[0].strip(), 'no script exists for context: ' + context
        result = admin(['script', 'get', '--context', context, '--tenant', tenant])
        assert result[2] == 0
        assert result[0].strip(), scripts[context+tenant]
        result = admin(['script', 'rm', '--context', context, '--tenant', tenant])
        assert result[2] == 0
        result = admin(['script', 'get', '--context', context, '--tenant', tenant])
        assert result[2] == 0
        assert result[0].strip(), 'no script exists for context: ' + context + ' in tenant: ' + tenant

@pytest.mark.basic_test
def test_multi_script_management_with_tenant():
    tenant = 'mytenant'
    conn2 = another_user(tenant)
    contexts = ['prerequest', 'postauth', 'postrequest'] # background context cannot use a tenant, getdata and putdata contexts cannot use --script-name
    script_names = ["c", "b", "a"]
    scripts = {}
    for context in contexts:
        for t in ['', tenant]:
            for script_name in script_names:
                script = 'print("hello from ' + '{} ({})'.format(context, script_name) + ' and ' + tenant + '")'
                result = put_script(script, context, t, script_name)
                assert result[2] == 0
                scripts.setdefault(context+t, {})[script_name] = script
    for context in contexts:
        for script_name in script_names:
            result = admin(['script', 'get', '--context', context, '--script-name', script_name])
            assert result[2] == 0
            assert result[0].strip(), scripts[context][script_name]
            result = admin(['script', 'rm', '--context', context, '--script-name', script_name])
            assert result[2] == 0
            result = admin(['script', 'get', '--context', context, '--script-name', script_name])
            assert result[2] == 0
            assert result[0].strip(), "'{}' script does not exist in context: ".format(script_name) + context
            result = admin(['script', 'get', '--context', context, '--tenant', tenant, '--script-name', script_name])
            assert result[2] == 0
            assert result[0].strip(), scripts[context+tenant][script_name]
            result = admin(['script', 'rm', '--context', context, '--tenant', tenant, '--script-name', script_name])
            assert result[2] == 0
            result = admin(['script', 'get', '--context', context, '--tenant', tenant, '--script-name', script_name])
            assert result[2] == 0
            assert result[0].strip(), "'{}' script does not exist in context: ".format(script_name) + context + ' in tenant: ' + tenant

@pytest.mark.request_test
def test_put_obj():
    script = '''
RGWDebugLog("op was: "..Request.RGWOp)
if Request.RGWOp == "put_obj" then
    local object = Request.Object
    local message = Request.bucket.Name .. "," .. object.Name .. 
        "," .. object.Id .. "," .. object.Size .. "," .. object.MTime
    RGWDebugLog("set: x-amz-meta-test to: " .. message)
    Request.HTTP.Metadata["x-amz-meta-test"] = message
end
'''
    context = "prerequest"
    result = put_script(script, context)
    assert result[2] == 0
	
    conn = connection()
    bucket_name = gen_bucket_name()
    conn.create_bucket(Bucket=bucket_name)
    key = "hello"
    conn.put_object(Body="1234567890".encode("ascii"), Bucket=bucket_name, Key=key)

    result = conn.get_object(Bucket=bucket_name, Key=key)
    message = result['ResponseMetadata']['HTTPHeaders']['x-amz-meta-test']

    # The MTime part is supposed to be a datetime "1970-01-01 00:00:00"
    # however in different TZ environment the hours can be different,
    # for example, for UTC+1, zero date turns into "1970-01-01 01:00:00",
    # while for UTC-1, it is "1969-12-31 23:00:00", so we only check
    # if the date corresponds to either of those two days.
    assert ',' in message
    (message_without_mtime, message_mtime) = message.rsplit(',', 1)
    assert message_without_mtime == f"{bucket_name},{key},{key},0"
    assert message_mtime[:10] in ['1969-12-31', '1970-01-01']

    # cleanup
    conn.delete_object(Bucket=bucket_name, Key=key)
    conn.delete_bucket(Bucket=bucket_name)
    contexts = ['prerequest', 'postauth', 'postrequest', 'getdata', 'putdata']
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert result[2] == 0


@pytest.mark.example_test
def test_copyfrom():
    script = '''
function print_object(object)
    RGWDebugLog("  Name: " .. object.Name)
    RGWDebugLog("  Instance: " .. object.Instance)
    RGWDebugLog("  Id: " .. object.Id)
    RGWDebugLog("  Size: " .. object.Size)
    RGWDebugLog("  MTime: " .. object.MTime)
end

if Request.CopyFrom and Request.Object and Request.CopyFrom.Object then
    RGWDebugLog("copy from object:")
    print_object(Request.CopyFrom.Object)
    RGWDebugLog("to object:")
    print_object(Request.Object)
end
RGWDebugLog("op was: "..Request.RGWOp)
'''

    contexts = ['prerequest', 'postauth', 'postrequest', 'getdata', 'putdata']
    for context in contexts:
        footer = '\nRGWDebugLog("context was: '+context+'\\n\\n")'
        result = put_script(script+footer, context)
        assert result[2] == 0
	
    conn = connection()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(Bucket=bucket_name)
    # create objects in the bucket
    number_of_objects = 5
    for i in range(number_of_objects):
        content = str(os.urandom(1024*1024)).encode("ascii")
        key = str(i)
        conn.put_object(Body=content, Bucket=bucket_name, Key=key)

    for i in range(number_of_objects):
        key = str(i)
        conn.copy_object(Bucket=bucket_name,
                Key='copyof'+key, 
                CopySource=bucket_name+'/'+key)

    # cleanup
    delete_all_objects(conn, bucket_name)
    conn.delete_bucket(Bucket=bucket_name)
    contexts = ['prerequest', 'postauth', 'postrequest', 'getdata', 'putdata']
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert result[2] == 0


@pytest.mark.example_test
def test_entropy():
    script = '''
function object_entropy()
    local byte_hist = {}
    local byte_hist_size = 256
    for i = 1,byte_hist_size do
        byte_hist[i] = 0
    end
    local total = 0

    for i, c in pairs(Data)  do
        local byte = c:byte() + 1
        byte_hist[byte] = byte_hist[byte] + 1
        total = total + 1
    end

    entropy = 0

    for _, count in ipairs(byte_hist) do
        if count ~= 0 then
            local p = 1.0 * count / total
            entropy = entropy - (p * math.log(p)/math.log(byte_hist_size))
        end
    end

    return entropy
end

local full_name = Request.Bucket.Name.."-"..Request.Object.Name
RGWDebugLog("entropy of chunk of: " .. full_name .. " at offset: " .. tostring(Offset)  ..  " is: " .. tostring(object_entropy()))
RGWDebugLog("payload size of chunk of: " .. full_name .. " is: " .. #Data)
'''

    result = put_script(script, "putdata")
    assert result[2] == 0

    conn = connection()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(Bucket=bucket_name)
    # create objects in the bucket (async)
    number_of_objects = 5
    for i in range(number_of_objects):
        content = str(os.urandom(1024*1024*16)).encode("ascii")
        key = str(i)
        conn.put_object(Body=content, Bucket=bucket_name, Key=key)

    # cleanup
    delete_all_objects(conn, bucket_name)
    conn.delete_bucket(Bucket=bucket_name)
    contexts = ['prerequest', 'postauth', 'postrequest', 'background', 'getdata', 'putdata']
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert result[2] == 0


@pytest.mark.example_test
def test_access_log():
    bucket_name = gen_bucket_name()
    socket_path = '/tmp/'+bucket_name

    script = '''
if Request.RGWOp == "get_obj" then
    local json = require("cjson")
    local socket = require("socket")
    local unix = require("socket.unix")
    local s = unix()
    E = {{}}

    msg = {{bucket = (Request.Bucket or (Request.CopyFrom or E).Bucket).Name,
        object = Request.Object.Name,
        time = Request.Time,
        operation = Request.RGWOp,
        http_status = Request.Response.HTTPStatusCode,
        error_code = Request.Response.HTTPStatus,
        object_size = Request.Object.Size,
        trans_id = Request.TransactionId}}
    assert(s:connect("{}"))
    s:send(json.encode(msg).."\\n")
    s:close()
end
'''.format(socket_path)

    result = admin(['script-package', 'add', '--package=lua-cjson', '--allow-compilation'])
    assert result[2] == 0
    result = admin(['script-package', 'add', '--package=luasocket', '--allow-compilation'])
    assert result[2] == 0
    result = admin(['script-package', 'reload'])
    assert result[2] == 0
    result = put_script(script, "postrequest")
    assert result[2] == 0

    socket_server = UnixSocket(socket_path)
    try:
        conn = connection()
        # create bucket
        bucket = conn.create_bucket(Bucket=bucket_name)
        # create objects in the bucket (async)
        number_of_objects = 5
        keys = []
        for i in range(number_of_objects):
            content = str(os.urandom(1024*1024)).encode("ascii")
            key = str(i)
            conn.put_object(Body=content, Bucket=bucket_name, Key=key)
            keys.append(key)

        for key in conn.list_objects(Bucket=bucket_name)['Contents']:
            conn.get_object(Bucket=bucket_name, Key=key['Key'])

        time.sleep(5)
        event_keys = []
        for event in socket_server.get_and_reset_events():
            assert event['bucket'] == bucket_name
            event_keys.append(event['object'])

        assert keys == event_keys

    finally:
        socket_server.shutdown()
        delete_all_objects(conn, bucket_name)
        conn.delete_bucket(Bucket=bucket_name)
        contexts = ['prerequest', 'postauth', 'postrequest', 'background', 'getdata', 'putdata']
        for context in contexts:
            result = admin(['script', 'rm', '--context', context])
            assert result[2] == 0

@pytest.mark.example_test
def test_multi_access_log():
    bucket_name = gen_bucket_name()
    socket_path = '/tmp/'+bucket_name

    result = admin(['script-package', 'add', '--package=lua-cjson', '--allow-compilation'])
    assert result[2] == 0
    result = admin(['script-package', 'add', '--package=luasocket', '--allow-compilation'])
    assert result[2] == 0
    result = admin(['script-package', 'reload'])
    assert result[2] == 0

    script_names = ["b", "a"]
    for name in script_names:
        delay = 0.5*script_names.index(name)
        script = '''
if Request.RGWOp == "get_obj" then
    local json = require("cjson")
    local socket = require("socket")
    local unix = require("socket.unix")
    local s = unix()
    E = {{}}

    -- sleep 0.5s for a and 0s for b
    -- we should expect script a to execute first despite taking longer to connect to the socket
    {}

    msg = {{bucket = (Request.Bucket or (Request.CopyFrom or E).Bucket).Name,
        object = Request.Object.Name,
        time = Request.Time,
        script_name = "{}",
        operation = Request.RGWOp,
        http_status = Request.Response.HTTPStatusCode,
        error_code = Request.Response.HTTPStatus,
        object_size = Request.Object.Size,
        trans_id = Request.TransactionId}}
    assert(s:connect("{}"))
    s:send(json.encode(msg).."\\n")
    s:close()
end
    '''.format("" if delay == 0 else "socket.sleep({})".format(delay), name, socket_path)
        result = put_script(script, "postrequest", None, name)
        assert result[2] == 0

    socket_server = UnixSocket(socket_path)
    try:
        conn = connection()
        # create bucket
        bucket = conn.create_bucket(Bucket=bucket_name)
        # create objects in the bucket (async)
        number_of_objects = 3
        keys = []
        for i in range(number_of_objects):
            content = str(os.urandom(1024)).encode("ascii")
            key = str(i)
            conn.put_object(Body=content, Bucket=bucket_name, Key=key)
            keys.append(key)

        for key in conn.list_objects(Bucket=bucket_name)['Contents']:
            conn.get_object(Bucket=bucket_name, Key=key['Key'])

        time.sleep(3)
        events = {}
        for event in socket_server.get_and_reset_events():
            assert event['bucket'] == bucket_name
            events[event['object']] = events.get(event['object'], []) + [event['script_name']]

        assert len(events) == number_of_objects
        sorted_script_names = sorted(script_names)
        for obj in events:
            assert sorted_script_names == events[obj]

    finally:
        socket_server.shutdown()
        delete_all_objects(conn, bucket_name)
        conn.delete_bucket(Bucket=bucket_name)
        contexts = ['prerequest', 'postrequest', 'background', 'getdata', 'putdata']
        for context in contexts:
            result = admin(['script', 'rm', '--context', context])
            assert result[2] == 0

@pytest.mark.example_test
def test_interrupt_request():
    script = '''
        return RGW_ABORT_REQUEST
    '''

    conn = connection()
    bucket_name = gen_bucket_name()
    conn.create_bucket(Bucket=bucket_name)
    
    result = put_script(script, "prerequest")
    assert result[2] == 0

    # sleep to allow lua_bytecode_cache to sync
    time.sleep(5)

    key = "hello"
    try:
        conn.put_object(Body="this should be blocked".encode("ascii"), Bucket=bucket_name, Key=key)
        pytest.fail("The put_object operation was not blocked by the Lua script.")
    except Exception as e:
        pass

    out, _, err = admin(['script', 'rm', '--context', 'prerequest'])
    assert err == 0

    try:
        conn.get_object(Bucket=bucket_name, Key=key)
        pytest.fail("The object was written to the bucket despite the error.")
    except Exception as e:
        assert e.response['Error']['Code'] == 'NoSuchKey'
        log.info("Successfully confirmed that the request was interrupted.")

    conn.delete_bucket(Bucket=bucket_name)

@pytest.mark.example_test
def test_interrupt_request_postauth():
    script = '''
        return RGW_ABORT_REQUEST
    '''

    conn = connection()
    bucket_name = gen_bucket_name()
    conn.create_bucket(Bucket=bucket_name)

    result = put_script(script, "postauth")
    assert result[2] == 0
    key = "hello"

    # sleep to allow lua_bytecode_cache to sync
    time.sleep(5)

    try:
        conn.put_object(Body="this should be blocked".encode("ascii"), Bucket=bucket_name, Key=key)
        pytest.fail("The put_object operation was not blocked by the Lua script.")
    except Exception as e:
        pass

    out, _, err = admin(['script', 'rm', '--context', 'postauth'])
    assert err == 0

    try:
        conn.get_object(Bucket=bucket_name, Key=key)
        pytest.fail("The object was written to the bucket despite the error.")
    except Exception as e:
        assert e.response['Error']['Code'] == 'NoSuchKey'
        log.info("Successfully confirmed that the request was interrupted.")

    conn.delete_bucket(Bucket=bucket_name)

@pytest.mark.example_test
def test_cache_invalidation():
    # This test covers possible race conditions in the lua bytecode cache in the
    # background thread.
    #
    # When a script is added via put_script(), its script OID is pushed onto the
    # processing_q and the background thread eventually consumes one or more script
    # OIDs to update the in-memory cache.
    #
    # If you attempt to override a script by calling put_script() a second time,
    # a subsequent call to execute() right after the second put_script() can race
    # with the background thread's script OID update, leading to sometimes reusing
    # the old bytecode cache.
    #
    # In the example test below, we first deploy a script that returns 0 status code.
    # If we quickly overwrite with a new script that we expect to return -EPERM and it
    # fails to assert the -EPERM then it implies the old bytecode cache is still being
    # read.
    conn = connection()
    bucket_name = gen_bucket_name()
    conn.create_bucket(Bucket=bucket_name)
    contexts = ['prerequest']
    for context in contexts:
        # SCRIPT 1: run the test_put_obj first but do not remove the script
        script = '''
    RGWDebugLog("op was: "..Request.RGWOp)
    if Request.RGWOp == "put_obj" then
        local object = Request.Object
        local message = Request.bucket.Name .. "," .. object.Name ..
            "," .. object.Id .. "," .. object.Size .. "," .. object.MTime
        RGWDebugLog("set: x-amz-meta-test to: " .. message)
        Request.HTTP.Metadata["x-amz-meta-test"] = message
    end
    '''
        result = put_script(script, context)
        assert result[2] == 0

        key = f"hello-{context}"
        conn.put_object(Body=f"this should not be blocked in the {context} context".encode("ascii"), Bucket=bucket_name, Key=key)

        # sleep to allow lua_bytecode_cache to sync
        time.sleep(5)

        result = conn.get_object(Bucket=bucket_name, Key=key)
        message = result['ResponseMetadata']['HTTPHeaders']['x-amz-meta-test']

        # The MTime part is supposed to be a datetime "1970-01-01 00:00:00"
        # however in different TZ environment the hours can be different,
        # for example, for UTC+1, zero date turns into "1970-01-01 01:00:00",
        # while for UTC-1, it is "1969-12-31 23:00:00", so we only check
        # if the date corresponds to either of those two days.
        assert ',' in message
        (message_without_mtime, message_mtime) = message.rsplit(',', 1)
        assert message_without_mtime == f"{bucket_name},{key},{key},0"
        assert message_mtime[:10] in ['1969-12-31', '1970-01-01']

        # SCRIPT 2: immediately create a script that forces an abort
        script = '''
            return RGW_ABORT_REQUEST
        '''

        result = put_script(script, context)
        assert result[2] == 0

        # sleep to allow lua_bytecode_cache to sync
        time.sleep(5)

        # If there is a caching/race/timing issue, SCRIPT 1 may be executed here instead of SCRIPT 2.
        try:
            conn.put_object(Body=f"this should be blocked in the {context} context".encode("ascii"), Bucket=bucket_name, Key=key)

            pytest.fail(f"The put_object operation was not blocked by the Lua script in the {context} context.")
        except Exception as e:
            pass

        out, _, err = admin(['script', 'rm', '--context', context])
        assert err == 0

    delete_all_objects(conn, bucket_name)
    conn.delete_bucket(Bucket=bucket_name)