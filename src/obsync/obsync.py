#!/usr/bin/env python

#
# Ceph - scalable distributed file system
#
# Copyright (C) 2011 New Dream Network
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#

"""
obsync.py: the object synchronizer
"""

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from optparse import OptionParser
from sys import stderr
from lxml import etree
import base64
import boto
import errno
import hashlib
import mimetypes
import os
from StringIO import StringIO
import rados
import re
import shutil
import string
import sys
import tempfile
import traceback

global opts
global xuser

###### Constants classes #######
RGW_META_BUCKET_NAME = ".rgw"

###### Exception classes #######
class LocalFileIsAcl(Exception):
    pass

class InvalidLocalName(Exception):
    pass

class NonexistentStore(Exception):
    pass

###### Helper functions #######
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError, exc:
        if exc.errno != errno.EEXIST:
            raise
        if (not os.path.isdir(path)):
            raise

def bytes_to_str(b):
    return ''.join(["%02x"% ord(x) for x in b]).strip()

def get_md5(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return "%s" % md5.hexdigest()

def strip_prefix(prefix, s):
    if not (s[0:len(prefix)] == prefix):
        return None
    return s[len(prefix):]

def etag_to_md5(etag):
    if (etag[:1] == '"'):
        start = 1
    else:
        start = 0
    if (etag[-1:] == '"'):
        end = -1
    else:
        end = None
    return etag[start:end]

def getenv(a, b):
    if os.environ.has_key(a):
        return os.environ[a]
    elif b and os.environ.has_key(b):
        return os.environ[b]
    else:
        return None

# Escaping functions.
#
# Valid names for local files are a little different than valid object
# names for S3. So these functions are needed to translate.
#
# Basically, in local names, every sequence starting with a dollar sign is
# reserved as a special escape sequence. If you want to create an S3 object
# with a dollar sign in the name, the local file should have a double dollar
# sign ($$).
#
# TODO: translate local files' control characters into escape sequences.
# Most S3 clients (boto included) cannot handle control characters in S3 object
# names.
# TODO: check for invalid utf-8 in local file names. Ideally, escape it, but
# if not, just reject the local file name. S3 object names must be valid
# utf-8.
#
# ----------		-----------
# In S3				Locally
# ----------		-----------
# foo/				foo$slash
#
# $money			$$money
#
# obj-with-acl		obj-with-acl
#					.obj-with-acl$acl
def s3_name_to_local_name(s3_name):
    s3_name = re.sub(r'\$', "$$", s3_name)
    if (s3_name[-1:] == "/"):
        s3_name = s3_name[:-1] + "$slash"
    return s3_name

def local_name_to_s3_name(local_name):
    if local_name.find(r'$acl') != -1:
        raise LocalFileIsAcl()
    local_name = re.sub(r'\$slash', "/", local_name)
    mre = re.compile("[$][^$]")
    if mre.match(local_name):
        raise InvalidLocalName("Local name contains a dollar sign escape \
sequence we don't understand.")
    local_name = re.sub(r'\$\$', "$", local_name)
    return local_name

def get_local_acl_file_name(local_name):
    if local_name.find(r'$acl') != -1:
        raise LocalFileIsAcl()
    return os.path.dirname(local_name) + "/." + \
        os.path.basename(local_name) + "$acl"

###### ACLs #######

# for buckets: allow list
# for object: allow grantee to read object data and metadata
READ = 1

# for buckets: allow create, overwrite, or deletion of any object in the bucket
WRITE = 2

# for buckets: allow grantee to read the bucket ACL
# for objects: allow grantee to read the object ACL
READ_ACP = 4

# for buckets: allow grantee to write the bucket ACL
# for objects: allow grantee to write the object ACL
WRITE_ACP = 8

# all of the above
FULL_CONTROL = READ | WRITE | READ_ACP | WRITE_ACP

ACL_TYPE_CANON_USER = "canon:"
ACL_TYPE_EMAIL_USER = "email:"
ACL_TYPE_GROUP = "group:"
ALL_ACL_TYPES = [ ACL_TYPE_CANON_USER, ACL_TYPE_EMAIL_USER, ACL_TYPE_GROUP ]

S3_GROUP_AUTH_USERS = ACL_TYPE_GROUP +  "AuthenticatedUsers"
S3_GROUP_ALL_USERS = ACL_TYPE_GROUP +  "AllUsers"
S3_GROUP_LOG_DELIVERY = ACL_TYPE_GROUP +  "LogDelivery"

NS = "http://s3.amazonaws.com/doc/2006-03-01/"
NS2 = "http://www.w3.org/2001/XMLSchema-instance"

def get_user_type(utype):
    for ut in [ ACL_TYPE_CANON_USER, ACL_TYPE_EMAIL_USER, ACL_TYPE_GROUP ]:
        if utype[:len(ut)] == ut:
            return ut
    raise Exception("unknown user type for user %s" % utype)

def strip_user_type(utype):
    for ut in [ ACL_TYPE_CANON_USER, ACL_TYPE_EMAIL_USER, ACL_TYPE_GROUP ]:
        if utype[:len(ut)] == ut:
            return utype[len(ut):]
    raise Exception("unknown user type for user %s" % utype)

def grantee_attribute_to_user_type(utype):
    if (utype == "Canonical User"):
        return ACL_TYPE_CANON_USER
    elif (utype == "CanonicalUser"):
        return ACL_TYPE_CANON_USER
    elif (utype == "Group"):
        return ACL_TYPE_GROUP
    elif (utype == "Email User"):
        return ACL_TYPE_EMAIL_USER
    elif (utype == "EmailUser"):
        return ACL_TYPE_EMAIL_USER
    else:
        raise Exception("unknown user type for user %s" % utype)

def user_type_to_attr(t):
    if (t == ACL_TYPE_CANON_USER):
        return "CanonicalUser"
    elif (t == ACL_TYPE_GROUP):
        return "Group"
    elif (t ==  ACL_TYPE_EMAIL_USER):
        return "EmailUser"
    else:
        raise Exception("unknown user type %s" % t)

def add_user_type(user):
    """ All users that are not specifically marked as something else
are treated as canonical users"""
    for atype in ALL_ACL_TYPES:
        if (user[:len(atype)] == atype):
            return user
    return ACL_TYPE_CANON_USER + user

class AclGrant(object):
    def __init__(self, user_id, display_name, permission):
        self.user_id = user_id
        self.display_name = display_name
        self.permission = permission
    def translate_users(self, xusers):
        # Keep in mind that xusers contains user_ids of the form "type:value"
        # So typical contents might be like { canon:XYZ => canon.123 }
        if (xusers.has_key(self.user_id)):
            self.user_id = xusers[self.user_id]
            # It's not clear what the new pretty-name should be, so just leave it blank.
            self.display_name = ""

class AclPolicy(object):
    def __init__(self, owner_id, owner_display_name, grants):
        self.owner_id = owner_id
        self.owner_display_name = owner_display_name
        self.grants = grants  # list of ACLGrant
    @staticmethod
    def from_xml(s):
        root = etree.parse(StringIO(s))
        owner_id = root.find("{%s}Owner/{%s}ID" % (NS,NS)).text
        owner_display_name = root.find("{%s}Owner/{%s}DisplayName" \
            % (NS,NS)).text
        grantlist = root.findall("{%s}AccessControlList/{%s}Grant" \
            % (NS,NS))
        grants = [ ]
        for g in grantlist:
            grantee = g.find("{%s}Grantee" % NS)
            user_id = grantee.find("{%s}ID" % NS).text
            user_type = grantee.attrib["{%s}type" % NS2]
            display_name = grantee.find("{%s}DisplayName" % NS).text
            permission = g.find("{%s}Permission" % NS).text
            g_user_id = grantee_attribute_to_user_type(user_type) + user_id
            grants.append(AclGrant(g_user_id, display_name, permission))
        return AclPolicy(owner_id, owner_display_name, grants)
    def to_xml(self, omit_owner):
        root = etree.Element("AccessControlPolicy", nsmap={None: NS})
        if (not omit_owner):
            owner = etree.SubElement(root, "Owner")
            id_elem = etree.SubElement(owner, "ID")
            id_elem.text = self.owner_id
            display_name_elem = etree.SubElement(owner, "DisplayName")
            display_name_elem.text = self.owner_display_name
        access_control_list = etree.SubElement(root, "AccessControlList")
        for g in self.grants:
            grant_elem = etree.SubElement(access_control_list, "Grant")
            grantee_elem = etree.SubElement(grant_elem, "{%s}Grantee" % NS,
                nsmap={None: NS, "xsi" : NS2})
            grantee_elem.set("type", user_type_to_attr(get_user_type(g.user_id)))
            user_id_elem = etree.SubElement(grantee_elem, "{%s}ID" % NS)
            user_id_elem.text = strip_user_type(g.user_id)
            display_name_elem = etree.SubElement(grantee_elem, "{%s}DisplayName" % NS)
            display_name_elem.text = g.display_name
            permission_elem = etree.SubElement(grant_elem, "{%s}Permission" % NS)
            permission_elem.text = g.permission
        return etree.tostring(root, encoding="UTF-8")
    def translate_users(self, xusers):
        # Translate the owner for consistency, although most of the time we
        # don't write out the owner to the ACL.
        # Owner ids are always expressed in terms of canonical user id
        if (xusers.has_key(ACL_TYPE_CANON_USER + self.owner_id)):
            self.owner_id = \
                strip_user_type(xusers[ACL_TYPE_CANON_USER + self.owner_id])
            self.owner_display_name = ""
        for g in self.grants:
            g.translate_users(xusers)


def compare_xml(xml1, xml2):
    tree1 = etree.parse(StringIO(xml1))
    out1 = etree.tostring(tree1, encoding="UTF-8")
    tree2 = etree.parse(StringIO(xml2))
    out2 = etree.tostring(tree2, encoding="UTF-8")
    out1 = out1.replace("xsi:type", "type")
    out2 = out2.replace("xsi:type", "type")
    if out1 != out2:
        print "out1 = %s" % out1
        print "out2 = %s" % out2
        raise Exception("compare xml failed")

#<?xml version="1.0" encoding="UTF-8"?>
def test_acl_policy():
    test1_xml = \
"<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" + \
"<Owner><ID>foo</ID><DisplayName>MrFoo</DisplayName></Owner><AccessControlList>" + \
"<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " + \
"xsi:type=\"CanonicalUser\"><ID>*** Owner-Canonical-User-ID ***</ID>" + \
"<DisplayName>display-name</DisplayName></Grantee>" + \
"<Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>"
    test1 = AclPolicy.from_xml(test1_xml)
    compare_xml(test1_xml, test1.to_xml(False))

###### Object #######
class Object(object):
    def __init__(self, name, md5, size):
        self.name = name
        self.md5 = md5
        self.size = int(size)
    def equals(self, rhs):
        if (self.name != rhs.name):
            return False
        if (self.md5 != rhs.md5):
            return False
        if (self.size != rhs.size):
            return False
        return True
    def local_name(self):
        return s3_name_to_local_name(self.name)
    @staticmethod
    def from_file(obj_name, path):
        f = open(path, 'r')
        try:
            md5 = get_md5(f)
        finally:
            f.close()
        size = os.path.getsize(path)
        #print "Object.from_file: path="+path+",md5=" + bytes_to_str(md5) +",size=" + str(size)
        return Object(obj_name, md5, size)

###### Store #######
class Store(object):
    @staticmethod
    def make_store(url, create, akey, skey):
        s3_url = strip_prefix("s3://", url)
        if (s3_url):
            return S3Store(s3_url, create, akey, skey)
        rados_url = strip_prefix("rados:", url)
        if (rados_url):
            return RadosStore(rados_url, create, akey, skey)
        file_url = strip_prefix("file://", url)
        if (file_url):
            return FileStore(file_url, create)
        if (url[0:1] == "/"):
            return FileStore(url, create)
        if (url[0:2] == "./"):
            return FileStore(url, create)
        raise Exception("Failed to find a prefix of s3://, file://, /, or ./ \
Cannot handle this URL.")
    def __init__(self, url):
        self.url = url

###### LocalCopy ######
class LocalCopy(object):
    def __init__(self, obj_name, path, path_is_temp):
        self.obj_name = obj_name
        self.path = path
        self.path_is_temp = path_is_temp
    def remove(self):
        if ((self.path_is_temp == True) and (self.path != None)):
            os.unlink(self.path)
        self.path = None
        self.path_is_temp = False
    def __del__(self):
        self.remove()

class LocalAcl(object):
    def __init__(self, obj_name):
        self.obj_name = obj_name
        self.acl_path = None
        self.acl_is_temp = False
    def set_acl_xml(self, acl_xml):
        self.remove()
        self.acl_is_temp = True
        self.acl_path = tempfile.NamedTemporaryFile(mode='w+b', delete=False).name
        temp_acl_file_f = open(self.acl_path, 'w')
        try:
            temp_acl_file_f.write(acl_xml)
        finally:
            temp_acl_file_f.close()
    def __del__(self):
        self.remove()
    def remove(self):
        if ((self.acl_is_temp == True) and (self.acl_path != None)):
            os.unlink(self.acl_path)
        self.acl_path = None
        self.acl_is_temp = False
    def equals(self, rhs):
        """ Compare two cached ACL files """
        if (self.acl_path == None):
            return (rhs.acl_path == None)
        if (rhs.acl_path == None):
            return (self.acl_path == None)
        f = open(self.acl_path, 'r')
        try:
            my_xml = f.read()
        finally:
            f.close()
        f = open(rhs.acl_path, 'r')
        try:
            rhs_xml = f.read()
        finally:
            f.close()
        return my_xml == rhs_xml
    def translate_users(self, xusers):
        # Do we even have an ACL?
        if (self.acl_path == None):
            return
        # Read the XML and parse it.
        f = open(self.acl_path, 'r')
        try:
            acl_xml = f.read()
        finally:
            f.close()
        try:
            policy = AclPolicy.from_xml(acl_xml)
        except Exception, e:
            print >>stderr, "Error parsing ACL from object \"%s\"" % \
                self.obj_name
            raise
        policy.translate_users(xusers)
        acl_xml2 = policy.to_xml(True)
        new_acl_temp = tempfile.NamedTemporaryFile(mode='w+b', delete=False).name
        f = open(new_acl_temp, 'w')
        try:
            f.write(acl_xml2)
        finally:
            f.close()
        if (self.acl_is_temp):
            os.unlink(self.acl_path)
        self.acl_path = new_acl_temp
        self.acl_is_temp = True

###### S3 store #######
class S3StoreIterator(object):
    """S3Store iterator"""
    def __init__(self, blrs):
        self.blrs = blrs
    def __iter__(self):
        return self
    def next(self):
        # This will raise StopIteration when there are no more objects to
        # iterate on
        key = self.blrs.next()
        ret = Object(key.name, etag_to_md5(key.etag), key.size)
        return ret

class S3Store(Store):
    def __init__(self, url, create, akey, skey):
        # Parse the s3 url
        host_end = string.find(url, "/")
        if (host_end == -1):
            raise Exception("S3Store URLs are of the form \
s3://host/bucket/key_prefix. Failed to find the host.")
        self.host = url[0:host_end]
        bucket_end = url.find("/", host_end+1)
        if (bucket_end == -1):
            self.bucket_name = url[host_end+1:]
            self.key_prefix = ""
        else:
            self.bucket_name = url[host_end+1:bucket_end]
            self.key_prefix = url[bucket_end+1:]
        if (self.bucket_name == ""):
            raise Exception("S3Store URLs are of the form \
s3://host/bucket/key_prefix. Failed to find the bucket.")
        if (opts.more_verbose):
            print "self.host = '" + self.host + "', ",
            print "self.bucket_name = '" + self.bucket_name + "' ",
            print "self.key_prefix = '" + self.key_prefix + "'"
        self.conn = S3Connection(calling_format=OrdinaryCallingFormat(),
                host=self.host, is_secure=False,
                aws_access_key_id=akey, aws_secret_access_key=skey)
        self.bucket = self.conn.lookup(self.bucket_name)
        if (self.bucket == None):
            if (create):
                if (opts.dry_run):
                    raise Exception("logic error: this should be unreachable.")
                self.bucket = self.conn.create_bucket(bucket_name = self.bucket_name)
            else:
                raise RuntimeError("%s: no such bucket as %s" % \
                    (url, self.bucket_name))
        Store.__init__(self, "s3://" + url)
    def __str__(self):
        return "s3://" + self.host + "/" + self.bucket_name + "/" + self.key_prefix
    def get_acl(self, obj):
        acl = LocalAcl(obj.name)
        acl_xml = self.bucket.get_xml_acl(obj.name)
        acl.set_acl_xml(acl_xml)
        return acl
    def make_local_copy(self, obj):
        k = Key(self.bucket)
        k.key = obj.name
        temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False).name
        try:
            k.get_contents_to_filename(temp_file)
        except:
            os.unlink(temp_file)
            raise
        return LocalCopy(obj.name, temp_file, True)
    def all_objects(self):
        blrs = self.bucket.list(prefix = self.key_prefix)
        return S3StoreIterator(blrs.__iter__())
    def locate_object(self, obj):
        k = self.bucket.get_key(obj.name)
        if (k == None):
            return None
        return Object(obj.name, etag_to_md5(k.etag), k.size)
    def upload(self, local_copy, src_acl, obj):
        if (opts.more_verbose):
            print "S3Store.UPLOAD: local_copy.path='" + local_copy.path + "' " + \
                "obj='" + obj.name + "'"
        if (opts.dry_run):
            return
#        mime = mimetypes.guess_type(local_copy.path)[0]
#        if (mime == NoneType):
#            mime = "application/octet-stream"
        k = Key(self.bucket)
        k.key = obj.name
        #k.set_metadata("Content-Type", mime)
        k.set_contents_from_filename(local_copy.path)
        if (src_acl.acl_path != None):
            f = open(src_acl.acl_path, 'r')
            try:
                acl_xml = f.read()
            finally:
                f.close()
            self.bucket.set_acl(acl_xml, k)

    def remove(self, obj):
        if (opts.dry_run):
            return
        self.bucket.delete_key(obj.name)
        if (opts.more_verbose):
            print "S3Store: removed %s" % obj.name

###### FileStore #######
class FileStoreIterator(object):
    """FileStore iterator"""
    def __init__(self, base):
        self.base = base
        if (opts.follow_symlinks):
            self.generator = os.walk(base, followlinks=True)
        else:
            self.generator = os.walk(base)
        self.path = ""
        self.files = []
    def __iter__(self):
        return self
    def next(self):
        while True:
            if (len(self.files) == 0):
                self.path, dirs, self.files = self.generator.next()
                continue
            path = self.path + "/" + self.files[0]
            self.files = self.files[1:]
            # Ignore non-files when iterating.
            if (not os.path.isfile(path)):
                continue
            try:
                obj_name = local_name_to_s3_name(path[len(self.base)+1:])
            except LocalFileIsAcl, e:
                # ignore ACL side files when iterating
                continue
            return Object.from_file(obj_name, path)

class FileStore(Store):
    def __init__(self, url, create):
        # Parse the file url
        self.base = url
        if (self.base[-1:] == '/'):
            self.base = self.base[:-1]
        if (create):
            if (opts.dry_run):
                raise Exception("logic error: this should be unreachable.")
            mkdir_p(self.base)
        elif (not os.path.isdir(self.base)):
            raise NonexistentStore()
        Store.__init__(self, "file://" + url)
    def __str__(self):
        return "file://" + self.base
    def get_acl(self, obj):
        acl = LocalAcl(obj.name)
        acl_name = get_local_acl_file_name(obj.local_name())
        if (os.path.exists(acl_name)):
            acl.acl_path = self.base + "/" + acl_name
        return acl
    def make_local_copy(self, obj):
        local_name = obj.local_name()
        return LocalCopy(obj.name, self.base + "/" + local_name, False)
    def all_objects(self):
        return FileStoreIterator(self.base)
    def locate_object(self, obj):
        path = self.base + "/" + obj.local_name()
        found = os.path.isfile(path)
        if (opts.more_verbose):
            if (found):
                print "FileStore::locate_object: found object '" + \
                    obj.name + "'"
            else:
                print "FileStore::locate_object: did not find object '" + \
                    obj.name + "'"
        if (not found):
            return None
        return Object.from_file(obj.name, path)
    def upload(self, local_copy, src_acl, obj):
        if (opts.more_verbose):
            print "FileStore.UPLOAD: local_copy.path='" + local_copy.path + "' " + \
                "obj='" + obj.name + "'"
        if (opts.dry_run):
            return
        s = local_copy.path
        lname = obj.local_name()
        d = self.base + "/" + lname
        #print "s='" + s +"', d='" + d + "'"
        mkdir_p(os.path.dirname(d))
        shutil.copy(s, d)
        if (src_acl.acl_path != None):
            shutil.copy(src_acl.acl_path,
                self.base + "/" + get_local_acl_file_name(lname))
    def remove(self, obj):
        if (opts.dry_run):
            return
        os.unlink(self.base + "/" + obj.name)
        if (opts.more_verbose):
            print "FileStore: removed %s" % obj.name

###### Rados store #######
class RadosStoreIterator(object):
    """RadosStore iterator"""
    def __init__(self, it, rados_store):
        self.it = it # has type rados.ObjectIterator
        self.rados_store = rados_store
        self.prefix = self.rados_store.prefix
        self.prefix_len = len(self.rados_store.prefix)
    def __iter__(self):
        return self
    def next(self):
        rados_obj = None
        while True:
            # This will raise StopIteration when there are no more objects to
            # iterate on
            rados_obj = self.it.next()
            # do the prefixes match?
            if rados_obj.key[:self.prefix_len] == self.prefix:
                break
        ret = self.rados_store.obsync_obj_from_rgw(rados_obj.key)
        if (ret == None):
            raise Exception("internal iterator error")
        return ret

class RadosStore(Store):
    def __init__(self, url, create, akey, skey):
        # Parse the rados url
        conf_end = string.find(url, ":")
        if (conf_end == -1):
            raise Exception("RadosStore URLs are of the form \
rados:path/to/ceph/conf:bucket:key_prefix. Failed to find the path to the conf.")
        self.conf_file_path = url[0:conf_end]
        bucket_end = url.find(":", conf_end+1)
        if (bucket_end == -1):
            self.rgw_bucket_name = url[conf_end+1:]
            self.key_prefix = ""
        else:
            self.rgw_bucket_name = url[conf_end+1:bucket_end]
            self.key_prefix = url[bucket_end+1:]
        if (self.rgw_bucket_name == ""):
            raise Exception("RadosStore URLs are of the form \
rados:/path/to/ceph/conf:pool:key_prefix. Failed to find the bucket.")
        if (opts.more_verbose):
            print "self.conf_file_path = '" + self.conf_file_path + "', ",
            print "self.rgw_bucket_name = '" + self.rgw_bucket_name + "' ",
            print "self.key_prefix = '" + self.key_prefix + "'"
        acl_hack = getenv("ACL_HACK", None)
        if (acl_hack == None):
            raise Exception("RadosStore error: You must specify an environment " +
                "variable called ACL_HACK containing the name of a file. This " +
                "file contains a serialized RGW ACL that you want " +
                "to insert into the user.rgw.acl extended attribute of all " +
                "the objects you create. This is a hack and yes, it will go " +
                "away soon.")
        acl_hack_f = open(acl_hack, "r")
        try:
            self.acl_hack = acl_hack_f.read()
        finally:
            acl_hack_f.close()
        self.rados = rados.Rados()
        self.rados.conf_read_file(self.conf_file_path)
        self.rados.connect()
        if (not self.rados.pool_exists(self.rgw_bucket_name)):
            if (create):
                self.create_rgw_bucket(self.rgw_bucket_name)
            else:
                raise NonexistentStore()
        self.ioctx = self.rados.open_ioctx(self.rgw_bucket_name)
        Store.__init__(self, "rados:" + url)
    def create_rgw_bucket(self, rgw_bucket_name):
        """ Create an rgw bucket named 'rgw_bucket_name' """
        self.rados.create_pool(self.rgw_bucket_name)
        meta_ctx = None
        try:
            meta_ctx = self.rados.open_ioctx(RGW_META_BUCKET_NAME)
            meta_ctx.write(rgw_bucket_name, "", 0)
            print "meta_ctx.set_xattr(rgw_bucket_name=" + rgw_bucket_name + ", " + \
                    "user.rgw.acl, self.acl_hack=" + self.acl_hack + ")"
            meta_ctx.set_xattr(rgw_bucket_name, "user.rgw.acl", self.acl_hack)
        finally:
            if (meta_ctx):
               meta_ctx.close()
    def obsync_obj_from_rgw(self, key):
        """Create an obsync object from a Rados object"""
        try:
            size, tm = self.ioctx.stat(key)
        except rados.ObjectNotFound:
            return None
        md5 = self.ioctx.get_xattr(key, "user.rgw.etag")
        return Object(key, md5, size)
    def __str__(self):
        return "rados:" + self.conf_file_path + ":" + self.rgw_bucket_name + ":" + self.key_prefix
    def get_acl(self, obj):
        acl = LocalAcl(obj.name)
        # todo: set XML ACL
        return acl
    def make_local_copy(self, obj):
        temp_file = None
        temp_file_f = None
        try:
            # read the object from rados in chunks
            temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
            temp_file_f = open(temp_file.name, 'w')
            while True:
                buf = self.ioctx.read(obj.name, off, 8192)
                if (len(buf) == 0):
                    break
                temp_file_f.write(buf)
                if (len(buf) < 8192):
                    break
                off += 8192
            temp_file_f.close()
            # TODO: implement ACLs
        except:
            if (temp_file_f):
                temp_file_f.close()
            if (temp_file):
                os.unlink(temp_file.name)
            raise
        return LocalCopy(obj.name, temp_file.name, True)
    def all_objects(self):
        it = self.bucket.list_objects()
        return RadosStoreIterator(it, self.key_prefix)
    def locate_object(self, obj):
        return self.obsync_obj_from_rgw(obj.name)
    def upload(self, local_copy, src_acl, obj):
        if (opts.more_verbose):
            print "RadosStore.UPLOAD: local_copy.path='" + local_copy.path + "' " + \
                "obj='" + obj.name + "'"
        if (opts.dry_run):
            return
        local_copy_f = open(local_copy.path, 'r')
        off = 0
        while True:
            buf = local_copy_f.read(8192)
            if ((len(buf) == 0) and (off != 0)):
                break
            self.ioctx.write(obj.name, buf, off)
            if (len(buf) < 8192):
                break
            off += 8192
        self.ioctx.set_xattr(obj.name, "user.rgw.etag", obj.md5)
        self.ioctx.set_xattr(obj.name, "user.rgw.acl", self.acl_hack)
        self.ioctx.set_xattr(obj.name, "user.rgw.content_type",
                            "application/octet-stream")
    def remove(self, obj):
        if (opts.dry_run):
            return
        self.ioctx.remove_object(obj.name)
        if (opts.more_verbose):
            print "RadosStore: removed %s" % obj.name
###### Functions #######
def delete_unreferenced(src, dst):
    """ delete everything from dst that is not referenced in src """
    if (opts.more_verbose):
        print "handling deletes."
    for dobj in dst.all_objects():
        sobj = src.locate_object(dobj)
        if (sobj == None):
            dst.remove(dobj)

def xuser_cb(opt, opt_str, value, parser):
    """ handle an --xuser argument """
    equals = value.find(r'=')
    if equals == -1:
        print >>stderr, "Error parsing --xuser: You must give both a source \
and destination user name, like so:\n\
--xuser SOURCE_USER=DEST_USER\n\
\n\
This will translate the user SOURCE_USER in the source to the user DEST_USER \n\
in the destination."
        sys.exit(1)
    src_user = value[:equals]
    dst_user = value[equals+1:]
    if ((len(src_user) == 0) or (len(dst_user) == 0)):
        print >>stderr, "Error parsing --xuser: can't have a zero-length \
user name."
        sys.exit(1)
    src_user = add_user_type(src_user)
    dst_user = add_user_type(dst_user)
    if (xuser.has_key(src_user)):
        print >>stderr, "Error parsing --xuser: we are already translating \
\"%s\" to \"%s\"; we cannot translate it to \"%s\"" % \
(src_user, xuser[src_user], dst_user)
        sys.exit(1)
    xuser[src_user] = dst_user

USAGE = """
obsync synchronizes S3, Rados, and local objects. The source and destination
can both be local or both remote.

Examples:
# copy contents of mybucket to disk
obsync -v s3://myhost/mybucket file://mydir

# copy contents of mydir to an S3 bucket
obsync -v file://mydir s3://myhost/mybucket

# synchronize two S3 buckets
SRC_AKEY=... SRC_SKEY=... \
DST_AKEY=... DST_SKEY=... \
obsync -v s3://myhost/mybucket1 s3://myhost2/mybucket2
   --xuser bob=robert --xuser joe=joseph -O bob

Note: You must specify an AWS access key and secret access key when accessing
S3. obsync honors these environment variables:
SRC_AKEY          Access key for the source URL
SRC_SKEY          Secret access key for the source URL
DST_AKEY          Access key for the destination URL
DST_SKEY          Secret access key for the destination URL
AKEY              Access key for both source and dest
SKEY              Secret access key for both source and dest

If these environment variables are not given, we will fall back on libboto
defaults.

obsync (options) [source] [destination]"""

parser = OptionParser(USAGE)
parser.add_option("-n", "--dry-run", action="store_true", \
    dest="dry_run", default=False)
parser.add_option("-c", "--create-dest", action="store_true", \
    dest="create", help="create the destination if it doesn't already exist")
parser.add_option("--delete-before", action="store_true", \
    dest="delete_before", help="delete objects that aren't in SOURCE from \
DESTINATION before transferring any objects")
parser.add_option("-d", "--delete-after", action="store_true", \
    dest="delete_after", help="delete objects that aren't in SOURCE from \
DESTINATION after doing all transfers.")
parser.add_option("-L", "--follow-symlinks", action="store_true", \
    dest="follow_symlinks", help="follow symlinks (please avoid symlink " + \
    "loops when using this option!)")
parser.add_option("--no-preserve-acls", action="store_true", \
    dest="no_preserve_acls", help="don't preserve ACLs when copying objects.")
parser.add_option("-v", "--verbose", action="store_true", \
    dest="verbose", help="be verbose")
parser.add_option("-V", "--more-verbose", action="store_true", \
    dest="more_verbose", help="be really, really verbose (developer mode)")
parser.add_option("-x", "--xuser", type="string", nargs=1, action="callback", \
    dest="SRC=DST", callback=xuser_cb, help="set up a user tranlation. You \
can specify multiple user translations with multiple --xuser arguments.")
parser.add_option("--unit", action="store_true", \
    dest="run_unit_tests", help="run unit tests and quit")
xuser = {}
(opts, args) = parser.parse_args()
if (opts.run_unit_tests):
    test_acl_policy()
    sys.exit(0)

opts.preserve_acls = not opts.no_preserve_acls
if (opts.create and opts.dry_run):
    raise Exception("You can't run with both --create-dest and --dry-run! \
By definition, a dry run never changes anything.")
if (len(args) < 2):
    print >>stderr, "Expected two positional arguments: source and destination"
    print >>stderr, USAGE
    sys.exit(1)
elif (len(args) > 2):
    print >>stderr, "Too many positional arguments."
    print >>stderr, USAGE
    sys.exit(1)
if (opts.more_verbose):
    print >>stderr, "User translations:"
    for k,v in xuser.items():
        print >>stderr, "\"%s\" ==> \"%s\"" % (k, v)
    print >>stderr, ""
if (opts.more_verbose):
    opts.verbose = True
    boto.set_stream_logger("stdout")
    boto.log.info("Enabling verbose boto logging.")
if (opts.delete_before and opts.delete_after):
    print >>stderr, "It doesn't make sense to specify both --delete-before \
and --delete-after."
    sys.exit(1)
src_name = args[0]
dst_name = args[1]
try:
    if (opts.more_verbose):
        print "SOURCE: " + src_name
    src = Store.make_store(src_name, False,
            getenv("SRC_AKEY", "AKEY"), getenv("SRC_SKEY", "SKEY"))
except NonexistentStore, e:
    print >>stderr, "Fatal error: Source " + src_name + " does not exist."
    sys.exit(1)
except Exception, e:
    print >>stderr, "error creating source: " + str(e)
    traceback.print_exc(100000, stderr)
    sys.exit(1)
try:
    if (opts.more_verbose):
        print "DESTINATION: " + dst_name
    dst = Store.make_store(dst_name, opts.create,
            getenv("DST_AKEY", "AKEY"), getenv("DST_SKEY", "SKEY"))
except NonexistentStore, e:
    print >>stderr, "Fatal error: Destination " + dst_name + " does " +\
        "not exist. Run with -c or --create-dest to create it automatically."
    sys.exit(1)
except Exception, e:
    print >>stderr, "error creating destination: " + str(e)
    traceback.print_exc(100000, stderr)
    sys.exit(1)

if (opts.delete_before):
    delete_unreferenced(src, dst)

for sobj in src.all_objects():
    if (opts.more_verbose):
        print "handling " + sobj.name
    dobj = dst.locate_object(sobj)
    upload = False
    src_acl = None
    dst_acl = None
    if (dobj == None):
        if (opts.verbose):
            print "+ " + sobj.name
        upload = True
    elif not sobj.equals(dobj):
        if (opts.verbose):
            print "> " + sobj.name
        upload = True
    elif (opts.preserve_acls):
        # Do the ACLs match?
        src_acl = src.get_acl(sobj)
        dst_acl = dst.get_acl(dobj)
        src_acl.translate_users(xuser)
        if (not src_acl.equals(dst_acl)):
            upload = True
        if (opts.verbose):
            print "^ " + sobj.name
        dst_acl.remove()
    else:
        if (opts.verbose):
            print ". " + sobj.name
    if (upload):
        if (opts.preserve_acls and src_acl == None):
            src_acl = src.get_acl(sobj)
            src_acl.translate_users(xuser)
        else:
            # Just default to an empty ACL
            src_acl = LocalAcl(sobj.name)
        local_copy = src.make_local_copy(sobj)
        try:
            dst.upload(local_copy, src_acl, sobj)
        finally:
            local_copy.remove()
            src_acl.remove()

if (opts.delete_after):
    delete_unreferenced(src, dst)

if (opts.more_verbose):
    print "finished."

sys.exit(0)
