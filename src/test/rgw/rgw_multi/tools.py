import json
import boto

def append_attr_value(d, attr, attrv):
    if attrv and len(str(attrv)) > 0:
        d[attr] = attrv

def append_attr(d, k, attr):
    try:
        attrv = getattr(k, attr)
    except:
        return
    append_attr_value(d, attr, attrv)

def get_attrs(k, attrs):
    d = {}
    for a in attrs:
        append_attr(d, k, a)

    return d

def append_query_arg(s, n, v):
    if not v:
        return s
    nv = '{n}={v}'.format(n=n, v=v)
    if not s:
        return nv
    return '{s}&{nv}'.format(s=s, nv=nv)

class KeyJSONEncoder(boto.s3.key.Key):
    @staticmethod
    def default(k, versioned=False):
        attrs = ['bucket', 'name', 'size', 'last_modified', 'metadata', 'cache_control',
                 'content_type', 'content_disposition', 'content_language',
                 'owner', 'storage_class', 'md5', 'version_id', 'encrypted',
                 'delete_marker', 'expiry_date', 'VersionedEpoch', 'RgwxTag']
        d = get_attrs(k, attrs)
        d['etag'] = k.etag[1:-1]
        if versioned:
            d['is_latest'] = k.is_latest
        return d

class DeleteMarkerJSONEncoder(boto.s3.key.Key):
    @staticmethod
    def default(k):
        attrs = ['name', 'version_id', 'last_modified', 'owner']
        d = get_attrs(k, attrs)
        d['delete_marker'] = True
        d['is_latest'] = k.is_latest
        return d

class UserJSONEncoder(boto.s3.user.User):
    @staticmethod
    def default(k):
        attrs = ['id', 'display_name']
        return get_attrs(k, attrs)

class BucketJSONEncoder(boto.s3.bucket.Bucket):
    @staticmethod
    def default(k):
        attrs = ['name', 'creation_date']
        return get_attrs(k, attrs)

class BotoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, boto.s3.key.Key):
            return KeyJSONEncoder.default(obj)
        if isinstance(obj, boto.s3.deletemarker.DeleteMarker):
            return DeleteMarkerJSONEncoder.default(obj)
        if isinstance(obj, boto.s3.user.User):
            return UserJSONEncoder.default(obj)
        if isinstance(obj, boto.s3.prefix.Prefix):
            return (lambda x: {'prefix': x.name})(obj)
        if isinstance(obj, boto.s3.bucket.Bucket):
            return BucketJSONEncoder.default(obj)
        return json.JSONEncoder.default(self, obj)


def dump_json(o, cls=BotoJSONEncoder):
    return json.dumps(o, cls=cls, indent=4)


