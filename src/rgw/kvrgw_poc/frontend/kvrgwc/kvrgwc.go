package kvrgwc

/*
#cgo CFLAGS: -I${SRCDIR}/../../backend/include
#cgo LDFLAGS: -L${SRCDIR}/../../build -lkvrgw
#cgo LDFLAGS: -Wl,-rpath,${SRCDIR}/../../build
#include "kvrgw_c_api.h"
#include <stdlib.h>
*/
import "C"

import (
	"runtime"
	"unsafe"
)

const (
	ErrOK                  = int32(C.KVRGW_ERR_OK)
	ErrNoSuchKey           = int32(C.KVRGW_ERR_NO_SUCH_KEY)
	ErrNoSuchBucket        = int32(C.KVRGW_ERR_NO_SUCH_BUCKET)
	ErrBucketAlreadyExists = int32(C.KVRGW_ERR_BUCKET_ALREADY_EXISTS)
	ErrBucketNotEmpty      = int32(C.KVRGW_ERR_BUCKET_NOT_EMPTY)
	ErrPreconditionFailed  = int32(C.KVRGW_ERR_PRECONDITION_FAILED)
	ErrAccessDenied        = int32(C.KVRGW_ERR_ACCESS_DENIED)
	ErrInvalidArgument     = int32(C.KVRGW_ERR_INVALID_ARGUMENT)
	ErrInvalidBucketName   = int32(C.KVRGW_ERR_INVALID_BUCKET_NAME)
	ErrNoSuchVersion       = int32(C.KVRGW_ERR_NO_SUCH_VERSION)
	ErrNoSuchTenant        = int32(C.KVRGW_ERR_NO_SUCH_TENANT)
	ErrInvalidRange        = int32(C.KVRGW_ERR_INVALID_RANGE)
	ErrInvalidRequest      = int32(C.KVRGW_ERR_INVALID_REQUEST)
	ErrInvalidTag          = int32(C.KVRGW_ERR_INVALID_TAG)
	ErrInternal            = int32(C.KVRGW_ERR_INTERNAL)
)

const (
	VersioningDisabled  = uint8(C.KVRGW_VERSIONING_DISABLED)
	VersioningEnabled   = uint8(C.KVRGW_VERSIONING_ENABLED)
	VersioningSuspended = uint8(C.KVRGW_VERSIONING_SUSPENDED)
)

const (
	MaxListKeys    = 1000
	MaxListBuckets = 10000
)

type Handle struct {
	h *C.KvRgwHandle
}

type KV struct {
	Key   string
	Value string
}

type ByteRange struct {
	Start        int64
	End          int64
	FromEnd      bool
	EndUnbounded bool
}

type ObjectMeta struct {
	ETag             string
	Size             uint64
	LastModifiedUnix int64
	ContentType      string
	VersionID        string
	TagsCount        uint32
	ErrorDetail      string
}

type GetObjectResult struct {
	Body     []byte
	Meta     ObjectMeta
	Metadata []KV
}

type HeadObjectResult struct {
	Meta     ObjectMeta
	Metadata []KV
}

type DeleteResult struct {
	CreatedDM   bool
	DMVersionID uint32
}

type CopyArgs struct {
	SrcBucket       string
	SrcKey          string
	DstBucket       string
	DstKey          string
	SrcVersionID    string
	IfMatch         string
	IfNoneMatch     string
	DstIfMatch      string
	DstIfNoneMatch  string
	ContentType     string
	ReplaceMetadata bool
	Metadata        []KV
	ReplaceTags     bool
	Tags            []KV
}

type CopyResult struct {
	ETag             string
	LastModifiedUnix int64
	VersionID        string
	SrcVersionID     string
}

type BucketEntry struct {
	Name          string
	CreatedAtUnix int64
}

type ListBucketsResult struct {
	Buckets   []BucketEntry
	NextToken string
}

type ObjectEntry struct {
	Key              string
	Size             uint64
	ETag             string
	LastModifiedUnix int64
}

type ListObjectsResult struct {
	Objects        []ObjectEntry
	CommonPrefixes []string
	Truncated      bool
	NextToken      string
}

type VersionEntry struct {
	Key              string
	VersionID        uint32
	IsLatest         bool
	IsDeleteMarker   bool
	Size             uint64
	ETag             string
	LastModifiedUnix int64
}

type ListVersionsResult struct {
	Versions      []VersionEntry
	Truncated     bool
	NextKey       string
	NextVersionID uint32
}

type DeleteMultiObject struct {
	Key       string
	VersionID string
}

type DeleteMultiOut struct {
	Key         string
	Error       bool
	ErrCode     string
	ErrMsg      string
	VersionID   string
	CreatedDM   bool
	DMVersionID uint32
}

type callPin struct {
	p runtime.Pinner
}

func (c *callPin) done() { c.p.Unpin() }

func (c *callPin) str(s string) (*C.char, C.size_t) {
	if s == "" {
		return nil, 0
	}
	ptr := unsafe.StringData(s)
	c.p.Pin(ptr)
	return (*C.char)(unsafe.Pointer(ptr)), C.size_t(len(s))
}

func (c *callPin) bytes(b []byte) (*C.uint8_t, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	c.p.Pin(&b[0])
	return (*C.uint8_t)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}

func (c *callPin) chars(b []byte) (*C.char, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	c.p.Pin(&b[0])
	return (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}

func (c *callPin) kvs(src []KV) []C.kvrgw_kv {
	if len(src) == 0 {
		return nil
	}
	dst := make([]C.kvrgw_kv, len(src))
	for i := range src {
		dst[i].key, dst[i].key_len = c.str(src[i].Key)
		dst[i].value, dst[i].value_len = c.str(src[i].Value)
	}
	c.p.Pin(&dst[0])
	return dst
}

func cstr(s string) (*C.char, C.size_t) {
	if s == "" {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s))), C.size_t(len(s))
}

func cbuf(b []byte) (*C.char, C.size_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(&b[0])), C.size_t(len(b))
}

func gostr(p *C.char, n C.size_t) string {
	if p == nil || n == 0 {
		return ""
	}
	return C.GoStringN(p, C.int(n))
}

func goCArr(p *C.char) string {
	if p == nil {
		return ""
	}
	return C.GoString(p)
}

func nzterm(b []byte) string {
	n := 0
	for n < len(b) && b[n] != 0 {
		n++
	}
	return string(b[:n])
}

func arenaStr[T ~uint32](arena []byte, off, length T) string {
	if length == 0 {
		return ""
	}
	end := int(off) + int(length)
	if int(off) > len(arena) || end > len(arena) {
		return ""
	}
	return string(arena[off:end])
}

func metaFromC(m *C.kvrgw_object_meta) ObjectMeta {
	return ObjectMeta{
		ETag:             goCArr(&m.etag[0]),
		Size:             uint64(m.size),
		LastModifiedUnix: int64(m.last_modified_unix),
		ContentType:      goCArr(&m.content_type[0]),
		VersionID:        goCArr(&m.version_id[0]),
		TagsCount:        uint32(m.tags_count),
		ErrorDetail:      goCArr(&m.error_detail[0]),
	}
}

func kvsFromC(src []C.kvrgw_kv, n int) []KV {
	if n <= 0 {
		return nil
	}
	out := make([]KV, n)
	for i := 0; i < n; i++ {
		out[i].Key = gostr(src[i].key, src[i].key_len)
		out[i].Value = gostr(src[i].value, src[i].value_len)
	}
	return out
}

func Start(dataRoot string) (*Handle, int32) {
	p, n := cstr(dataRoot)
	var out *C.KvRgwHandle
	ec := C.kvrgw_start(p, n, &out)
	if ec != C.KVRGW_ERR_OK {
		return nil, int32(ec)
	}
	return &Handle{h: out}, 0
}

func (h *Handle) Stop() {
	if h == nil || h.h == nil {
		return
	}
	C.kvrgw_stop(h.h)
	h.h = nil
}

func (h *Handle) AddTenant(name string) (uint32, int32) {
	p, n := cstr(name)
	var id C.uint32_t
	ec := C.kvrgw_add_tenant(h.h, p, n, &id)
	return uint32(id), int32(ec)
}

func (h *Handle) ResolveTenant(name string) (exists bool, id uint32, errCode int32) {
	p, n := cstr(name)
	var ex C.int
	var tid C.uint32_t
	ec := C.kvrgw_resolve_tenant(h.h, p, n, &ex, &tid)
	return ex != 0, uint32(tid), int32(ec)
}

func (h *Handle) CreateBucket(tenantID uint32, bucket string) int32 {
	p, n := cstr(bucket)
	return int32(C.kvrgw_create_bucket(h.h, C.uint32_t(tenantID), p, n))
}

func (h *Handle) DeleteBucket(tenantID uint32, bucket string) int32 {
	p, n := cstr(bucket)
	return int32(C.kvrgw_delete_bucket(h.h, C.uint32_t(tenantID), p, n))
}

func (h *Handle) BucketExists(tenantID uint32, bucket string) (exists bool, id uint64, errCode int32) {
	p, n := cstr(bucket)
	var ex C.int
	var bid C.uint64_t
	ec := C.kvrgw_bucket_exists(h.h, C.uint32_t(tenantID), p, n, &ex, &bid)
	return ex != 0, uint64(bid), int32(ec)
}

func (h *Handle) BucketExistsCached(tenantID uint32, bucket string) (exists bool, id uint64, errCode int32) {
	p, n := cstr(bucket)
	var ex C.int
	var bid C.uint64_t
	ec := C.kvrgw_bucket_exists_cached(h.h, C.uint32_t(tenantID), p, n, &ex, &bid)
	return ex != 0, uint64(bid), int32(ec)
}

func (h *Handle) PutBucketVersioning(tenantID uint32, bucket string, state uint8) int32 {
	p, n := cstr(bucket)
	return int32(C.kvrgw_put_bucket_versioning(h.h, C.uint32_t(tenantID), p, n, C.uint8_t(state)))
}

func (h *Handle) GetBucketVersioning(tenantID uint32, bucket string) (uint8, int32) {
	p, n := cstr(bucket)
	var st C.uint8_t
	ec := C.kvrgw_get_bucket_versioning(h.h, C.uint32_t(tenantID), p, n, &st)
	return uint8(st), int32(ec)
}

func (h *Handle) PutBucketPolicy(tenantID uint32, bucket, policy string) int32 {
	b, bn := cstr(bucket)
	p, pn := cstr(policy)
	return int32(C.kvrgw_put_bucket_policy(h.h, C.uint32_t(tenantID), b, bn, p, pn))
}

func (h *Handle) GetBucketPolicy(tenantID uint32, bucket string) ([]byte, int32) {
	b, bn := cstr(bucket)
	buf := make([]byte, 4096)
	for range 6 {
		p, capn := cbuf(buf)
		var outLen C.size_t
		ec := C.kvrgw_get_bucket_policy(h.h, C.uint32_t(tenantID), b, bn, p, capn, &outLen)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				return nil, int32(ec)
			}
			return buf[:int(outLen)], 0
		}
		need := int(outLen)
		if need > len(buf) {
			buf = make([]byte, need)
			continue
		}
		if len(buf) == 0 {
			buf = make([]byte, 1)
			continue
		}
		return nil, int32(ec)
	}
	return nil, ErrInvalidArgument
}

func (h *Handle) DeleteBucketPolicy(tenantID uint32, bucket string) int32 {
	p, n := cstr(bucket)
	return int32(C.kvrgw_delete_bucket_policy(h.h, C.uint32_t(tenantID), p, n))
}

func (h *Handle) PutObject(tenantID uint32, bucket, key string, data []byte, contentType string,
	estimatedSize uint64, ifMatch, ifNoneMatch string, tags, metadata []KV) (etag, versionID string, errCode int32) {
	var pin callPin
	defer pin.done()
	b, bn := pin.str(bucket)
	k, kn := pin.str(key)
	d, dn := pin.bytes(data)
	ct, ctn := pin.str(contentType)
	im, imn := pin.str(ifMatch)
	inm, inmn := pin.str(ifNoneMatch)
	tagArr := pin.kvs(tags)
	var tagPtr *C.kvrgw_kv
	if len(tagArr) > 0 {
		tagPtr = &tagArr[0]
	}
	metaArr := pin.kvs(metadata)
	var metaPtr *C.kvrgw_kv
	if len(metaArr) > 0 {
		metaPtr = &metaArr[0]
	}
	etagBuf := make([]byte, 48)
	vidBuf := make([]byte, 9)
	pin.p.Pin(&etagBuf[0])
	pin.p.Pin(&vidBuf[0])
	ec := C.kvrgw_put_object(h.h, C.uint32_t(tenantID), b, bn, k, kn, d, dn, ct, ctn,
		C.uint64_t(estimatedSize), im, imn, inm, inmn, tagPtr, C.size_t(len(tags)),
		metaPtr, C.size_t(len(metadata)),
		(*C.char)(unsafe.Pointer(&etagBuf[0])), C.size_t(len(etagBuf)),
		(*C.char)(unsafe.Pointer(&vidBuf[0])), C.size_t(len(vidBuf)))
	return nzterm(etagBuf), nzterm(vidBuf), int32(ec)
}

func (h *Handle) GetObject(tenantID uint32, bucket, key, versionID string, rng *ByteRange) (GetObjectResult, int32) {
	b, bn := cstr(bucket)
	k, kn := cstr(key)
	v, vn := cstr(versionID)
	var cr C.kvrgw_byte_range
	var crp *C.kvrgw_byte_range
	if rng != nil {
		cr.start = C.int64_t(rng.Start)
		cr.end = C.int64_t(rng.End)
		if rng.FromEnd {
			cr.from_end = 1
		}
		if rng.EndUnbounded {
			cr.end_unbounded = 1
		}
		crp = &cr
	}
	bodySize := 1
	mdCap := 16
	arenaSize := 4096
	var result GetObjectResult
	for range 8 {
		var pin callPin
		var meta C.kvrgw_object_meta
		body := make([]byte, bodySize)
		md := make([]C.kvrgw_kv, mdCap)
		arena := make([]byte, arenaSize)
		bp, bcap := pin.bytes(body)
		pin.p.Pin(&md[0])
		ap, acap := pin.chars(arena)
		var bodyLen C.size_t
		var mdn C.uint32_t
		ec := C.kvrgw_get_object(h.h, C.uint32_t(tenantID), b, bn, k, kn, v, vn, crp, &meta,
			bp, bcap, &bodyLen, &md[0], C.uint32_t(len(md)), &mdn, ap, acap)
		result.Meta = metaFromC(&meta)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				pin.done()
				return result, int32(ec)
			}
			result.Body = body[:int(bodyLen)]
			result.Metadata = kvsFromC(md, int(mdn))
			pin.done()
			return result, 0
		}
		grew := false
		if int(bodyLen) > bodySize {
			bodySize = int(bodyLen)
			grew = true
		}
		if int(mdn) > mdCap {
			mdCap = int(mdn)
			grew = true
		}
		if !grew {
			arenaSize *= 2
		}
		pin.done()
	}
	return result, ErrInvalidArgument
}

func (h *Handle) HeadObject(tenantID uint32, bucket, key, versionID string) (HeadObjectResult, int32) {
	b, bn := cstr(bucket)
	k, kn := cstr(key)
	v, vn := cstr(versionID)
	mdCap := 16
	arenaSize := 4096
	var result HeadObjectResult
	for range 8 {
		var pin callPin
		var meta C.kvrgw_object_meta
		md := make([]C.kvrgw_kv, mdCap)
		arena := make([]byte, arenaSize)
		pin.p.Pin(&md[0])
		ap, acap := pin.chars(arena)
		var mdn C.uint32_t
		ec := C.kvrgw_head_object(h.h, C.uint32_t(tenantID), b, bn, k, kn, v, vn, &meta,
			&md[0], C.uint32_t(len(md)), &mdn, ap, acap)
		result.Meta = metaFromC(&meta)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				pin.done()
				return result, int32(ec)
			}
			result.Metadata = kvsFromC(md, int(mdn))
			pin.done()
			return result, 0
		}
		if int(mdn) > mdCap {
			mdCap = int(mdn)
			pin.done()
			continue
		}
		arenaSize *= 2
		pin.done()
	}
	return result, ErrInvalidArgument
}

func (h *Handle) DeleteObject(tenantID uint32, bucket, key, ifMatch string,
	ifMatchMtime, ifMatchSize int64, hasIfMatchSize bool) (DeleteResult, int32) {
	b, bn := cstr(bucket)
	k, kn := cstr(key)
	im, imn := cstr(ifMatch)
	has := C.int(0)
	if hasIfMatchSize {
		has = 1
	}
	var created C.int
	var dm C.uint32_t
	ec := C.kvrgw_delete_object(h.h, C.uint32_t(tenantID), b, bn, k, kn, im, imn,
		C.int64_t(ifMatchMtime), C.int64_t(ifMatchSize), has, &created, &dm)
	return DeleteResult{CreatedDM: created != 0, DMVersionID: uint32(dm)}, int32(ec)
}

func (h *Handle) DeleteObjectVersion(tenantID uint32, bucket, key, versionID, ifMatch string,
	ifMatchMtime, ifMatchSize int64, hasIfMatchSize bool) int32 {
	b, bn := cstr(bucket)
	k, kn := cstr(key)
	v, vn := cstr(versionID)
	im, imn := cstr(ifMatch)
	has := C.int(0)
	if hasIfMatchSize {
		has = 1
	}
	return int32(C.kvrgw_delete_object_version(h.h, C.uint32_t(tenantID), b, bn, k, kn, v, vn, im, imn,
		C.int64_t(ifMatchMtime), C.int64_t(ifMatchSize), has))
}

func (h *Handle) DeleteMulti(tenantID uint32, bucket string, keys []string, objects []DeleteMultiObject) ([]DeleteMultiOut, int32) {
	var pin callPin
	defer pin.done()
	b, bn := pin.str(bucket)
	var keyPtr *C.kvrgw_buf
	if n := len(keys); n > 0 {
		keyBufs := make([]C.kvrgw_buf, n)
		for i := 0; i < n; i++ {
			keyBufs[i].data, keyBufs[i].len = pin.str(keys[i])
		}
		pin.p.Pin(&keyBufs[0])
		keyPtr = &keyBufs[0]
	}
	var objPtr *C.kvrgw_kv
	if n := len(objects); n > 0 {
		kvs := make([]KV, n)
		for i := 0; i < n; i++ {
			kvs[i] = KV{Key: objects[i].Key, Value: objects[i].VersionID}
		}
		objArr := pin.kvs(kvs)
		objPtr = &objArr[0]
	}
	outCap := len(keys) + len(objects)
	if outCap == 0 {
		outCap = 1
	}
	arenaSize := 256 * 1024
	for range 6 {
		outs := make([]C.kvrgw_del_multi_out, outCap)
		arena := make([]byte, arenaSize)
		ap, acap := cbuf(arena)
		var outCount C.uint32_t
		ec := C.kvrgw_delete_multi(h.h, C.uint32_t(tenantID), b, bn, keyPtr, C.size_t(len(keys)),
			objPtr, C.size_t(len(objects)), &outs[0], C.uint32_t(len(outs)), &outCount, ap, acap)
		if int32(ec) != ErrInvalidArgument {
			n := int(outCount)
			result := make([]DeleteMultiOut, n)
			for i := 0; i < n; i++ {
				e := outs[i]
				result[i] = DeleteMultiOut{
					Key:         arenaStr(arena, e.key_off, e.key_len),
					Error:       e.status != 0,
					ErrCode:     arenaStr(arena, e.err_code_off, e.err_code_len),
					ErrMsg:      arenaStr(arena, e.err_msg_off, e.err_msg_len),
					VersionID:   arenaStr(arena, e.version_id_off, e.version_id_len),
					CreatedDM:   e.created_dm != 0,
					DMVersionID: uint32(e.dm_version_id),
				}
			}
			return result, int32(ec)
		}
		if int(outCount) > outCap {
			outCap = int(outCount)
			continue
		}
		arenaSize *= 2
	}
	return nil, ErrInvalidArgument
}

func (h *Handle) ListBuckets(tenantID uint32, prefix, continuation string, maxBuckets uint32) (ListBucketsResult, int32) {
	p, pn := cstr(prefix)
	c, cn := cstr(continuation)
	nCap := int(maxBuckets)
	if nCap <= 0 {
		nCap = 1
	} else if nCap > MaxListBuckets {
		nCap = MaxListBuckets
	}
	arenaSize := 1 << 20
	tokSize := 4096
	var result ListBucketsResult
	for range 6 {
		entries := make([]C.kvrgw_list_bkt_entry, nCap)
		arena := make([]byte, arenaSize)
		tok := make([]byte, tokSize)
		ap, acap := cbuf(arena)
		tp, tcap := cbuf(tok)
		var nent C.uint32_t
		var ntok C.size_t
		ec := C.kvrgw_list_buckets(h.h, C.uint32_t(tenantID), p, pn, c, cn, C.uint32_t(maxBuckets),
			&entries[0], C.uint32_t(len(entries)), &nent, ap, acap, tp, tcap, &ntok)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				return result, int32(ec)
			}
			n := int(nent)
			result.Buckets = make([]BucketEntry, n)
			for i := 0; i < n; i++ {
				e := entries[i]
				result.Buckets[i] = BucketEntry{
					Name:          arenaStr(arena, e.name_off, e.name_len),
					CreatedAtUnix: int64(e.created_at_unix),
				}
			}
			result.NextToken = string(tok[:int(ntok)])
			return result, 0
		}
		if int(nent) > nCap {
			nCap = int(nent)
			continue
		}
		if int(ntok) > tokSize {
			tokSize = int(ntok)
			continue
		}
		arenaSize *= 2
	}
	return result, ErrInvalidArgument
}

func (h *Handle) ListObjects(tenantID uint32, bucket, prefix, delimiter, continuation, marker string, maxKeys uint32) (ListObjectsResult, int32) {
	if maxKeys > MaxListKeys {
		maxKeys = MaxListKeys
	}
	b, bn := cstr(bucket)
	p, pn := cstr(prefix)
	d, dn := cstr(delimiter)
	c, cn := cstr(continuation)
	m, mn := cstr(marker)
	nCap := MaxListKeys
	prefCap := MaxListKeys
	arenaSize := 1 << 20
	tokSize := 4096
	var result ListObjectsResult
	for range 6 {
		entries := make([]C.kvrgw_list_obj_entry, nCap)
		po := make([]uint32, prefCap)
		pl := make([]uint32, prefCap)
		arena := make([]byte, arenaSize)
		tok := make([]byte, tokSize)
		ap, acap := cbuf(arena)
		tp, tcap := cbuf(tok)
		var nent, npref C.uint32_t
		var trunc C.int
		var ntok C.size_t
		ec := C.kvrgw_list_objects(h.h, C.uint32_t(tenantID), b, bn, p, pn, d, dn, C.uint32_t(maxKeys),
			c, cn, m, mn, &entries[0], C.uint32_t(len(entries)), &nent,
			(*C.uint32_t)(unsafe.Pointer(&po[0])), (*C.uint32_t)(unsafe.Pointer(&pl[0])),
			C.uint32_t(len(po)), &npref, ap, acap, &trunc, tp, tcap, &ntok)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				return result, int32(ec)
			}
			n := int(nent)
			result.Objects = make([]ObjectEntry, n)
			for i := 0; i < n; i++ {
				e := entries[i]
				result.Objects[i] = ObjectEntry{
					Key:              arenaStr(arena, e.key_off, e.key_len),
					Size:             uint64(e.size),
					ETag:             arenaStr(arena, e.etag_off, e.etag_len),
					LastModifiedUnix: int64(e.last_modified_unix),
				}
			}
			np := int(npref)
			result.CommonPrefixes = make([]string, np)
			for i := 0; i < np; i++ {
				result.CommonPrefixes[i] = arenaStr(arena, po[i], pl[i])
			}
			result.Truncated = trunc != 0
			result.NextToken = string(tok[:int(ntok)])
			return result, 0
		}
		if int(nent) > nCap {
			nCap = int(nent)
			continue
		}
		if int(npref) > prefCap {
			prefCap = int(npref)
			continue
		}
		if int(ntok) > tokSize {
			tokSize = int(ntok)
			continue
		}
		arenaSize *= 2
	}
	return result, ErrInvalidArgument
}

func (h *Handle) ListObjectVersions(tenantID uint32, bucket, prefix, keyMarker string, maxKeys, versionIDMarker uint32) (ListVersionsResult, int32) {
	if maxKeys > MaxListKeys {
		maxKeys = MaxListKeys
	}
	b, bn := cstr(bucket)
	p, pn := cstr(prefix)
	km, kmn := cstr(keyMarker)
	nCap := MaxListKeys
	arenaSize := 1 << 20
	keySize := 4096
	var result ListVersionsResult
	for range 6 {
		entries := make([]C.kvrgw_list_ver_entry, nCap)
		arena := make([]byte, arenaSize)
		nk := make([]byte, keySize)
		ap, acap := cbuf(arena)
		nkp, nkcap := cbuf(nk)
		var nent C.uint32_t
		var trunc C.int
		var nkl C.size_t
		var nvid C.uint32_t
		ec := C.kvrgw_list_object_versions(h.h, C.uint32_t(tenantID), b, bn, p, pn, C.uint32_t(maxKeys),
			km, kmn, C.uint32_t(versionIDMarker), &entries[0], C.uint32_t(len(entries)), &nent,
			ap, acap, &trunc, nkp, nkcap, &nkl, &nvid)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				return result, int32(ec)
			}
			n := int(nent)
			result.Versions = make([]VersionEntry, n)
			for i := 0; i < n; i++ {
				e := entries[i]
				result.Versions[i] = VersionEntry{
					Key:              arenaStr(arena, e.key_off, e.key_len),
					VersionID:        uint32(e.version_id),
					IsLatest:         e.is_latest != 0,
					IsDeleteMarker:   e.is_delete_marker != 0,
					Size:             uint64(e.size),
					ETag:             arenaStr(arena, e.etag_off, e.etag_len),
					LastModifiedUnix: int64(e.last_modified_unix),
				}
			}
			result.Truncated = trunc != 0
			result.NextKey = string(nk[:int(nkl)])
			result.NextVersionID = uint32(nvid)
			return result, 0
		}
		if int(nent) > nCap {
			nCap = int(nent)
			continue
		}
		if int(nkl) > keySize {
			keySize = int(nkl)
			continue
		}
		arenaSize *= 2
	}
	return result, ErrInvalidArgument
}

func (h *Handle) CopyObject(tenantID uint32, args CopyArgs) (CopyResult, int32) {
	var pin callPin
	defer pin.done()
	var cargs C.kvrgw_copy_args
	cargs.src_bucket.data, cargs.src_bucket.len = pin.str(args.SrcBucket)
	cargs.src_key.data, cargs.src_key.len = pin.str(args.SrcKey)
	cargs.dst_bucket.data, cargs.dst_bucket.len = pin.str(args.DstBucket)
	cargs.dst_key.data, cargs.dst_key.len = pin.str(args.DstKey)
	cargs.src_version_id.data, cargs.src_version_id.len = pin.str(args.SrcVersionID)
	cargs.if_match.data, cargs.if_match.len = pin.str(args.IfMatch)
	cargs.if_none_match.data, cargs.if_none_match.len = pin.str(args.IfNoneMatch)
	cargs.dst_if_match.data, cargs.dst_if_match.len = pin.str(args.DstIfMatch)
	cargs.dst_if_none_match.data, cargs.dst_if_none_match.len = pin.str(args.DstIfNoneMatch)
	cargs.content_type.data, cargs.content_type.len = pin.str(args.ContentType)
	if args.ReplaceMetadata {
		cargs.replace_metadata = 1
	}
	if n := len(args.Metadata); n > 0 {
		metaArr := pin.kvs(args.Metadata)
		cargs.metadata = &metaArr[0]
		cargs.metadata_count = C.size_t(n)
	}
	if args.ReplaceTags {
		cargs.replace_tags = 1
		if n := len(args.Tags); n > 0 {
			tagArr := pin.kvs(args.Tags)
			cargs.tags = &tagArr[0]
			cargs.tags_count = C.size_t(n)
		}
	}
	pin.p.Pin(&cargs)
	etagBuf := make([]byte, 48)
	vidBuf := make([]byte, 9)
	svidBuf := make([]byte, 9)
	var mtime C.int64_t
	ec := C.kvrgw_copy_object(h.h, C.uint32_t(tenantID), &cargs,
		(*C.char)(unsafe.Pointer(&etagBuf[0])), C.size_t(len(etagBuf)),
		&mtime,
		(*C.char)(unsafe.Pointer(&vidBuf[0])), C.size_t(len(vidBuf)),
		(*C.char)(unsafe.Pointer(&svidBuf[0])), C.size_t(len(svidBuf)))
	return CopyResult{
		ETag:             nzterm(etagBuf),
		LastModifiedUnix: int64(mtime),
		VersionID:        nzterm(vidBuf),
		SrcVersionID:     nzterm(svidBuf),
	}, int32(ec)
}

func (h *Handle) PutObjectTagging(tenantID uint32, bucket, key string, tags []KV) int32 {
	var pin callPin
	defer pin.done()
	b, bn := pin.str(bucket)
	k, kn := pin.str(key)
	tagArr := pin.kvs(tags)
	var tagPtr *C.kvrgw_kv
	if len(tagArr) > 0 {
		tagPtr = &tagArr[0]
	}
	return int32(C.kvrgw_put_object_tagging(h.h, C.uint32_t(tenantID), b, bn, k, kn, tagPtr, C.size_t(len(tags))))
}

func (h *Handle) GetObjectTagging(tenantID uint32, bucket, key string) ([]KV, int32) {
	b, bn := cstr(bucket)
	k, kn := cstr(key)
	tagCap := 16
	arenaSize := 8192
	for range 6 {
		var pin callPin
		tags := make([]C.kvrgw_kv, tagCap)
		arena := make([]byte, arenaSize)
		pin.p.Pin(&tags[0])
		ap, acap := pin.chars(arena)
		var n C.uint32_t
		ec := C.kvrgw_get_object_tagging(h.h, C.uint32_t(tenantID), b, bn, k, kn,
			&tags[0], C.uint32_t(len(tags)), &n, ap, acap)
		if int32(ec) != ErrInvalidArgument {
			if ec != C.KVRGW_ERR_OK {
				pin.done()
				return nil, int32(ec)
			}
			out := kvsFromC(tags, int(n))
			pin.done()
			return out, 0
		}
		if int(n) > tagCap {
			tagCap = int(n)
			pin.done()
			continue
		}
		arenaSize *= 2
		pin.done()
	}
	return nil, ErrInvalidArgument
}

func (h *Handle) DeleteObjectTagging(tenantID uint32, bucket, key string) int32 {
	b, bn := cstr(bucket)
	k, kn := cstr(key)
	return int32(C.kvrgw_delete_object_tagging(h.h, C.uint32_t(tenantID), b, bn, k, kn))
}
