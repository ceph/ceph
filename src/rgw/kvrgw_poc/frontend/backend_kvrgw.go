package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kv-poc/frontend/kvrgwc"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

type KvRgwBackend struct {
	backend.BackendUnsupported
	handle     *kvrgwc.Handle
	tenant     string
	tenantMu   sync.RWMutex
	tenantID   uint32
	hasID      bool
	rootAccess string
}

func NewKvRgwBackend(handle *kvrgwc.Handle, tenantName, rootAccess string) *KvRgwBackend {
	return &KvRgwBackend{handle: handle, tenant: tenantName, rootAccess: rootAccess}
}

func (b *KvRgwBackend) String() string { return "kv-rgw" }

func (b *KvRgwBackend) tenantName() string { return b.tenant }

func (b *KvRgwBackend) cachedTenantID() uint32 {
	b.tenantMu.RLock()
	defer b.tenantMu.RUnlock()
	return b.tenantID
}

func (b *KvRgwBackend) setTenantID(id uint32) {
	b.tenantMu.Lock()
	b.tenantID = id
	b.hasID = true
	b.tenantMu.Unlock()
}

func (b *KvRgwBackend) resolveTenantID(name string) (uint32, int32) {
	id, ec := b.handle.AddTenant(name)
	if ec == kvrgwc.ErrOK {
		return id, ec
	}
	if ec != kvrgwc.ErrTenantAlreadyExists {
		return 0, ec
	}
	exists, rid, rec := b.handle.ResolveTenant(name)
	if rec != kvrgwc.ErrOK {
		return 0, rec
	}
	if !exists {
		return 0, kvrgwc.ErrNoSuchTenant
	}
	return rid, ec
}

func (b *KvRgwBackend) EnsureTenant(ctx context.Context) {
	id, ec := b.resolveTenantID(b.tenant)
	if ec != kvrgwc.ErrOK && ec != kvrgwc.ErrTenantAlreadyExists {
		log.Printf("WARNING: AddTenant(%q) failed: error_code=%d", b.tenant, ec)
		return
	}
	if ec == kvrgwc.ErrTenantAlreadyExists {
		log.Printf("tenant %q already exists", b.tenant)
	}
	b.setTenantID(id)
	if ec == kvrgwc.ErrOK {
		log.Printf("tenant %q created (id=%d)", b.tenant, id)
	}
}

func (b *KvRgwBackend) AddTenant(ctx context.Context, tenantName string) (uint32, error) {
	id, ec := b.resolveTenantID(tenantName)
	if ec != kvrgwc.ErrOK && ec != kvrgwc.ErrTenantAlreadyExists {
		return 0, mapErrorCode(ec)
	}
	if tenantName == b.tenant {
		b.setTenantID(id)
	}
	return id, nil
}

// --- Bucket operations ---

func (b *KvRgwBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	maxBuckets := uint32(0)
	if input.MaxBuckets > 0 {
		maxBuckets = uint32(input.MaxBuckets)
	}
	resp, ec := b.handle.ListBuckets(b.cachedTenantID(), input.Prefix, input.ContinuationToken, maxBuckets)
	if err := mapErrorCode(ec); err != nil {
		return s3response.ListAllMyBucketsResult{}, err
	}
	var buckets []s3response.ListAllMyBucketsEntry
	for _, bkt := range resp.Buckets {
		buckets = append(buckets, s3response.ListAllMyBucketsEntry{
			Name:         bkt.Name,
			CreationDate: time.Unix(bkt.CreatedAtUnix, 0).UTC(),
		})
	}
	result := s3response.ListAllMyBucketsResult{
		Owner:   s3response.CanonicalUser{ID: "kv-rgw", DisplayName: "kv-rgw"},
		Buckets: s3response.ListAllMyBucketsList{Bucket: buckets},
	}
	if resp.NextToken != "" {
		result.ContinuationToken = resp.NextToken
	}
	return result, nil
}

func (b *KvRgwBackend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	exists, ec := b.handle.BucketExists(b.cachedTenantID(), deref(input.Bucket))
	if err := mapErrorCode(ec); err != nil {
		return nil, err
	}
	if !exists {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}
	return &s3.HeadBucketOutput{}, nil
}

func (b *KvRgwBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, _ []byte) error {
	return mapErrorCode(b.handle.CreateBucket(b.cachedTenantID(), deref(input.Bucket)))
}

func (b *KvRgwBackend) DeleteBucket(ctx context.Context, bucket string) error {
	return mapErrorCode(b.handle.DeleteBucket(b.cachedTenantID(), bucket))
}

func (b *KvRgwBackend) PutBucketAcl(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (b *KvRgwBackend) PutObjectAcl(_ context.Context, _ *s3.PutObjectAclInput) error {
	return nil
}

func (b *KvRgwBackend) GetBucketOwnershipControls(_ context.Context, _ string) (types.ObjectOwnership, error) {
	return types.ObjectOwnershipBucketOwnerEnforced, nil
}

func (b *KvRgwBackend) PutBucketOwnershipControls(_ context.Context, _ string, _ types.ObjectOwnership) error {
	return nil
}

func (b *KvRgwBackend) DeleteBucketOwnershipControls(_ context.Context, _ string) error {
	return nil
}

func (b *KvRgwBackend) GetObjectLockConfiguration(_ context.Context, _ string) ([]byte, error) {
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}

func (b *KvRgwBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	policy, ec := b.handle.GetBucketPolicy(b.cachedTenantID(), bucket)
	if err := mapErrorCode(ec); err != nil {
		return nil, err
	}
	if len(policy) == 0 {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucketPolicy)
	}
	return policy, nil
}

func (b *KvRgwBackend) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	return mapErrorCode(b.handle.PutBucketPolicy(b.cachedTenantID(), bucket, string(policy)))
}

func (b *KvRgwBackend) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	return mapErrorCode(b.handle.DeleteBucketPolicy(b.cachedTenantID(), bucket))
}

func (b *KvRgwBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	exists, ec := b.handle.BucketExistsCached(b.cachedTenantID(), deref(input.Bucket))
	if err := mapErrorCode(ec); err != nil {
		return nil, err
	}
	if !exists {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
	}

	type aclGrantee struct {
		Permission string     `json:"Permission"`
		Access     string     `json:"Access"`
		Type       types.Type `json:"Type"`
	}
	type aclData struct {
		Owner    string       `json:"Owner"`
		Grantees []aclGrantee `json:"Grantees"`
	}
	acl := aclData{
		Owner: b.rootAccess,
		Grantees: []aclGrantee{{
			Permission: "FULL_CONTROL",
			Access:     b.rootAccess,
			Type:       types.TypeCanonicalUser,
		}},
	}
	return json.Marshal(acl)
}

func (b *KvRgwBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	st, ec := b.handle.GetBucketVersioning(b.cachedTenantID(), bucket)
	if err := mapErrorCode(ec); err != nil {
		return s3response.GetBucketVersioningOutput{}, err
	}
	out := s3response.GetBucketVersioningOutput{}
	switch st {
	case kvrgwc.VersioningEnabled:
		v := types.BucketVersioningStatusEnabled
		out.Status = &v
	case kvrgwc.VersioningSuspended:
		v := types.BucketVersioningStatusSuspended
		out.Status = &v
	}
	return out, nil
}

func (b *KvRgwBackend) PutBucketVersioning(ctx context.Context, bucket string, st types.BucketVersioningStatus) error {
	var state uint8
	switch st {
	case types.BucketVersioningStatusEnabled:
		state = kvrgwc.VersioningEnabled
	case types.BucketVersioningStatusSuspended:
		state = kvrgwc.VersioningSuspended
	default:
		return mapErrorCode(kvrgwc.ErrInvalidArgument)
	}
	return mapErrorCode(b.handle.PutBucketVersioning(b.cachedTenantID(), bucket, state))
}

// --- Object operations ---

func (b *KvRgwBackend) PutObject(ctx context.Context, input s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	contentType := "application/octet-stream"
	if input.ContentType != nil && *input.ContentType != "" {
		contentType = *input.ContentType
	}

	var contentLength uint64
	if input.ContentLength != nil && *input.ContentLength > 0 {
		contentLength = uint64(*input.ContentLength)
	}

	var data []byte
	if input.Body != nil {
		var err error
		data, err = io.ReadAll(input.Body)
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	ifMatch := ""
	if input.IfMatch != nil && *input.IfMatch != "" {
		ifMatch = strings.Trim(*input.IfMatch, `"`)
	}
	ifNoneMatch := ""
	if input.IfNoneMatch != nil && *input.IfNoneMatch != "" {
		ifNoneMatch = strings.Trim(*input.IfNoneMatch, `"`)
	}
	var tags []kvrgwc.KV
	if input.Tagging != nil && *input.Tagging != "" {
		tags = parseTaggingHeader(*input.Tagging)
		if len(tags) == 0 {
			return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidTagKey)
		}
	}
	metadata := mapToKV(input.Metadata)

	etag, versionID, ec := b.handle.PutObject(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		data, contentType, contentLength, ifMatch, ifNoneMatch, tags, metadata)
	if err := mapErrorCode(ec); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	if etag != "" {
		etag = fmt.Sprintf(`"%s"`, etag)
	}
	return s3response.PutObjectOutput{ETag: etag, VersionID: versionIDFromProto(versionID)}, nil
}

func (b *KvRgwBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	versionID := ""
	if input.VersionId != nil && *input.VersionId != "" {
		versionID = versionIDToInternal(*input.VersionId)
	}

	var br *kvrgwc.ByteRange
	rangeRequested := false
	if input.Range != nil && *input.Range != "" {
		parsed, err := parseRange(*input.Range)
		if err == nil {
			br = parsed
			rangeRequested = true
		}
	}

	var partNumber uint16
	partNumberRequested := false
	if input.PartNumber != nil {
		partNumber = uint16(*input.PartNumber)
		partNumberRequested = true
	}

	res, ec := b.handle.GetObject(b.cachedTenantID(), deref(input.Bucket), deref(input.Key), versionID, br, partNumber)
	if err := mapErrorCode(ec); err != nil {
		if _, _, ok := parseDeleteMarkerDetail([]byte(res.Meta.ErrorDetail)); ok {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return nil, err
	}

	size := int64(res.Meta.Size)
	lastMod := time.Unix(res.Meta.LastModifiedUnix, 0).UTC()
	etag := fmt.Sprintf(`"%s"`, res.Meta.ETag)
	ct := res.Meta.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}

	out := &s3.GetObjectOutput{
		Body:         io.NopCloser(bytes.NewReader(res.Body)),
		ContentType:  &ct,
		ETag:         &etag,
		LastModified: &lastMod,
	}

	if vid := res.Meta.VersionID; vid != "" {
		extVid := versionIDToExternal(uint32(mustParseUint(vid)))
		out.VersionId = &extVid
	}
	if m := kvToMap(res.Metadata); len(m) > 0 {
		out.Metadata = m
	}
	if tc := res.Meta.TagsCount; tc > 0 {
		tagCount := int32(tc)
		out.TagCount = &tagCount
	}
	if partNumberRequested && res.Meta.PartsCount > 0 {
		pc := int32(res.Meta.PartsCount)
		out.PartsCount = &pc
	}

	if rangeRequested && br != nil {
		rangeLen := rangeContentLength(br, size)
		out.ContentLength = &rangeLen
		cr := formatContentRange(br, size)
		out.ContentRange = &cr
	} else {
		out.ContentLength = &size
	}

	return out, nil
}

func (b *KvRgwBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	versionID := ""
	if input.VersionId != nil && *input.VersionId != "" {
		versionID = versionIDToInternal(*input.VersionId)
	}
	var partNumber uint16
	partNumberRequested := false
	if input.PartNumber != nil {
		partNumber = uint16(*input.PartNumber)
		partNumberRequested = true
	}

	res, ec := b.handle.HeadObject(b.cachedTenantID(), deref(input.Bucket), deref(input.Key), versionID, partNumber)
	if err := mapErrorCode(ec); err != nil {
		if vid, mtime, ok := parseDeleteMarkerDetail([]byte(res.Meta.ErrorDetail)); ok {
			out := &s3.HeadObjectOutput{LastModified: &mtime}
			if vid != "" {
				out.VersionId = &vid
			}
			return out, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return nil, err
	}

	size := int64(res.Meta.Size)
	lastMod := time.Unix(res.Meta.LastModifiedUnix, 0).UTC()
	etag := fmt.Sprintf(`"%s"`, res.Meta.ETag)
	ct := res.Meta.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}

	out := &s3.HeadObjectOutput{
		ContentLength: &size,
		ContentType:   &ct,
		ETag:          &etag,
		LastModified:  &lastMod,
	}
	if vid := res.Meta.VersionID; vid != "" {
		extVid := versionIDToExternal(uint32(mustParseUint(vid)))
		out.VersionId = &extVid
	}
	if tc := res.Meta.TagsCount; tc > 0 {
		tagCount := int32(tc)
		out.TagCount = &tagCount
	}
	if partNumberRequested && res.Meta.PartsCount > 0 {
		pc := int32(res.Meta.PartsCount)
		out.PartsCount = &pc
	}
	if m := kvToMap(res.Metadata); len(m) > 0 {
		out.Metadata = m
	}
	return out, nil
}

func (b *KvRgwBackend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	versionID := ""
	if input.VersionId != nil && *input.VersionId != "" {
		versionID = versionIDToInternal(*input.VersionId)
	}
	maxParts := uint32(1000)
	if input.MaxParts != nil && *input.MaxParts > 0 {
		maxParts = uint32(*input.MaxParts)
	}
	var partNumberMarker uint16
	if input.PartNumberMarker != nil && *input.PartNumberMarker != "" {
		if v, err := strconv.ParseUint(*input.PartNumberMarker, 10, 16); err == nil {
			partNumberMarker = uint16(v)
		}
	}

	res, ec := b.handle.GetObjectAttributes(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		versionID, partNumberMarker, maxParts)
	if err := mapErrorCode(ec); err != nil {
		if vid, mtime, ok := parseDeleteMarkerDetail([]byte(res.Meta.ErrorDetail)); ok {
			out := s3response.GetObjectAttributesResponse{LastModified: &mtime}
			if vid != "" {
				out.VersionId = &vid
			}
			dm := true
			out.DeleteMarker = &dm
			return out, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}
		return s3response.GetObjectAttributesResponse{}, err
	}

	size := int64(res.Meta.Size)
	etag := res.Meta.ETag
	out := s3response.GetObjectAttributesResponse{
		ETag:         &etag,
		ObjectSize:   &size,
		StorageClass: types.StorageClassStandard,
	}
	if vid := res.Meta.VersionID; vid != "" {
		extVid := versionIDToExternal(uint32(mustParseUint(vid)))
		out.VersionId = &extVid
	}
	if res.HasParts {
		parts := make([]types.ObjectPart, 0, len(res.Parts))
		for _, p := range res.Parts {
			pn := int32(p.PartNumber)
			sz := int64(p.Size)
			parts = append(parts, types.ObjectPart{
				PartNumber: &pn,
				Size:       &sz,
			})
		}
		objParts := &s3response.ObjectParts{
			PartNumberMarker: int(partNumberMarker),
			MaxParts:         int(maxParts),
			IsTruncated:      res.Truncated,
			Parts:            parts,
			PartsCount:       int(res.TotalPartsCount),
		}
		if res.Truncated {
			objParts.NextPartNumberMarker = int(res.NextPartNumberMarker)
		}
		out.ObjectParts = objParts
	}
	return out, nil
}

func (b *KvRgwBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	ifMatch := ""
	if input.IfMatch != nil && *input.IfMatch != "" {
		ifMatch = strings.Trim(*input.IfMatch, `"`)
	}
	var mtime int64
	if input.IfMatchLastModifiedTime != nil {
		mtime = input.IfMatchLastModifiedTime.Unix()
	}
	var matchSize int64
	hasSize := false
	if input.IfMatchSize != nil {
		matchSize = *input.IfMatchSize
		hasSize = true
	}

	if input.VersionId != nil && *input.VersionId != "" {
		internal := versionIDToInternal(*input.VersionId)
		if _, err := parseVersionID(internal); err != nil {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchVersion)
		}
		ec := b.handle.DeleteObjectVersion(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
			internal, ifMatch, mtime, matchSize, hasSize)
		if err := mapErrorCode(ec); err != nil {
			return nil, err
		}
		return &s3.DeleteObjectOutput{VersionId: input.VersionId}, nil
	}

	res, ec := b.handle.DeleteObject(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		ifMatch, mtime, matchSize, hasSize)
	if err := mapErrorCode(ec); err != nil {
		return nil, err
	}

	if res.CreatedDM {
		dm := true
		vid := versionIDToExternal(res.DMVersionID)
		if vid == "" {
			vid = "null"
		}
		return &s3.DeleteObjectOutput{DeleteMarker: &dm, VersionId: &vid}, nil
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (b *KvRgwBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if len(input.Delete.Objects) > 1000 {
		return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrMalformedXML)
	}
	hasVersions := false
	for _, obj := range input.Delete.Objects {
		if obj.VersionId != nil && *obj.VersionId != "" {
			hasVersions = true
			break
		}
	}

	var outs []kvrgwc.DeleteMultiOut
	var ec int32
	if hasVersions {
		objects := make([]kvrgwc.DeleteMultiObject, 0, len(input.Delete.Objects))
		for _, obj := range input.Delete.Objects {
			dmo := kvrgwc.DeleteMultiObject{Key: deref(obj.Key)}
			if obj.VersionId != nil && *obj.VersionId != "" {
				dmo.VersionID = versionIDToInternal(*obj.VersionId)
			}
			objects = append(objects, dmo)
		}
		outs, ec = b.handle.DeleteMulti(b.cachedTenantID(), deref(input.Bucket), nil, objects)
	} else {
		keys := make([]string, 0, len(input.Delete.Objects))
		for _, obj := range input.Delete.Objects {
			keys = append(keys, deref(obj.Key))
		}
		outs, ec = b.handle.DeleteMulti(b.cachedTenantID(), deref(input.Bucket), keys, nil)
	}
	if err := mapErrorCode(ec); err != nil {
		return s3response.DeleteResult{}, err
	}

	var result s3response.DeleteResult
	for _, d := range outs {
		if d.Error {
			code := d.ErrCode
			if code == "" {
				code = "InternalError"
			}
			k := d.Key
			msg := d.ErrMsg
			result.Error = append(result.Error, types.Error{
				Key:     &k,
				Code:    &code,
				Message: &msg,
			})
			continue
		}
		k := d.Key
		obj := types.DeletedObject{Key: &k}
		if d.VersionID != "" {
			vid := d.VersionID
			obj.VersionId = &vid
		}
		if d.CreatedDM {
			dm := true
			obj.DeleteMarker = &dm
			extDmVid := versionIDToExternal(d.DMVersionID)
			obj.DeleteMarkerVersionId = &extDmVid
		}
		result.Deleted = append(result.Deleted, obj)
	}
	return result, nil
}

// --- List operations ---

func (b *KvRgwBackend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	if input.MaxKeys != nil && *input.MaxKeys == 0 {
		bucket := deref(input.Bucket)
		isTruncated := false
		keyCount := int32(0)
		return s3response.ListObjectsV2Result{
			Name:        &bucket,
			Prefix:      input.Prefix,
			MaxKeys:     input.MaxKeys,
			IsTruncated: &isTruncated,
			KeyCount:    &keyCount,
		}, nil
	}
	prefix := deref(input.Prefix)
	delimiter := deref(input.Delimiter)
	var maxKeys uint32
	if input.MaxKeys != nil {
		maxKeys = uint32(*input.MaxKeys)
	}
	continuation := deref(input.ContinuationToken)
	if continuation == "" {
		continuation = deref(input.StartAfter)
	}

	resp, ec := b.handle.ListObjects(b.cachedTenantID(), deref(input.Bucket), prefix, delimiter, continuation, "", maxKeys)
	if err := mapErrorCode(ec); err != nil {
		return s3response.ListObjectsV2Result{}, err
	}

	var contents []s3response.Object
	for _, obj := range resp.Objects {
		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		key := obj.Key
		size := int64(obj.Size)
		lastMod := time.Unix(obj.LastModifiedUnix, 0).UTC()
		contents = append(contents, s3response.Object{
			ETag:         &etag,
			Key:          &key,
			Size:         &size,
			LastModified: &lastMod,
		})
	}

	var commonPrefixes []types.CommonPrefix
	for _, cp := range resp.CommonPrefixes {
		p := cp
		commonPrefixes = append(commonPrefixes, types.CommonPrefix{Prefix: &p})
	}

	isTruncated := resp.Truncated
	keyCount := int32(len(contents))
	bucket := deref(input.Bucket)

	result := s3response.ListObjectsV2Result{
		Name:           &bucket,
		Prefix:         input.Prefix,
		MaxKeys:        input.MaxKeys,
		IsTruncated:    &isTruncated,
		KeyCount:       &keyCount,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}
	if input.Delimiter != nil && *input.Delimiter != "" {
		result.Delimiter = input.Delimiter
	}
	if input.StartAfter != nil {
		result.StartAfter = input.StartAfter
	}
	if input.ContinuationToken != nil {
		result.ContinuationToken = input.ContinuationToken
	}
	if isTruncated {
		token := resp.NextToken
		result.NextContinuationToken = &token
	}

	return result, nil
}

func (b *KvRgwBackend) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	if input.MaxKeys != nil && *input.MaxKeys == 0 {
		bucket := deref(input.Bucket)
		isTruncated := false
		return s3response.ListObjectsResult{
			Name:        &bucket,
			Prefix:      input.Prefix,
			Marker:      input.Marker,
			MaxKeys:     input.MaxKeys,
			IsTruncated: &isTruncated,
		}, nil
	}
	prefix := deref(input.Prefix)
	delimiter := deref(input.Delimiter)
	var maxKeys uint32
	if input.MaxKeys != nil {
		maxKeys = uint32(*input.MaxKeys)
	}
	continuation := deref(input.Marker)

	resp, ec := b.handle.ListObjects(b.cachedTenantID(), deref(input.Bucket), prefix, delimiter, continuation, "", maxKeys)
	if err := mapErrorCode(ec); err != nil {
		return s3response.ListObjectsResult{}, err
	}

	var contents []s3response.Object
	for _, obj := range resp.Objects {
		etag := fmt.Sprintf(`"%s"`, obj.ETag)
		key := obj.Key
		size := int64(obj.Size)
		lastMod := time.Unix(obj.LastModifiedUnix, 0).UTC()
		contents = append(contents, s3response.Object{
			ETag:         &etag,
			Key:          &key,
			Size:         &size,
			LastModified: &lastMod,
		})
	}

	var commonPrefixes []types.CommonPrefix
	for _, cp := range resp.CommonPrefixes {
		p := cp
		commonPrefixes = append(commonPrefixes, types.CommonPrefix{Prefix: &p})
	}

	isTruncated := resp.Truncated
	bucket := deref(input.Bucket)

	result := s3response.ListObjectsResult{
		Name:           &bucket,
		Prefix:         input.Prefix,
		Marker:         input.Marker,
		MaxKeys:        input.MaxKeys,
		IsTruncated:    &isTruncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}
	if input.Delimiter != nil && *input.Delimiter != "" {
		result.Delimiter = input.Delimiter
	}
	if isTruncated {
		if len(contents) > 0 {
			result.NextMarker = contents[len(contents)-1].Key
		} else if len(commonPrefixes) > 0 {
			result.NextMarker = commonPrefixes[len(commonPrefixes)-1].Prefix
		}
	}

	return result, nil
}

func (b *KvRgwBackend) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	prefix := deref(input.Prefix)
	var maxKeys uint32
	if input.MaxKeys != nil {
		maxKeys = uint32(*input.MaxKeys)
	}
	keyMarker := deref(input.KeyMarker)
	var versionIDMarker uint32
	if input.VersionIdMarker != nil {
		vid, err := parseVersionID(versionIDToInternal(*input.VersionIdMarker))
		if err == nil {
			versionIDMarker = vid
		}
	}

	resp, ec := b.handle.ListObjectVersions(b.cachedTenantID(), deref(input.Bucket), prefix, keyMarker, maxKeys, versionIDMarker)
	if err := mapErrorCode(ec); err != nil {
		return s3response.ListVersionsResult{}, err
	}

	var versions []s3response.ObjectVersion
	for _, v := range resp.Versions {
		if v.IsDeleteMarker {
			continue
		}
		key := v.Key
		size := int64(v.Size)
		etag := fmt.Sprintf(`"%s"`, v.ETag)
		lastMod := time.Unix(v.LastModifiedUnix, 0).UTC()
		isLatest := v.IsLatest
		vid := versionIDToExternal(v.VersionID)
		versions = append(versions, s3response.ObjectVersion{
			Key:          &key,
			Size:         &size,
			ETag:         &etag,
			LastModified: &lastMod,
			IsLatest:     &isLatest,
			VersionId:    &vid,
		})
	}

	var deleteMarkers []types.DeleteMarkerEntry
	for _, v := range resp.Versions {
		if !v.IsDeleteMarker {
			continue
		}
		key := v.Key
		lastMod := time.Unix(v.LastModifiedUnix, 0).UTC()
		isLatest := v.IsLatest
		vid := versionIDToExternal(v.VersionID)
		deleteMarkers = append(deleteMarkers, types.DeleteMarkerEntry{
			Key:          &key,
			LastModified: &lastMod,
			IsLatest:     &isLatest,
			VersionId:    &vid,
		})
	}

	isTruncated := resp.Truncated
	bucket := deref(input.Bucket)

	if versions == nil {
		versions = []s3response.ObjectVersion{}
	}
	if deleteMarkers == nil {
		deleteMarkers = []types.DeleteMarkerEntry{}
	}

	result := s3response.ListVersionsResult{
		Name:          &bucket,
		Prefix:        input.Prefix,
		KeyMarker:     input.KeyMarker,
		MaxKeys:       input.MaxKeys,
		IsTruncated:   &isTruncated,
		Versions:      versions,
		DeleteMarkers: deleteMarkers,
	}
	if isTruncated {
		nkm := resp.NextKey
		result.NextKeyMarker = &nkm
		nvid := versionIDToExternal(resp.NextVersionID)
		result.NextVersionIdMarker = &nvid
	}

	return result, nil
}

func (b *KvRgwBackend) CopyObject(ctx context.Context, input s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	srcBucket, srcKey, srcVersionId := parseCopySource(deref(input.CopySource))

	args := kvrgwc.CopyArgs{
		SrcBucket:    srcBucket,
		SrcKey:       srcKey,
		DstBucket:    deref(input.Bucket),
		DstKey:       deref(input.Key),
		SrcVersionID: versionIDToInternal(srcVersionId),
	}
	if srcVersionId == "" {
		args.SrcVersionID = ""
	}
	if input.CopySourceIfMatch != nil {
		args.IfMatch = strings.Trim(*input.CopySourceIfMatch, `"`)
	}
	if input.CopySourceIfNoneMatch != nil {
		args.IfNoneMatch = strings.Trim(*input.CopySourceIfNoneMatch, `"`)
	}
	if input.MetadataDirective == types.MetadataDirectiveReplace {
		args.ReplaceMetadata = true
		if input.ContentType != nil {
			args.ContentType = *input.ContentType
		}
		args.Metadata = mapToKV(input.Metadata)
	}
	if input.TaggingDirective == types.TaggingDirectiveReplace {
		args.ReplaceTags = true
		if input.Tagging == nil || *input.Tagging == "" {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidTagKey)
		}
		args.Tags = parseTaggingHeader(*input.Tagging)
		if len(args.Tags) == 0 {
			return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrInvalidTagKey)
		}
	}

	resp, ec := b.handle.CopyObject(b.cachedTenantID(), args)
	if err := mapErrorCode(ec); err != nil {
		return s3response.CopyObjectOutput{}, err
	}

	lastMod := time.Unix(resp.LastModifiedUnix, 0).UTC()
	etag := fmt.Sprintf(`"%s"`, resp.ETag)
	result := s3response.CopyObjectOutput{
		CopyObjectResult: &s3response.CopyObjectResult{
			ETag:         &etag,
			LastModified: &lastMod,
		},
	}
	if vid := resp.VersionID; vid != "" {
		extVid := versionIDFromProto(vid)
		result.VersionId = &extVid
	}
	if svid := resp.SrcVersionID; svid != "" {
		extSvid := versionIDFromProto(svid)
		result.CopySourceVersionId = &extSvid
	}
	return result, nil
}

func (b *KvRgwBackend) PutObjectTagging(ctx context.Context, bucket, object, versionId string, tags map[string]string) error {
	return mapErrorCode(b.handle.PutObjectTagging(b.cachedTenantID(), bucket, object, mapToKV(tags)))
}

func (b *KvRgwBackend) GetObjectTagging(ctx context.Context, bucket, object, versionId string) (map[string]string, error) {
	tags, ec := b.handle.GetObjectTagging(b.cachedTenantID(), bucket, object)
	if err := mapErrorCode(ec); err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return nil, nil
	}
	return kvToMap(tags), nil
}

func (b *KvRgwBackend) DeleteObjectTagging(ctx context.Context, bucket, object, versionId string) error {
	return mapErrorCode(b.handle.DeleteObjectTagging(b.cachedTenantID(), bucket, object))
}

// --- Multipart upload operations ---

func (b *KvRgwBackend) CreateMultipartUpload(ctx context.Context, input s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	contentType := "application/octet-stream"
	if input.ContentType != nil && *input.ContentType != "" {
		contentType = *input.ContentType
	}
	var tags []kvrgwc.KV
	if input.Tagging != nil && *input.Tagging != "" {
		tags = parseTaggingHeader(*input.Tagging)
		if len(tags) == 0 {
			return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrInvalidTagKey)
		}
	}
	metadata := mapToKV(input.Metadata)

	uploadID, ec := b.handle.InitiateMultipartUpload(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		contentType, tags, metadata)
	if err := mapErrorCode(ec); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}
	return s3response.InitiateMultipartUploadResult{
		Bucket:   deref(input.Bucket),
		Key:      deref(input.Key),
		UploadId: uploadID,
	}, nil
}

func (b *KvRgwBackend) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	var data []byte
	if input.Body != nil {
		var err error
		data, err = io.ReadAll(input.Body)
		if err != nil {
			return nil, err
		}
	}
	partNumber := uint16(0)
	if input.PartNumber != nil {
		partNumber = uint16(*input.PartNumber)
	}

	etag, ec := b.handle.UploadPart(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		deref(input.UploadId), partNumber, data)
	if err := mapErrorCode(ec); err != nil {
		return nil, err
	}
	etag = fmt.Sprintf(`"%s"`, etag)
	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (b *KvRgwBackend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	var parts []kvrgwc.CompletedPart
	if input.MultipartUpload != nil {
		parts = make([]kvrgwc.CompletedPart, 0, len(input.MultipartUpload.Parts))
		for _, p := range input.MultipartUpload.Parts {
			var partNumber uint16
			if p.PartNumber != nil {
				partNumber = uint16(*p.PartNumber)
			}
			etag := ""
			if p.ETag != nil {
				etag = strings.Trim(*p.ETag, `"`)
			}
			parts = append(parts, kvrgwc.CompletedPart{PartNumber: partNumber, ETag: etag})
		}
	}

	ifMatch := ""
	if input.IfMatch != nil && *input.IfMatch != "" {
		ifMatch = strings.Trim(*input.IfMatch, `"`)
	}
	ifNoneMatch := ""
	if input.IfNoneMatch != nil && *input.IfNoneMatch != "" {
		ifNoneMatch = strings.Trim(*input.IfNoneMatch, `"`)
	}

	etag, versionID, ec := b.handle.CompleteMultipartUpload(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		deref(input.UploadId), parts, ifMatch, ifNoneMatch)
	if err := mapErrorCode(ec); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	etag = fmt.Sprintf(`"%s"`, etag)
	bucket := deref(input.Bucket)
	key := deref(input.Key)
	result := s3response.CompleteMultipartUploadResult{
		Bucket: &bucket,
		Key:    &key,
		ETag:   &etag,
	}
	return result, versionIDFromProto(versionID), nil
}

func (b *KvRgwBackend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	return mapErrorCode(b.handle.AbortMultipartUpload(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		deref(input.UploadId)))
}

func (b *KvRgwBackend) ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	maxUploads := uint32(1000)
	if input.MaxUploads != nil && *input.MaxUploads > 0 {
		maxUploads = uint32(*input.MaxUploads)
	}

	resp, ec := b.handle.ListMultipartUploads(b.cachedTenantID(), deref(input.Bucket), deref(input.Prefix),
		deref(input.KeyMarker), deref(input.UploadIdMarker), maxUploads)
	if err := mapErrorCode(ec); err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}

	owner := s3response.Owner{ID: b.rootAccess, DisplayName: b.rootAccess}
	uploads := make([]s3response.Upload, 0, len(resp.Uploads))
	for _, u := range resp.Uploads {
		uploads = append(uploads, s3response.Upload{
			Key:       u.Key,
			UploadID:  u.UploadID,
			Initiator: s3response.Initiator(owner),
			Owner:     owner,
			Initiated: time.Unix(u.InitiatedUnix, 0).UTC(),
		})
	}

	result := s3response.ListMultipartUploadsResult{
		Bucket:         deref(input.Bucket),
		KeyMarker:      deref(input.KeyMarker),
		UploadIDMarker: deref(input.UploadIdMarker),
		Delimiter:      deref(input.Delimiter),
		Prefix:         deref(input.Prefix),
		MaxUploads:     int(maxUploads),
		IsTruncated:    resp.Truncated,
		Uploads:        uploads,
	}
	if resp.Truncated {
		result.NextKeyMarker = resp.NextKeyMarker
		result.NextUploadIDMarker = resp.NextUploadIDMarker
	}
	return result, nil
}

func (b *KvRgwBackend) ListParts(ctx context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	maxParts := uint32(1000)
	if input.MaxParts != nil && *input.MaxParts > 0 {
		maxParts = uint32(*input.MaxParts)
	}
	var partNumberMarker uint16
	if input.PartNumberMarker != nil && *input.PartNumberMarker != "" {
		if v, err := strconv.ParseUint(*input.PartNumberMarker, 10, 16); err == nil {
			partNumberMarker = uint16(v)
		}
	}

	resp, ec := b.handle.ListParts(b.cachedTenantID(), deref(input.Bucket), deref(input.Key),
		deref(input.UploadId), partNumberMarker, maxParts)
	if err := mapErrorCode(ec); err != nil {
		return s3response.ListPartsResult{}, err
	}

	owner := s3response.Owner{ID: b.rootAccess, DisplayName: b.rootAccess}
	parts := make([]s3response.Part, 0, len(resp.Parts))
	for _, p := range resp.Parts {
		etag := fmt.Sprintf(`"%s"`, p.ETag)
		parts = append(parts, s3response.Part{
			PartNumber:   int(p.PartNumber),
			ETag:         etag,
			Size:         int64(p.Size),
			LastModified: time.Unix(p.LastModifiedUnix, 0).UTC(),
		})
	}

	result := s3response.ListPartsResult{
		Bucket:           deref(input.Bucket),
		Key:              deref(input.Key),
		UploadID:         deref(input.UploadId),
		PartNumberMarker: int(partNumberMarker),
		MaxParts:         int(maxParts),
		IsTruncated:      resp.Truncated,
		Parts:            parts,
		Initiator:        s3response.Initiator(owner),
		Owner:            owner,
	}
	if resp.Truncated {
		result.NextPartNumberMarker = int(resp.NextPartNumberMarker)
	}
	return result, nil
}

func parseCopySource(src string) (bucket, key, versionId string) {
	src = strings.TrimPrefix(src, "/")
	if idx := strings.Index(src, "?versionId="); idx >= 0 {
		versionId = src[idx+len("?versionId="):]
		src = src[:idx]
	}
	parts := strings.SplitN(src, "/", 2)
	if len(parts) == 2 {
		bucket = parts[0]
		key = parts[1]
	}
	return
}

// --- Error mapping ---

func parseDeleteMarkerDetail(detail []byte) (versionID string, mtime time.Time, ok bool) {
	s := string(detail)
	if !strings.HasPrefix(s, "DeleteMarker:") {
		return "", time.Time{}, false
	}
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return "", time.Time{}, false
	}
	vid := parts[1]
	if vid != "" && vid != fmt.Sprintf("%08x", nullVersionInternal) {
		versionID = versionIDToExternal(uint32(mustParseUint(vid)))
	}
	sec, _ := strconv.ParseInt(parts[2], 10, 64)
	return versionID, time.Unix(sec, 0).UTC(), true
}

func mapErrorCode(code int32) error {
	switch code {
	case kvrgwc.ErrOK:
		return nil
	case kvrgwc.ErrNoSuchKey:
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	case kvrgwc.ErrNoSuchBucket:
		return s3err.GetAPIError(s3err.ErrNoSuchBucket)
	case kvrgwc.ErrBucketAlreadyExists:
		return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	case kvrgwc.ErrBucketNotEmpty:
		return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	case kvrgwc.ErrPreconditionFailed:
		return s3err.GetAPIError(s3err.ErrPreconditionFailed)
	case kvrgwc.ErrAccessDenied:
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	case kvrgwc.ErrInvalidArgument:
		return s3err.InvalidArgumentError{Description: "Invalid Argument"}
	case kvrgwc.ErrInvalidRequest:
		return s3err.GetAPIError(s3err.ErrInvalidRequest)
	case kvrgwc.ErrInvalidTag:
		return s3err.GetAPIError(s3err.ErrInvalidTagKey)
	case kvrgwc.ErrInvalidBucketName:
		return s3err.GetAPIError(s3err.ErrInvalidBucketName)
	case kvrgwc.ErrInvalidRange:
		return s3err.GetAPIError(s3err.ErrInvalidRange)
	case kvrgwc.ErrNoSuchVersion:
		return s3err.GetAPIError(s3err.ErrNoSuchKey)
	case kvrgwc.ErrNoSuchTenant:
		return s3err.GetAPIError(s3err.ErrInternalError)
	case kvrgwc.ErrNoSuchUpload:
		return s3err.GetAPIError(s3err.ErrNoSuchUpload)
	case kvrgwc.ErrInvalidPart:
		return s3err.GetAPIError(s3err.ErrInvalidPart)
	case kvrgwc.ErrInvalidPartOrder:
		return s3err.GetAPIError(s3err.ErrInvalidPartOrder)
	case kvrgwc.ErrEntityTooSmall:
		return s3err.GetAPIError(s3err.ErrEntityTooSmall)
	case kvrgwc.ErrEntityTooLarge:
		return s3err.GetAPIError(s3err.ErrEntityTooLarge)
	case kvrgwc.ErrUploadPartBusy, kvrgwc.ErrUploadInProgress:
		// Both are "retry shortly" conditions (fixing_agent_mess.md §3/§5) — no
		// dedicated S3 error code for either, SlowDown/503 is the closest match.
		return s3err.GetAPIError(s3err.ErrSlowDown)
	default:
		return s3err.GetAPIError(s3err.ErrInternalError)
	}
}

// --- Helpers ---

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func mapToKV(m map[string]string) []kvrgwc.KV {
	if len(m) == 0 {
		return nil
	}
	out := make([]kvrgwc.KV, 0, len(m))
	for k, v := range m {
		out = append(out, kvrgwc.KV{Key: k, Value: v})
	}
	return out
}

func kvToMap(kvs []kvrgwc.KV) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

func parseTaggingHeader(header string) []kvrgwc.KV {
	var tags []kvrgwc.KV
	for _, pair := range strings.Split(header, "&") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		key := kv[0]
		if key == "" {
			continue
		}
		value := ""
		if len(kv) == 2 {
			value = kv[1]
		}
		tags = append(tags, kvrgwc.KV{Key: key, Value: value})
	}
	return tags
}

func parseVersionID(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

const nullVersionInternal uint32 = math.MaxUint32

func versionIDToInternal(s string) string {
	if s == "null" {
		return fmt.Sprintf("%08x", nullVersionInternal)
	}
	return s
}

func versionIDToExternal(vid uint32) string {
	if vid == nullVersionInternal {
		return "null"
	}
	return fmt.Sprintf("%08x", vid)
}

func versionIDFromProto(s string) string {
	if s == "" {
		return ""
	}
	if s == fmt.Sprintf("%08x", nullVersionInternal) {
		return "null"
	}
	return s
}

func mustParseUint(s string) uint64 {
	v, _ := strconv.ParseUint(s, 16, 32)
	return v
}

func parseRange(rangeHeader string) (*kvrgwc.ByteRange, error) {
	if len(rangeHeader) < 7 || rangeHeader[:6] != "bytes=" {
		return nil, fmt.Errorf("invalid range header")
	}
	spec := rangeHeader[6:]

	br := &kvrgwc.ByteRange{}

	if spec[0] == '-' {
		var n int64
		if _, err := fmt.Sscanf(spec, "-%d", &n); err != nil {
			return nil, err
		}
		br.FromEnd = true
		br.End = n
		return br, nil
	}

	var start, end int64
	n, _ := fmt.Sscanf(spec, "%d-%d", &start, &end)
	br.Start = start
	if n == 1 {
		br.EndUnbounded = true
	} else {
		br.End = end
	}
	return br, nil
}

func rangeContentLength(br *kvrgwc.ByteRange, totalSize int64) int64 {
	if br.FromEnd {
		if br.End > totalSize {
			return totalSize
		}
		return br.End
	}
	end := br.End
	if br.EndUnbounded {
		end = totalSize - 1
	}
	if end >= totalSize {
		end = totalSize - 1
	}
	return end - br.Start + 1
}

func formatContentRange(br *kvrgwc.ByteRange, totalSize int64) string {
	var start, end int64
	if br.FromEnd {
		length := br.End
		if length > totalSize {
			length = totalSize
		}
		start = totalSize - length
		end = totalSize - 1
	} else {
		start = br.Start
		end = br.End
		if br.EndUnbounded {
			end = totalSize - 1
		}
		if end >= totalSize {
			end = totalSize - 1
		}
	}
	return fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize)
}
