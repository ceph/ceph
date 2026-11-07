package main

// Response-rewriting middleware.
//
// Vanilla versitygw differs from Ceph RGW (the behavior the s3-tests expect)
// in four places that the Backend interface cannot influence. Rather than
// patching versitygw, this middleware adjusts the response for exactly those
// requests. Everything else takes the fast exit and is untouched.
//
//	GET  ?tagging    200  sort <Tag> by key (versitygw ranges over a Go map)
//	PUT  ?tagging    400  BadRequest -> InvalidTag (more than 10 object tags)
//	GET  ?attributes 200  add <PartsCount> to <ObjectParts>
//	POST ?uploadId   400  InvalidRequest -> BadDigest (malformed checksum)

import (
	"bytes"
	"encoding/xml"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gofiber/fiber/v3"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// partsCountKey is the request user-value under which the backend stores the
// total number of parts for GetObjectAttributes.
const partsCountKey = "kvrgw.parts-count"

var xmlHeader = []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")

type rewriteKind uint8

const (
	rewriteNone rewriteKind = iota
	rewriteSortTags
	rewriteTagLimit
	rewritePartsCount
	rewriteBadDigest
)

// responseRewriteMiddleware is the only code on the hot path. It must stay
// small: a request without a query string returns after one length check,
// and every rewrite lives in its own function.
func responseRewriteMiddleware(c fiber.Ctx) error {
	if len(c.Request().URI().QueryString()) == 0 {
		return c.Next()
	}
	kind := classifyRequest(c)
	if kind == rewriteNone {
		return c.Next()
	}
	err := c.Next()
	applyRewrite(c, kind)
	return err
}

func classifyRequest(c fiber.Ctx) rewriteKind {
	q := c.Request().URI().QueryArgs()
	switch string(c.Request().Header.Method()) {
	case fiber.MethodGet:
		if q.Has("tagging") {
			return rewriteSortTags
		}
		if q.Has("attributes") {
			return rewritePartsCount
		}
	case fiber.MethodPut:
		if q.Has("tagging") {
			return rewriteTagLimit
		}
	case fiber.MethodPost:
		if q.Has("uploadId") {
			return rewriteBadDigest
		}
	}
	return rewriteNone
}

func applyRewrite(c fiber.Ctx, kind rewriteKind) {
	switch kind {
	case rewriteSortTags:
		sortTagsResponse(c)
	case rewriteTagLimit:
		fixTagLimitCode(c)
	case rewritePartsCount:
		addPartsCount(c)
	case rewriteBadDigest:
		fixBadDigest(c)
	}
}

func setResponseBody(c fiber.Ctx, body []byte) {
	c.Response().SetBody(body)
	c.Response().Header.SetContentLength(len(body))
}

// sortTagsResponse sorts the TagSet of a GetObjectTagging/GetBucketTagging
// response by key, as Ceph RGW (std::multimap) does.
func sortTagsResponse(c fiber.Ctx) {
	if c.Response().StatusCode() != fiber.StatusOK {
		return
	}
	var t s3response.Tagging
	if err := xml.Unmarshal(c.Response().Body(), &t); err != nil {
		return
	}
	tags := t.TagSet.Tags
	less := func(i, j int) bool { return tags[i].Key < tags[j].Key }
	if len(tags) < 2 || sort.SliceIsSorted(tags, less) {
		return
	}
	sort.SliceStable(tags, less)
	out, err := xml.Marshal(t)
	if err != nil {
		return
	}
	setResponseBody(c, append(append(make([]byte, 0, len(xmlHeader)+len(out)), xmlHeader...), out...))
}

// fixTagLimitCode turns versitygw's BadRequest for "more than 10 object tags"
// into InvalidTag, the code S3 and RGW return.
func fixTagLimitCode(c fiber.Ctx) {
	if c.Response().StatusCode() != fiber.StatusBadRequest {
		return
	}
	body := c.Response().Body()
	if !bytes.Contains(body, []byte(s3err.GetAPIError(s3err.ErrObjectTaggingLimited).Description)) {
		return
	}
	setResponseBody(c, bytes.Replace(body, []byte("<Code>BadRequest</Code>"), []byte("<Code>InvalidTag</Code>"), 1))
}

// addPartsCount inserts <PartsCount> into <ObjectParts>, using the count the
// backend stored on the request. s3response.ObjectParts has no such field.
func addPartsCount(c fiber.Ctx) {
	if c.Response().StatusCode() != fiber.StatusOK {
		return
	}
	n, ok := c.Locals(partsCountKey).(int)
	if !ok {
		return
	}
	body := c.Response().Body()
	const closeTag = "</ObjectParts>"
	i := bytes.LastIndex(body, []byte(closeTag))
	if i < 0 {
		return
	}
	ins := "<PartsCount>" + strconv.Itoa(n) + "</PartsCount>"
	out := make([]byte, 0, len(body)+len(ins))
	out = append(out, body[:i]...)
	out = append(out, ins...)
	out = append(out, body[i:]...)
	setResponseBody(c, out)
}

// fixBadDigest turns versitygw's InvalidRequest for a malformed whole-object
// checksum header on CompleteMultipartUpload into BadDigest, as RGW does. The
// other InvalidRequest causes for these headers are left alone: a value with
// more than one '-' and a non-numeric part-count suffix.
func fixBadDigest(c fiber.Ctx) {
	if c.Response().StatusCode() != fiber.StatusBadRequest {
		return
	}
	body := c.Response().Body()
	if !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) {
		return
	}
	for key, value := range c.Request().Header.All() {
		k := string(key)
		if !strings.HasPrefix(k, "X-Amz-Checksum-") {
			continue
		}
		switch k {
		case "X-Amz-Checksum-Type", "X-Amz-Checksum-Algorithm":
			continue
		}
		algo := types.ChecksumAlgorithm(strings.ToUpper(strings.TrimPrefix(k, "X-Amz-Checksum-")))
		desc := s3err.GetInvalidChecksumHeaderErr("x-amz-checksum-" + strings.ToLower(string(algo))).Description
		if !bytes.Contains(body, []byte(desc)) {
			continue
		}
		val := string(value)
		if algo != types.ChecksumAlgorithmCrc64nvme {
			parts := strings.Split(val, "-")
			if len(parts) > 2 {
				return
			}
			if len(parts) == 2 {
				if _, err := strconv.ParseInt(parts[1], 10, 32); err != nil {
					return
				}
				val = parts[0]
			}
		}
		if utils.IsValidChecksum(val, algo) {
			return
		}
		requestID, hostID := utils.EnsureRequestIDs(c)
		setResponseBody(c, s3err.GetAPIError(s3err.ErrBadDigest).XMLBody(requestID, hostID))
		return
	}
}
