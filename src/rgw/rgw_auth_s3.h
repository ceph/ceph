// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_AUTH_S3_H
#define CEPH_RGW_AUTH_S3_H


#include "rgw_common.h"

void rgw_create_s3_canonical_header(const char *method,
				    const char *content_md5,
				    const char *content_type, const char *date,
				    map<string, string>& meta_map,
				    const char *request_uri,
				    map<string, string>& sub_resources,
				    string& dest_str);
bool rgw_create_s3_canonical_header(req_info& info, utime_t *header_time,
				    string& dest, bool qsr);
int rgw_get_s3_header_digest(const string& auth_hdr, const string& key,
			     string& dest);
int rgw_get_s3_header_digest(const string& auth_hdr, const string& key, string& dest);

void rgw_hash_s3_string_sha256(const char *data, int len, string& dest);
void rgw_create_s3_v4_canonical_request(struct req_state *s, const string& canonical_uri,
                                        const string& canonical_qs, const string& canonical_hdrs,
                                        const string& signed_hdrs, const string& request_payload,
                                        bool unsigned_payload,
                                        string& canonical_req, string& canonical_req_hash);
void rgw_create_s3_v4_string_to_sign(CephContext *cct, const string& algorithm,
                                     const string& request_date, const string& credential_scope,
                                     const string& hashed_qr, string& string_to_sign);
int rgw_calculate_s3_v4_aws_signature(struct req_state *s, const string& access_key_id,
                                      const string &date, const string& region,
                                      const string& service, const string& string_to_sign,
                                      string& signature);

#endif
