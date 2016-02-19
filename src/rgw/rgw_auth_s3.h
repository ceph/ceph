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

#endif
