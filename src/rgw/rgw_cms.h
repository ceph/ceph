#ifndef CEPH_RGW_CMS_H
#define CEPH_RGW_CMS_H

#include "common/ceph_json.h"
#include "rgw_common.h"
#include "common/ceph_crypto_cms.h"
#include "common/armor.h"

bool is_pki_token(const string& token);
int open_cms_envelope(CephContext *cct, string& src, string& dst);
int decode_b64_cms(CephContext *cct, const string& signed_b64, bufferlist& bl);

#endif
