#ifndef CEPH_CRYPTO_CMS_H
#define CEPH_CRYPTO_CMS_H

#include "include/buffer_fwd.h"

class CephContext;

int ceph_decode_cms(CephContext *cct, bufferlist& cms_bl, bufferlist& decoded_bl);

#endif
