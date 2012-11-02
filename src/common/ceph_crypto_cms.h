#ifndef CEPH_CRYPTO_CMS_H
#define CEPH_CRYPTO_CMS_H

#include "include/buffer.h"

int decode_cms(bufferlist& cms_bl, bufferlist& decoded_bl);

#endif
