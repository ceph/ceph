// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <string.h>
#include <limits.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_ws_s3.h"
#include "rgw_user.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

class WSIdxDocSuffix_S3 : public XMLObj {
  public:
    WSIdxDocSuffix_S3() {}
    ~WSIdxDocSuffix_S3() {}
};

bool WSIdxDoc_S3::xml_end(const char *el) {
  WSIdxDocSuffix_S3 *ws_suffix = static_cast<WSIdxDocSuffix_S3 *>(find_first("Suffix"));

  // Suffix is mandatory
  if (!ws_suffix)
    return false;
  suffix = ws_suffix->get_data();

  return true;
}

bool RGWWebsiteConfiguration_S3::xml_end(const char *el) {
  WSIdxDoc_S3 *idx_doc_p = static_cast<WSIdxDoc_S3 *>(find_first("IndexDocument"));
  if (!idx_doc_p)
    return false;

  idx_doc = *idx_doc_p;
  return true;
}

XMLObj *RGWWSXMLParser_S3::alloc_obj(const char *el) {
  if (strcmp(el, "WebsiteConfiguration") == 0) {
    return new RGWWebsiteConfiguration_S3;
  } else if (strcmp(el, "IndexDocument") == 0) {
    return new WSIdxDoc_S3;
  } else if (strcmp(el, "Suffix") == 0) {
    return new WSIdxDocSuffix_S3;
  }
  return NULL;
}
