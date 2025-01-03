// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_tier_rados.h"

using namespace std;

const std::string MP_META_SUFFIX = ".meta";

bool MultipartMetaFilter(const string& name, string& key) {
  // the length of the suffix so we can skip past it
  static const size_t MP_META_SUFFIX_LEN = MP_META_SUFFIX.length();

  size_t len = name.size();

  // make sure there's room for suffix plus at least one more
  // character
  if (len <= MP_META_SUFFIX_LEN)
    return false;

  size_t pos = name.find(MP_META_SUFFIX, len - MP_META_SUFFIX_LEN);
  if (pos == string::npos)
    return false;

  pos = name.rfind('.', pos - 1);
  if (pos == string::npos)
    return false;

  key = name.substr(0, pos);

  return true;
}
