// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_WS_H
#define CEPH_RGW_WS_H

#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

class WSIdxDoc
{
protected:
  string suffix;
public:
  WSIdxDoc() {}
  ~WSIdxDoc() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(suffix, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(suffix, bl);
    DECODE_FINISH(bl);
  }
  void set_suffix(const string& _suffix) { suffix = _suffix; }
  string& get_suffix() { return suffix; }
};
WRITE_CLASS_ENCODER(WSIdxDoc)

class RGWWebsiteConfiguration
{
  protected:
    CephContext *cct;
    WSIdxDoc idx_doc;
  public:
    RGWWebsiteConfiguration() {}
    ~RGWWebsiteConfiguration() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(idx_doc, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(idx_doc, bl);
    DECODE_FINISH(bl);
  }

  void set_idx_doc(WSIdxDoc& o) { idx_doc = o; }
  WSIdxDoc& get_idx_doc() {
    return idx_doc;
  }

};
WRITE_CLASS_ENCODER(RGWWebsiteConfiguration)



#endif /*CEPH_RGW_WS_H*/
