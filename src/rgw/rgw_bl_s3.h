// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_BL_S3_H_
#define RGW_BL_S3_H_


#include <map>
#include <string>
#include <iostream>
#include <include/types.h>

#include <expat.h>

#include "include/str_list.h"
#include "rgw_bl.h"
#include "rgw_xml.h"

using namespace std;

class BLTargetBucket_S3 : public XMLObj
{
public:
  BLTargetBucket_S3() {}
  ~BLTargetBucket_S3() override {}
  string& to_str() { return data; }
};

class BLTargetPrefix_S3 : public XMLObj
{
public:
  BLTargetPrefix_S3() {}
  ~BLTargetPrefix_S3() override {}
  string& to_str() { return data; }
};

class BLLoggingEnabled_S3 : public BLLoggingEnabled, public XMLObj
{
public:
  BLLoggingEnabled_S3(CephContext *_cct) : BLLoggingEnabled(_cct) {}
  BLLoggingEnabled_S3() {}
  ~BLLoggingEnabled_S3() override {}
  string& to_str() { return data; }

  bool xml_end(const char *el) override;
};

class RGWBLXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el) override;
public:
  RGWBLXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};

class RGWBucketLoggingStatus_S3 : public RGWBucketLoggingStatus, public XMLObj
{
public:
  RGWBucketLoggingStatus_S3(CephContext *_cct) : RGWBucketLoggingStatus(_cct) {}
  RGWBucketLoggingStatus_S3() : RGWBucketLoggingStatus(nullptr) {}
  ~RGWBucketLoggingStatus_S3() override {}

  bool xml_end(const char *el) override;
  void to_xml(ostream& out)
  {
    out << "<BucketLoggingStatus xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
    if (is_enabled()) {
      out << "<LoggingEnabled>";

      string _bucket = this->get_target_bucket();
      if (!_bucket.empty()) {
        out << "<TargetBucket>" << _bucket << "</TargetBucket>";
      }

      string _prefix = this->get_target_prefix();
      if (!_prefix.empty()) {
        out << "<TargetPrefix>" << _prefix << "</TargetPrefix>";
      }

      out << "</LoggingEnabled>";
    }
    out << "</BucketLoggingStatus>";
  }

  int rebuild(RGWRados *store, RGWBucketLoggingStatus& dest);
  void dump_xml(Formatter *f) const
  {
    f->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);

    if (is_enabled()) {
      f->open_object_section("LoggingEnabled");

      string _bucket = this->get_target_bucket();
      if (!_bucket.empty()) {
        encode_xml("TargetBucket", _bucket, f);
      }
      string _prefix = this->get_target_prefix();
      if (!_prefix.empty()) {
        encode_xml("TargetPrefix", _prefix, f);
      }

      f->close_section(); // LoggingEnabled
    }

    f->close_section(); // BucketLoggingStatus
  }

};

#endif
