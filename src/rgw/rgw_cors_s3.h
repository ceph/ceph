// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <map>
#include <string>
#include <iosfwd>

#include <include/types.h>
#include <common/Formatter.h>
#include <common/dout.h>
#include "rgw_xml.h"
#include "rgw_cors.h"

class RGWCORSRule_S3 : public RGWCORSRule, public XMLObj
{
  const DoutPrefixProvider *dpp;
  public:
    RGWCORSRule_S3(const DoutPrefixProvider *dpp) : dpp(dpp) {}
    ~RGWCORSRule_S3() override {}
    
    bool xml_end(const char *el) override;
    void to_xml(XMLFormatter& f);
};

class RGWCORSConfiguration_S3 : public RGWCORSConfiguration, public XMLObj
{
  const DoutPrefixProvider *dpp;
  public:
    RGWCORSConfiguration_S3(const DoutPrefixProvider *dpp) : dpp(dpp) {}
    ~RGWCORSConfiguration_S3() override {}

    bool xml_end(const char *el) override;
    void to_xml(std::ostream& out);
};

class RGWCORSXMLParser_S3 : public RGWXMLParser
{
  const DoutPrefixProvider *dpp;
  CephContext *cct;

  XMLObj *alloc_obj(const char *el) override;
public:
  explicit RGWCORSXMLParser_S3(const DoutPrefixProvider *_dpp, CephContext *_cct) : dpp(_dpp), cct(_cct) {}
};
