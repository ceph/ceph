// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <vector>
#include "rgw_xml.h"
#include "rgw_common.h"

class RGWMultiDelDelete : public XMLObj
{
public:
  RGWMultiDelDelete() :quiet(false) {}
  ~RGWMultiDelDelete() override {}
  bool xml_end(const char *el) override;

  std::vector<rgw_obj_key> objects;
  bool quiet;
  bool is_quiet() { return quiet; }
};

class RGWMultiDelQuiet : public XMLObj
{
public:
  RGWMultiDelQuiet() {}
  ~RGWMultiDelQuiet() override {}
};

class RGWMultiDelObject : public XMLObj
{
  std::string key;
  std::string version_id;
public:
  RGWMultiDelObject() {}
  ~RGWMultiDelObject() override {}
  bool xml_end(const char *el) override;

  const std::string& get_key() { return key; }
  const std::string& get_version_id() { return version_id; }
};

class RGWMultiDelKey : public XMLObj
{
public:
  RGWMultiDelKey() {}
  ~RGWMultiDelKey() override {}
};

class RGWMultiDelVersionId : public XMLObj
{
public:
  RGWMultiDelVersionId() {}
  ~RGWMultiDelVersionId() override {}
};

class RGWMultiDelXMLParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override;
public:
  RGWMultiDelXMLParser() {}
  ~RGWMultiDelXMLParser() override {}
};
