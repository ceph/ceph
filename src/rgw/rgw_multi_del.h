// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <vector>
#include "rgw_xml.h"
#include "rgw_common.h"

class RGWMultiDelObject : public XMLObj
{
  std::string key;
  std::string version_id;
  const char *if_match{nullptr};
  ceph::real_time last_mod_time;
  std::optional<uint64_t> size_match;
public:
  RGWMultiDelObject() {}
  ~RGWMultiDelObject() override {}
  bool xml_end(const char *el) override;

  const std::string& get_key() const { return key; }
  const std::string& get_version_id() const { return version_id; }
  const char* get_if_match() const { return if_match; }
  const ceph::real_time& get_last_mod_time() const { return last_mod_time; }
  const std::optional<uint64_t> get_size_match() const { return size_match; }
};

class RGWMultiDelDelete : public XMLObj
{
public:
  RGWMultiDelDelete() :quiet(false) {}
  ~RGWMultiDelDelete() override {}
  bool xml_end(const char *el) override;

  std::vector<RGWMultiDelObject> objects;
  bool quiet;
  bool is_quiet() { return quiet; }
};

class RGWMultiDelQuiet : public XMLObj
{
public:
  RGWMultiDelQuiet() {}
  ~RGWMultiDelQuiet() override {}
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
