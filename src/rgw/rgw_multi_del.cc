// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <iostream>

#include "include/types.h"

#include "rgw_xml.h"
#include "rgw_multi_del.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


bool RGWMultiDelObject::xml_end(const char *el)
{
  RGWMultiDelKey *key_obj = static_cast<RGWMultiDelKey *>(find_first("Key"));
  RGWMultiDelVersionId *vid = static_cast<RGWMultiDelVersionId *>(find_first("VersionId"));

  if (!key_obj)
    return false;

  string s = key_obj->get_data();
  if (s.empty())
    return false;

  key = s;

  if (vid) {
    version_id = vid->get_data();
  }

  return true;
}

bool RGWMultiDelDelete::xml_end(const char *el) {
  RGWMultiDelQuiet *quiet_set = static_cast<RGWMultiDelQuiet *>(find_first("Quiet"));
  if (quiet_set) {
    string quiet_val = quiet_set->get_data();
    quiet = (strcasecmp(quiet_val.c_str(), "true") == 0);
  }

  XMLObjIter iter = find("Object");
  RGWMultiDelObject *object = static_cast<RGWMultiDelObject *>(iter.get_next());
  while (object) {
    const string& key = object->get_key();
    const string& instance = object->get_version_id();
    rgw_obj_key k(key, instance);
    objects.push_back(k);
    object = static_cast<RGWMultiDelObject *>(iter.get_next());
  }
  return true;
}

XMLObj *RGWMultiDelXMLParser::alloc_obj(const char *el) {
  XMLObj *obj = NULL;
  if (strcmp(el, "Delete") == 0) {
    obj = new RGWMultiDelDelete();
  } else if (strcmp(el, "Quiet") == 0) {
    obj = new RGWMultiDelQuiet();
  } else if (strcmp(el, "Object") == 0) {
    obj = new RGWMultiDelObject ();
  } else if (strcmp(el, "Key") == 0) {
    obj = new RGWMultiDelKey();
  } else if (strcmp(el, "VersionId") == 0) {
    obj = new RGWMultiDelVersionId();
  }

  return obj;
}

