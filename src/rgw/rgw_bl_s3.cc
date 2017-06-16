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

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_user.h"
#include "rgw_bl_s3.h"


#define dout_subsys ceph_subsys_rgw

using namespace std;

bool BLTargetGrants_S3::xml_end(const char *el) {
  BLGrant *bl_grant;
  int bl_grant_nums = 0;
  grants.clear();

  XMLObjIter grant_iter = find("Grant");
  bl_grant = static_cast<BLGrant*>(static_cast<BLGrant_S3 *>(grant_iter.get_next()));
  while (bl_grant && bl_grant_nums < 100) {
    grants.push_back(*bl_grant);
    bl_grant_nums ++;
    bl_grant = static_cast<BLGrant*>(static_cast<BLGrant_S3 *>(grant_iter.get_next()));;
  }

  return true;
}

bool BLGrant_S3::xml_end(const char *el) {
  BLID_S3 *bl_id;
  BLDisplayName_S3 *bl_display_name;
  BLEmailAddress_S3 *bl_email_address;
  BLURI_S3 *bl_uri;
  BLPermission_S3 *bl_permission;
  BLGrantee_S3 *bl_grantee;

  bl_grantee = static_cast<BLGrantee_S3 *>(find_first("Grantee"));

  if (bl_grantee) {
    grantee_specified = true;
    bl_grantee->get_attr("xsi:type", type);
    ldout(cct, 15) << __func__ << " grantee type = " << type << dendl;

    bl_id = static_cast<BLID_S3 *>(bl_grantee->find_first("ID"));
    if (bl_id) {
      id = bl_id->get_data();
      id_specified = true;

      bl_display_name = static_cast<BLDisplayName_S3 *>(bl_grantee->find_first("DisplayName"));
      if (bl_display_name) {
        display_name = bl_display_name->get_data();
      }
    }

    bl_email_address = static_cast<BLEmailAddress_S3 *>(bl_grantee->find_first("EmailAddress"));
    if (bl_email_address) {
      email_address = bl_email_address->get_data();
      email_address_specified = true;
    }

    bl_uri = static_cast<BLURI_S3 *>(bl_grantee->find_first("URI"));
    if (bl_uri) {
      uri = bl_uri->get_data();
      uri_specified = true;
    }
    
    bl_permission = static_cast<BLPermission_S3 *>(find_first("Permission"));
    if (bl_permission) {
      permission = bl_permission->get_data();
      permission_specified = true;
    }
  }
  return true;
}

bool BLLoggingEnabled_S3::xml_end(const char *el) {
  BLTargetBucket_S3 *bl_target_bucket;
  BLTargetPrefix_S3 *bl_target_prefix;
  BLTargetGrants_S3 *bl_target_grants;

  target_bucket.clear();
  target_prefix.clear();
  target_grants.clear();
  
  bl_target_bucket = static_cast<BLTargetBucket_S3 *>(find_first("TargetBucket"));
  if (bl_target_bucket) {
    target_bucket = bl_target_bucket->get_data();
    target_bucket_specified = true;
  }

  bl_target_prefix = static_cast<BLTargetPrefix_S3 *>(find_first("TargetPrefix"));
  if (bl_target_prefix) {
    target_prefix = bl_target_prefix->get_data();
    target_prefix_specified = true;
  }

  bl_target_grants = static_cast<BLTargetGrants_S3 *>(find_first("TargetGrants"));
  if (bl_target_grants) {
    target_grants = bl_target_grants->get_grants();
    target_grants_specified = true;
  }

  return true;
}

bool RGWBucketLoggingStatus_S3::xml_end(const char *el) {
  BLLoggingEnabled_S3 *bl_enabled = static_cast<BLLoggingEnabled_S3 *>(find_first("LoggingEnabled"));

  if (bl_enabled) {
    enabled.set_true();
    if (bl_enabled->target_bucket_specified) {
      enabled.target_bucket_specified = true;
      enabled.set_target_bucket(bl_enabled->get_target_bucket());
    }
    if (bl_enabled->target_prefix_specified) {
      enabled.target_prefix_specified = true;
      enabled.set_target_prefix(bl_enabled->get_target_prefix());
    }
    if (bl_enabled->target_grants_specified) {
      enabled.target_grants_specified = true;
      enabled.set_target_grants(bl_enabled->get_target_grants());
    }
  } else {
    enabled.set_false();
  }

  return true;
}

int RGWBucketLoggingStatus_S3::rebuild(RGWRados *store, RGWBucketLoggingStatus& dest)
{
  int ret = 0;
  // FIXME(jiaying) checkout ACL rule count limits here.
  if (this->is_enabled()) {
    dest.enabled.set_true();
    dest.enabled.set_target_bucket(this->get_target_bucket());
    dest.enabled.set_target_prefix(this->get_target_prefix());
    dest.enabled.set_target_grants(this->get_target_grants());
    ldout(cct, 20) << "bl is_enabled, create new bl config"  << dendl;
  } else {
    dest.enabled.set_false();
  }

  return ret;
}

XMLObj *RGWBLXMLParser_S3::alloc_obj(const char *el)
{
  XMLObj *obj = nullptr;
  if (strcmp(el, "BucketLoggingStatus") == 0) {
    obj = new RGWBucketLoggingStatus_S3(cct);
  } else if (strcmp(el, "LoggingEnabled") == 0) {
    obj = new BLLoggingEnabled_S3(cct);
  } else if (strcmp(el, "TargetBucket") == 0) {
    obj = new BLTargetBucket_S3();
  } else if (strcmp(el, "TargetPrefix") == 0) {
    obj = new BLTargetPrefix_S3();
  } else if (strcmp(el, "TargetGrants") == 0) {
    obj = new BLTargetGrants_S3(cct);
  } else if (strcmp(el, "Grant") == 0) {
    obj = new BLGrant_S3(cct);
  } else if (strcmp(el, "Grantee") == 0) {
    obj = new BLGrantee_S3();
  } else if (strcmp(el, "ID") == 0) {
    obj = new BLID_S3();
  } else if (strcmp(el, "DisplayName") == 0) {
    obj = new BLDisplayName_S3();
  } else if (strcmp(el, "EmailAddress") == 0) {
    obj = new BLEmailAddress_S3();
  } else if (strcmp(el, "URI") == 0) {
    obj = new BLURI_S3();
  } else if (strcmp(el, "Permission") == 0) {
    obj = new BLPermission_S3();
  }
  return obj;
}
