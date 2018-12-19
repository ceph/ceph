// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_xml.h"
#include "rgw_multi.h"
#include "rgw_op.h"

#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw



bool RGWMultiPart::xml_end(const char *el)
{
  RGWMultiPartNumber *num_obj = static_cast<RGWMultiPartNumber *>(find_first("PartNumber"));
  RGWMultiETag *etag_obj = static_cast<RGWMultiETag *>(find_first("ETag"));

  if (!num_obj || !etag_obj)
    return false;

  string s = num_obj->get_data();
  if (s.empty())
    return false;

  num = atoi(s.c_str());

  s = etag_obj->get_data();
  etag = s;

  return true;
}

bool RGWMultiCompleteUpload::xml_end(const char *el) {
  XMLObjIter iter = find("Part");
  RGWMultiPart *part = static_cast<RGWMultiPart *>(iter.get_next());
  while (part) {
    int num = part->get_num();
    string etag = part->get_etag();
    parts[num] = etag;
    part = static_cast<RGWMultiPart *>(iter.get_next());
  }
  return true;
}


XMLObj *RGWMultiXMLParser::alloc_obj(const char *el) {
  XMLObj *obj = NULL;
  if (strcmp(el, "CompleteMultipartUpload") == 0 ||
      strcmp(el, "MultipartUpload") == 0) {
    obj = new RGWMultiCompleteUpload();
  } else if (strcmp(el, "Part") == 0) {
    obj = new RGWMultiPart();
  } else if (strcmp(el, "PartNumber") == 0) {
    obj = new RGWMultiPartNumber();
  } else if (strcmp(el, "ETag") == 0) {
    obj = new RGWMultiETag();
  }

  return obj;
}

bool is_v2_upload_id(const string& upload_id)
{
  const char *uid = upload_id.c_str();

  return (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX, sizeof(MULTIPART_UPLOAD_ID_PREFIX) - 1) == 0) ||
         (strncmp(uid, MULTIPART_UPLOAD_ID_PREFIX_LEGACY, sizeof(MULTIPART_UPLOAD_ID_PREFIX_LEGACY) - 1) == 0);
}

int list_multipart_parts(RGWRados *store, RGWBucketInfo& bucket_info, CephContext *cct,
                                const string& upload_id,
                                string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted)
{
  map<string, bufferlist> parts_map;
  map<string, bufferlist>::iterator iter;

  rgw_obj obj;
  obj.init_ns(bucket_info.bucket, meta_oid, RGW_OBJ_NS_MULTIPART);
  obj.set_in_extra_data(true);

  rgw_raw_obj raw_obj;
  store->obj_to_raw(bucket_info.placement_rule, obj, &raw_obj);

  bool sorted_omap = is_v2_upload_id(upload_id) && !assume_unsorted;

  int ret;

  parts.clear();


  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(raw_obj);

  if (sorted_omap) {
    string p;
    p = "part.";
    char buf[32];

    snprintf(buf, sizeof(buf), "%08d", marker);
    p.append(buf);

    ret = sysobj.omap().get_vals(p, num_parts + 1, &parts_map, nullptr);
  } else {
    ret = sysobj.omap().get_all(&parts_map);
  }
  if (ret < 0)
    return ret;

  int i;
  int last_num = 0;

  uint32_t expected_next = marker + 1;

  for (i = 0, iter = parts_map.begin(); (i < num_parts || !sorted_omap) && iter != parts_map.end(); ++iter, ++i) {
    bufferlist& bl = iter->second;
    auto bli = bl.cbegin();
    RGWUploadPartInfo info;
    try {
      decode(info, bli);
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: could not part info, caught buffer::error" << dendl;
      return -EIO;
    }
    if (sorted_omap) {
      if (info.num != expected_next) {
        /* ouch, we expected a specific part num here, but we got a different one. Either
         * a part is missing, or it could be a case of mixed rgw versions working on the same
         * upload, where one gateway doesn't support correctly sorted omap keys for multipart
         * upload just assume data is unsorted.
         */
        return list_multipart_parts(store, bucket_info, cct, upload_id, meta_oid, num_parts, marker, parts, next_marker, truncated, true);
      }
      expected_next++;
    }
    if (sorted_omap ||
      (int)info.num > marker) {
      parts[info.num] = info;
      last_num = info.num;
    }
  }

  if (sorted_omap) {
    if (truncated)
      *truncated = (iter != parts_map.end());
  } else {
    /* rebuild a map with only num_parts entries */

    map<uint32_t, RGWUploadPartInfo> new_parts;
    map<uint32_t, RGWUploadPartInfo>::iterator piter;

    for (i = 0, piter = parts.begin(); i < num_parts && piter != parts.end(); ++i, ++piter) {
      new_parts[piter->first] = piter->second;
      last_num = piter->first;
    }

    if (truncated)
      *truncated = (piter != parts.end());

    parts.swap(new_parts);
  }

  if (next_marker) {
    *next_marker = last_num;
  }

  return 0;
}

int list_multipart_parts(RGWRados *store, struct req_state *s,
                                const string& upload_id,
                                string& meta_oid, int num_parts,
                                int marker, map<uint32_t, RGWUploadPartInfo>& parts,
                                int *next_marker, bool *truncated,
                                bool assume_unsorted)
{
  return list_multipart_parts(store, s->bucket_info, s->cct, upload_id, meta_oid, num_parts, marker, parts, next_marker, truncated, assume_unsorted);
}

int abort_multipart_upload(RGWRados *store, CephContext *cct, RGWObjectCtx *obj_ctx, RGWBucketInfo& bucket_info, RGWMPObj& mp_obj)
{
  rgw_obj meta_obj;
  meta_obj.init_ns(bucket_info.bucket, mp_obj.get_meta(), RGW_OBJ_NS_MULTIPART);
  meta_obj.set_in_extra_data(true);
  meta_obj.index_hash_source = mp_obj.get_key();
  cls_rgw_obj_chain chain;
  list<rgw_obj_index_key> remove_objs;
  map<uint32_t, RGWUploadPartInfo> obj_parts;
  bool truncated;
  int marker = 0;
  int ret;

  do {
    ret = list_multipart_parts(store, bucket_info, cct, mp_obj.get_upload_id(), mp_obj.get_meta(), 1000,
      marker, obj_parts, &marker, &truncated);
    if (ret < 0)
      return (ret == -ENOENT) ? -ERR_NO_SUCH_UPLOAD : ret;
    for (auto obj_iter = obj_parts.begin(); obj_iter != obj_parts.end(); ++obj_iter) {
      RGWUploadPartInfo& obj_part = obj_iter->second;
      rgw_obj obj;
      if (obj_part.manifest.empty()) {
        string oid = mp_obj.get_part(obj_iter->second.num);
        obj.init_ns(bucket_info.bucket, oid, RGW_OBJ_NS_MULTIPART);
        obj.index_hash_source = mp_obj.get_key();
        ret = store->delete_obj(*obj_ctx, bucket_info, obj, 0);
        if (ret < 0 && ret != -ENOENT)
          return ret;
      } else {
        store->update_gc_chain(meta_obj, obj_part.manifest, &chain);
        RGWObjManifest::obj_iterator oiter = obj_part.manifest.obj_begin();
        if (oiter != obj_part.manifest.obj_end()) {
          rgw_obj head;
          rgw_raw_obj raw_head = oiter.get_location().get_raw_obj(store);
          rgw_raw_obj_to_obj(bucket_info.bucket, raw_head, &head);

          rgw_obj_index_key key;
          head.key.get_index_key(&key);
          remove_objs.push_back(key);
        }
      }
    }
  } while (truncated);
  /* use upload id as tag */
  ret = store->send_chain_to_gc(chain, mp_obj.get_upload_id() , false);  // do it async
  if (ret < 0) {
    ldout(cct, 5) << "gc->send_chain() returned " << ret << dendl;
    return (ret == -ENOENT) ? -ERR_NO_SUCH_UPLOAD : ret;
  }
  RGWRados::Object del_target(store, bucket_info, *obj_ctx, meta_obj);
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket_info.owner;
  del_op.params.versioning_status = 0;
  if (!remove_objs.empty()) {
    del_op.params.remove_objs = &remove_objs;
  }

  // and also remove the metadata obj
  ret = del_op.delete_obj();
  return (ret == -ENOENT) ? -ERR_NO_SUCH_UPLOAD : ret;
}

int list_bucket_multiparts(RGWRados *store, RGWBucketInfo& bucket_info,
                                string& prefix, string& marker, string& delim,
                                int& max_uploads, vector<rgw_bucket_dir_entry> *objs,
                                map<string, bool> *common_prefixes, bool *is_truncated)
{
  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);
  MultipartMetaFilter mp_filter;

  list_op.params.prefix = prefix;
  list_op.params.delim = delim;
  list_op.params.marker = marker;
  list_op.params.ns = RGW_OBJ_NS_MULTIPART;
  list_op.params.filter = &mp_filter;

  return(list_op.list_objects(max_uploads, objs, common_prefixes,is_truncated));
}

int abort_bucket_multiparts(RGWRados *store, CephContext *cct, RGWBucketInfo& bucket_info,
				string& prefix, string& delim)
{
  int ret, max = 1000, num_deleted = 0;
  vector<rgw_bucket_dir_entry> objs;
  RGWObjectCtx obj_ctx(store);
  string marker;
  bool is_truncated;

  do {
    ret = list_bucket_multiparts(store, bucket_info, prefix, marker, delim,
				max, &objs, nullptr, &is_truncated);
    if (ret < 0) {
      return ret;
    }
    if (!objs.empty()) {
      RGWMPObj mp;
      for (const auto& obj : objs) {
        rgw_obj_key key(obj.key);
        if (!mp.from_meta(key.name))
          continue;
        ret = abort_multipart_upload(store, cct, &obj_ctx, bucket_info, mp);
        if (ret < 0 && ret != -ENOENT && ret != -ERR_NO_SUCH_UPLOAD) {
          return ret;
        }
        num_deleted++;
      }
      if (num_deleted) {
        ldout(store->ctx(),0) << "WARNING : aborted " << num_deleted << " incomplete multipart uploads" << dendl;
      }
    }
  } while (is_truncated);

  return ret;
}
