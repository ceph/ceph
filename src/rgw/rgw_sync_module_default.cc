#include "common/errno.h"
#include "rgw_coroutine.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_default.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include <boost/asio/yield.hpp>
#define dout_subsys ceph_subsys_rgw

template <class T>
static int decode_attr(map<string, bufferlist>& attrs, const string& attr_name, T *val)
{
  map<string, bufferlist>::iterator iter = attrs.find(attr_name);
  if (iter == attrs.end()) {
    *val = T();
    return 0;
  }

  bufferlist::iterator biter = iter->second.begin();
  try {
    decode(*val, biter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

class RGWFetchRemoteObjMultipartCR : public RGWCoroutine {
  using properties = rgw_sync_default_src_obj_properties;
  RGWDataSyncEnv *sync_env;
  const RGWBucketInfo bucket_info;
  const rgw_obj_key key;
  const properties src_properties;
  map<string, bufferlist> attrs; //attrs read from remote zone
  rgw_sync_default_multipart_upload_info status;
  rgw_raw_obj status_obj;
  rgw_sync_default_multipart_part_info *pcur_part_info{nullptr};

  uint64_t size;
  uint64_t epoch;
  real_time mtime;
  rgw_obj obj;
  map<string, bufferlist> local_attrs;
  uint32_t zone_short_id{0};
  uint64_t pg_ver{0};
  const uint64_t sync_thread_num;

  int ret;

  bool not_older_than(const real_time& _mtime,
                     const uint32_t _zone_short_id,
                     const uint64_t _pg_ver) {
    if (mtime > _mtime) {
      return true;
    }
    if (mtime < _mtime) {
      return false;
    }
    if (!zone_short_id || !_zone_short_id) {
      /* don't compare zone ids, if one wasn't provided */
      return true;
    }
    if (zone_short_id != _zone_short_id) {
      return (zone_short_id >= _zone_short_id);
    }
    return (pg_ver >= _pg_ver);
  }

public:
  RGWFetchRemoteObjMultipartCR(RGWDataSyncEnv *_sync_env,
                               const RGWBucketInfo& _bucket_info,
                               rgw_obj_key& _key,
                               const properties& _properties,
                               const map<string, bufferlist>& _attrs) :
                                           RGWCoroutine(_sync_env->cct),
                                           sync_env(_sync_env),
                                           bucket_info(_bucket_info),
                                           key(_key),
                                           src_properties(_properties),
                                           attrs(_attrs),
                                           status_obj(_sync_env->store->get_zone_params().log_pool,
                                               RGWBucketSyncStatusManager::obj_status_oid(_sync_env->source_zone,
                                               rgw_obj(bucket_info.bucket, key))),
                                           sync_thread_num(std::max(uint64_t(1), _sync_env->cct->_conf->get_val<uint64_t>(
                                               "rgw_sync_multipart_concurrent_requests"))) {
    obj = rgw_obj(bucket_info.bucket, key);
  }
  int operate() override {
    /* 1. compare mtime
     * 2. init
     * 3. upload
     * 4. complete
     * 5. remove status obj
     */
    bool again;
    reenter(this) {
      /* get local mtime */
      yield call(new RGWStatObjCR(sync_env->async_rados, sync_env->store,
                                  bucket_info, obj, &size, &mtime, &epoch, &local_attrs));
      if (retcode >=0) {
        ret = decode_attr(attrs, string(RGW_ATTR_PG_VER), &pg_ver);
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
        } else {
          ret = decode_attr(attrs, string(RGW_ATTR_SOURCE_ZONE), &zone_short_id);
          if (ret < 0) {
            ldout(sync_env->cct, 0) << "ERROR: failed to decode source zone short_id attr, ignoring" << dendl;
            pg_ver = 0; /* all or nothing */
          }
        }
        if (not_older_than(src_properties.mtime, src_properties.zone_short_id, src_properties.pg_ver)) {
          ldout(sync_env->cct,10)<<"obj:" << obj << " not modified." << dendl;
          return set_cr_done();
        }
      }
      yield call(new RGWSimpleRadosReadCR<rgw_sync_default_multipart_upload_info>(sync_env->async_rados,
                                                                                    sync_env->store,
                                                                                    status_obj, &status, false));
      if (retcode < 0 && retcode != -ENOENT) {
        ldout(sync_env->cct, 0) << "ERROR: failed to read sync status of object " << status_obj << " retcode=" << retcode << dendl;
        return retcode;
      }

      if (retcode >= 0) {
        /* Fix me: only compare mtime and size, because etag for multipart is not the same as md5 */
        if (status.src_properties.mtime != src_properties.mtime
            || status.src_properties.size != src_properties.size) {
          /* clear all parts */
          yield call(new RGWAbortMultipartCR(sync_env->async_rados, sync_env->store,
                                             bucket_info, key, status.upload_id));
          retcode = -ENOENT;
          ldout(sync_env->cct, 20) << "WARNING: get unfinished multipart sync, "
               << "but object properties mismatched, clear all parts" << dendl;
        }
        ldout(sync_env->cct, 20) << "get unfinished multipart sync, restart from part #"
             << status.cur_part + 1 << dendl;
      }

      if (retcode == -ENOENT) {
        attrs.erase(string(RGW_ATTR_PG_VER));
        attrs.erase(string(RGW_ATTR_SOURCE_ZONE));
        yield call(new RGWInitMultipartCR(sync_env->async_rados, sync_env->store, bucket_info, key, &status.upload_id, attrs));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: init multipart failed for obj=" << obj << dendl;
          return set_cr_error(retcode);
        }

        status.src_properties = src_properties;
        status.part_size = sync_env->cct->_conf->rgw_multipart_min_part_size;
        status.num_parts = (status.src_properties.size + status.part_size - 1) / status.part_size;
        status.cur_part = 1;
        status.finished_parts.assign(status.num_parts, -1);
      } else {
        status.cur_part++;
      }

      while ((uint32_t)status.cur_part <= status.num_parts) {
        ldout(sync_env->cct,20) << "sync obj=" << obj << "cur_part=" << status.cur_part
            << ", already spawned " << num_spawned() << "stacks." <<dendl;
        if (status.finished_parts[status.cur_part] == 0) {
          // this part has been saved successfully, try next
          ldout(sync_env->cct,20) << "sync obj=" << obj << " got already finished part="
              << status.cur_part << dendl;
          status.cur_part++;
          continue;
        }
//        yield {
        pcur_part_info = &status.parts[status.cur_part];
        pcur_part_info->part_num = status.cur_part;
        pcur_part_info->ofs = status.cur_ofs;
        pcur_part_info->size = std::min((uint64_t)status.part_size, status.src_properties.size - status.cur_ofs);

        status.cur_ofs += pcur_part_info->size;

        spawn(new RGWFetchRemoteObjMultipartPartCR(sync_env->async_rados,
                                                   sync_env->store,
                                                   sync_env->source_zone,
                                                   bucket_info, key,
                                                   status.src_properties,
                                                   status.upload_id,
                                                   *pcur_part_info,
                                                   &status.finished_parts[status.cur_part-1]), false);
        status.cur_part++;
//        }
        if (status.cur_part > status.num_parts) {
          break;
        }

        // wait if the num of concurrent put reaches limit
        while (num_spawned() >= sync_thread_num) {
          yield wait_for_child(); //wait for at least on part finish
          again = true;
          while (again) {
            again = collect(&ret, nullptr);
            if (ret < 0) {
              //get an error, stop this sync round
              ldout(sync_env->cct, 0) << "ERROR: failed to sync obj=" << obj
                  << ", via multipart upload, upload_id=" << status.upload_id
                  << ", when writing parts (error: " << cpp_strerror(-ret) << ")" << dendl;
              /* obj at remote zone has changed, clear all parts we have uploaded.
               * otherwise, just return and expecting a retry for the failed or unsaved parts.
               */
              if (ret == -ENOENT) {
                yield call(new RGWAbortMultipartCR(sync_env->async_rados, sync_env->store,
                                               bucket_info, key, status.upload_id));
                yield call(new RGWSimpleRadosRemoveCR(sync_env->async_rados, sync_env->store, status_obj));
              }
              drain_all();
              return set_cr_error(ret);
            }
          }

          /* write status info to disk */
          yield call(new RGWSimpleRadosWriteCR<rgw_sync_default_multipart_upload_info>(sync_env->async_rados, sync_env->store, status_obj, status));

          if (retcode < 0) {
            ldout(sync_env->cct, 0) << "ERROR: failed to store multipart upload state, retcode="
                << retcode << " (error: "<<cpp_strerror(-retcode)<<dendl;
            /* continue with upload anyway */
          }
        }
      }

      /* wait for all pending parts finish */
      while (num_spawned()) {
        yield wait_for_child();
        again = true;
        while (again) {
          again = collect(&ret, nullptr);
          if (ret < 0) {
            ldout(sync_env->cct, 0) << "ERROR: failed to sync obj=" << obj
                << ", via multipart upload, upload_id=" << status.upload_id
                << ", when writing parts (error: " << cpp_strerror(-ret) << ")" << dendl;
            if (retcode == -ENOENT) {
              yield call(new RGWAbortMultipartCR(sync_env->async_rados, sync_env->store,
                                             bucket_info, key, status.upload_id));
              yield call(new RGWSimpleRadosRemoveCR(sync_env->async_rados, sync_env->store, status_obj));
            }
            drain_all();
            return set_cr_error(ret);
          }
        }
      }

      yield call(new RGWSimpleRadosWriteCR<rgw_sync_default_multipart_upload_info>(sync_env->async_rados, sync_env->store, status_obj, status));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to store multipart upload state, retcode="
            << retcode << " (error: "<<cpp_strerror(-retcode)<<dendl;
        /* continue with upload anyway */
      }

      // now do complete
      ldout(sync_env->cct,0)<<"SESETIME: do complete"<<dendl;
      yield call(new RGWCompleteMultipartCR(sync_env->async_rados, sync_env->store,
                                            bucket_info, key, status.upload_id,
                                            status.parts, src_properties.mtime));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to complete multipart upload of obj="
            << obj << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
        // expect it will complete next sync round, don't clear existing parts
        //yield call(new RGWAbortMultipartCR(sync_env->async_rados, sync_env->store, bucket_info, key, status.upload_id));
        return set_cr_error(retcode);
      }

      /* remove status obj */
      yield call(new RGWSimpleRadosRemoveCR(sync_env->async_rados, sync_env->store, status_obj));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to remove status obj="
            << status_obj << " upload_id=" << status.upload_id << " part number "
            << status.cur_part << " (" << cpp_strerror(-retcode) << ")" << dendl;
        /* ignore error, best effort */
      }
      return set_cr_done();
    }
    return 0;
  }
};
class RGWDefaultHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  uint64_t versioned_epoch{0};
  bool copy_if_newer;
  rgw_zone_set *zones_trace;
  const uint64_t multipart_threshold;
  uint32_t src_zone_short_id{0};
  uint64_t src_pg_ver{0};
  int ret{0};

public:
  RGWDefaultHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                                RGWBucketInfo _bucket_info, rgw_obj_key _key,
                                uint64_t _versioned_epoch, bool _if_newer,
                                rgw_zone_set *_zones_trace) :
                                        RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                        versioned_epoch(_versioned_epoch), copy_if_newer(_if_newer),
                                        zones_trace(_zones_trace),
                                        multipart_threshold(_sync_env->cct->_conf->get_val<uint64_t>(
                                                "rgw_sync_multipart_threshold")) {
  }

  int operate() override {
    reenter(this) {
      yield {
        if (multipart_threshold == 0 || size < multipart_threshold) {
          call(new RGWFetchRemoteObjCR(sync_env->async_rados, sync_env->store, sync_env->source_zone,
                                       bucket_info, key, versioned_epoch, true, zones_trace));
        } else {
          ret = decode_attr(attrs, string(RGW_ATTR_PG_VER), &src_pg_ver);
          if (ret < 0) {
            ldout(sync_env->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
          } else {
            ret = decode_attr(attrs, string(RGW_ATTR_SOURCE_ZONE), &src_zone_short_id);
            if (ret < 0) {
              ldout(sync_env->cct, 0) << "ERROR: failed to decode source zone short_id attr, ignoring" << dendl;
              src_pg_ver = 0; /* all or nothing */
            }
          }
          rgw_sync_default_src_obj_properties src_properties;
          src_properties.mtime = mtime;
          src_properties.etag = etag;
          src_properties.zone_short_id = src_zone_short_id;
          src_properties.pg_ver = src_pg_ver;
          src_properties.versioned_epoch = versioned_epoch;
          src_properties.size = size;

          call(new RGWFetchRemoteObjMultipartCR(sync_env, bucket_info, key, src_properties, attrs));
        }
      }
      if (retcode < 0)
        return set_cr_error(retcode);
      return set_cr_done();
    }
    return 0;
  }
};
RGWStatRemoteObjCBCR* RGWDefaultHandleRemoteObjCR::allocate_callback() {
  return new RGWDefaultHandleRemoteObjCBCR(sync_env, bucket_info, key, versioned_epoch, copy_if_newer, zones_trace);
}
