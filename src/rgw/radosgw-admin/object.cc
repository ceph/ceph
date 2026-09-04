// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/object.h"
#include <fcntl.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "include/buffer.h"
#include "driver/rados/rgw_bucket.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "radosgw-admin/bucket.h"
#include "radosgw-admin/admin_io.h"
#include "rgw_data_access.h"
using namespace rgw_admin;
using namespace std;

static int check_obj_locator_underscore(const DoutPrefixProvider* dpp,
                                        rgw::sal::Driver* driver,
                                        rgw::sal::Object* obj, bool fix,
                                        bool remove_bad, ceph::Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "head");
  f->dump_string("name", obj->get_name());
  f->dump_string("instance", obj->get_instance());
  f->close_section();

  string oid;
  string locator;

  get_obj_bucket_and_oid_loc(obj->get_obj(), oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj->get_read_op();

  int ret = read_op->prepare(null_yield, dpp);
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->fix_head_obj_locator(dpp, obj->get_bucket()->get_info(), needs_fixing, remove_bad, obj->get_key(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: fix_head_object_locator() returned ret=" << ret << std::endl;
      goto done;
    }
    status = "fixed";
  }

done:
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

static int check_obj_tail_locator_underscore(const DoutPrefixProvider* dpp,
                                             rgw::sal::Driver* driver,
                                             RGWBucketInfo& bucket_info,
                                             rgw_obj_key& key, bool fix,
                                             ceph::Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "tail");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  bool needs_fixing;
  string status;

  int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->fix_tail_obj_locator(dpp, bucket_info, key, fix, &needs_fixing, null_yield);
  if (ret < 0) {
    cerr << "ERROR: fix_tail_object_locator_underscore() returned ret=" << ret << std::endl;
    status = "failed";
  } else {
    status = (needs_fixing && !fix ? "needs_fixing" : "ok");
  }

  f->dump_bool("needs_fixing", needs_fixing);
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

int do_check_object_locator(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            const string& tenant_name,
                            const string& bucket_name,
                            bool fix, bool remove_bad, ceph::Formatter *f)
{
  if (remove_bad && !fix) {
    cerr << "ERROR: can't have remove_bad specified without fix" << std::endl;
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  string bucket_id;

  f->open_object_section("bucket");
  f->dump_string("bucket", bucket_name);
  int ret = rgw_admin_init_bucket(dpp, driver, tenant_name, bucket_name, bucket_id, &bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  int count = 0;

  int max_entries = 1000;

  string prefix;
  string delim;
  string marker;
  vector<rgw_bucket_dir_entry> result;
  string ns;

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = prefix;
  params.delim = delim;
  params.marker = rgw_obj_key(marker);
  params.ns = ns;
  params.enforce_ns = true;
  params.list_versions = true;

  f->open_array_section("check_objects");
  do {
    ret = bucket->list(dpp, params, max_entries - count, results, null_yield);
    if (ret < 0) {
      cerr << "ERROR: driver->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += results.objs.size();

    for (vector<rgw_bucket_dir_entry>::iterator iter = results.objs.begin(); iter != results.objs.end(); ++iter) {
      std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(iter->key);

      if (obj->get_name()[0] == '_') {
        ret = check_obj_locator_underscore(dpp, driver, obj.get(), fix, remove_bad, f);

	if (ret >= 0) {
          ret = check_obj_tail_locator_underscore(dpp, driver, bucket->get_info(), obj->get_key(), fix, f);
          if (ret < 0) {
              cerr << "ERROR: check_obj_tail_locator_underscore(): " << cpp_strerror(-ret) << std::endl;
              return -ret;
          }
	}
      }
    }
    f->flush(cout);
  } while (results.is_truncated && count < max_entries);
  f->close_section();
  f->close_section();

  f->flush(cout);

  return 0;
}

int rgw_admin_object(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     rgw::SiteConfig& site,
                     ceph::Formatter* formatter,
                     RGWStreamFlusher& stream_flusher,
                     RGWBucketAdminOpState& bucket_op,
                     std::unique_ptr<rgw::sal::Bucket>& bucket,
                     const rgw_admin_object_options& opts)
{
  auto& command = opts.command;
  auto& tenant = opts.tenant;
  auto& bucket_name = opts.bucket_name;
  auto& bucket_id = opts.bucket_id;
  auto& object = opts.object;
  auto& object_version = opts.object_version;
  auto& infile = opts.infile;
  auto& objects_file = opts.objects_file;
  auto& end_date = opts.end_date;
  auto& start_date = opts.start_date;
  int64_t min_rewrite_size = opts.min_rewrite_size;
  int64_t max_rewrite_size = opts.max_rewrite_size;
  uint64_t min_rewrite_stripe_size = opts.min_rewrite_stripe_size;
  bool yes_i_really_mean_it = opts.yes_i_really_mean_it;
  int ret = 0;

  if (command == OPT::OBJECT_PUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }

    RGWDataAccess data_access(driver);
    rgw_obj_key key(object, object_version);

    RGWDataAccess::BucketRef b;
    RGWDataAccess::ObjectRef obj;

    int ret = data_access.get_bucket(dpp, tenant, bucket_name, bucket_id, &b, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    ret = b->get_object(key, &obj);
    if (ret < 0) {
      cerr << "ERROR: failed to get object: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bufferlist bl;
    ret = rgw_admin_read_input(infile, bl);
    if (ret < 0) {
      cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    }

    map<string, bufferlist> attrs;
    ret = obj->put(bl, attrs, dpp, null_yield);
    if (ret < 0) {
      cerr << "ERROR: put object returned error: " << cpp_strerror(-ret) << std::endl;
    }
  }

  if (command == OPT::OBJECT_RM) {
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_obj_key key(object, object_version);

    ret = rgw_remove_object(dpp, driver, bucket.get(), key, null_yield, yes_i_really_mean_it);
    if (ret < 0) {
      cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::OBJECT_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);
    bool need_rewrite = true;
    if (min_rewrite_stripe_size > 0) {
      ret = rgw_admin_check_min_obj_stripe_size(dpp, driver, obj.get(), min_rewrite_stripe_size, &need_rewrite);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "WARNING: check_min_obj_stripe_size failed, r=" << ret << dendl;
      }
    }
    if (need_rewrite) {
      RGWRados* store = static_cast<rgw::sal::RadosStore*>(driver)->getRados();
      ret = store->rewrite_obj(bucket->get_info(), obj->get_obj(), dpp, null_yield);
      if (ret < 0) {
        cerr << "ERROR: object rewrite returned: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ldpp_dout(dpp, 20) << "skipped object" << dendl;
    }
  } // OPT::OBJECT_REWRITE

  if (command == OPT::OBJECT_REINDEX) {
    if (bucket_name.empty()) {
      cerr << "ERROR: --bucket not specified." << std::endl;
      return EINVAL;
    }
    if (object.empty() && objects_file.empty()) {
      cerr << "ERROR: neither --object nor --objects-file specified." << std::endl;
      return EINVAL;
    } else if (!object.empty() && !objects_file.empty()) {
      cerr << "ERROR: both --object and --objects-file specified and only one is allowed." << std::endl;
      return EINVAL;
    } else if (!objects_file.empty() && !object_version.empty()) {
      cerr << "ERROR: cannot specify --object_version when --objects-file specified." << std::endl;
      return EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
	"." << std::endl;
      return -ret;
    }

    rgw::sal::RadosStore* rados_store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!rados_store) {
      cerr <<
	"ERROR: this command can only work when the cluster has a RADOS backing store." <<
	std::endl;
      return EPERM;
    }
    RGWRados* store = rados_store->getRados();

    auto process = [&](const std::string& p_object, const std::string& p_object_version) -> int {
      std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(p_object);
      obj->set_instance(p_object_version);
      ret = store->reindex_obj(driver, bucket->get_info(), obj->get_obj(), dpp, null_yield);
      if (ret < 0) {
	return ret;
      }
      return 0;
    };

    if (!object.empty()) {
      ret = process(object, object_version);
      if (ret < 0) {
	return -ret;
      }
    } else {
      std::ifstream file;
      file.open(objects_file);
      if (!file.is_open()) {
	std::cerr << "ERROR: unable to open objects-file \"" <<
	  objects_file << "\"." << std::endl;
	return ENOENT;
      }

      std::string obj_name;
      while (std::getline(file, obj_name)) {
	std::string version;
	auto pos = obj_name.find('\t');
	if (pos != std::string::npos) {
	  version = obj_name.substr(1 + pos);
	  obj_name = obj_name.substr(0, pos);
	}

	ret = process(obj_name, version);
	if (ret < 0) {
	  std::cerr << "ERROR: while processing \"" << obj_name <<
	    "\", received " << cpp_strerror(-ret) << "." << std::endl;
	  if (!yes_i_really_mean_it) {
	    std::cerr <<
	      "NOTE: with *caution* you can use --yes-i-really-mean-it to push through errors and continue processing." <<
	      std::endl;
	    return -ret;
	  }
	}
      } // while
    }
  } // OPT::OBJECT_REINDEX

  if (command == OPT::OBJECTS_EXPIRE) {
    if (!driver->process_expired_objects(dpp, null_yield)) {
      cerr << "ERROR: process_expired_objects() processing returned error." << std::endl;
      return 1;
    }
  }

  if (command == OPT::OBJECTS_EXPIRE_STALE_LIST) {
    ret = RGWBucketAdminOp::fix_obj_expiry(driver, bucket_op, stream_flusher, dpp, null_yield, true);
    if (ret < 0) {
      cerr << "ERROR: listing returned " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::OBJECTS_EXPIRE_STALE_RM) {
    ret = RGWBucketAdminOp::fix_obj_expiry(driver, bucket_op, stream_flusher, dpp, null_yield, false);
    if (ret < 0) {
      cerr << "ERROR: removing returned " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::BUCKET_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    uint64_t start_epoch = 0;
    uint64_t end_epoch = 0;

    if (!end_date.empty()) {
      int ret = utime_t::parse_date(end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return EINVAL;
      }
    }
    if (!start_date.empty()) {
      int ret = utime_t::parse_date(start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return EINVAL;
      }
    }

    bool is_truncated = true;
    bool cls_filtered = true;

    rgw_obj_index_key marker;
    string empty_prefix;
    string empty_delimiter;

    formatter->open_object_section("result");
    formatter->dump_string("bucket", bucket_name);
    formatter->open_array_section("objects");

    constexpr uint32_t NUM_ENTRIES = 1000;
    uint16_t expansion_factor = 1;
    while (is_truncated) {
      RGWRados::ent_map_t result;
      result.reserve(NUM_ENTRIES);

      const auto& current_index = bucket->get_info().layout.current_index;
      int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->cls_bucket_list_ordered(
	dpp, bucket->get_info(), current_index, RGW_NO_SHARD,
	marker, empty_prefix, empty_delimiter,
	NUM_ENTRIES, true, expansion_factor,
	result, &is_truncated, &cls_filtered, &marker,
	null_yield,
	rgw_bucket_object_check_filter);
      if (r < 0 && r != -ENOENT) {
        cerr << "ERROR: failed operation r=" << r << std::endl;
      } else if (r == -ENOENT) {
        break;
      }

      if (result.size() < NUM_ENTRIES / 8) {
	++expansion_factor;
      } else if (result.size() > NUM_ENTRIES * 7 / 8 &&
		 expansion_factor > 1) {
	--expansion_factor;
      }

      for (auto iter = result.begin(); iter != result.end(); ++iter) {
        rgw_obj_key key = iter->second.key;
        rgw_bucket_dir_entry& entry = iter->second;

        formatter->open_object_section("object");
        formatter->dump_string("name", key.name);
        formatter->dump_string("instance", key.instance);
        formatter->dump_int("size", entry.meta.size);
        utime_t ut(entry.meta.mtime);
        ut.gmtime(formatter->dump_stream("mtime"));

        if ((static_cast<int64_t>(entry.meta.size) < min_rewrite_size) ||
            (static_cast<int64_t>(entry.meta.size) > max_rewrite_size) ||
            (start_epoch > 0 && start_epoch > static_cast<uint64_t>(ut.sec())) ||
            (end_epoch > 0 && end_epoch < static_cast<uint64_t>(ut.sec()))) {
          formatter->dump_string("status", "Skipped");
        } else {
	  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);

          bool need_rewrite = true;
          if (min_rewrite_stripe_size > 0) {
            r = rgw_admin_check_min_obj_stripe_size(dpp, driver, obj.get(), min_rewrite_stripe_size, &need_rewrite);
            if (r < 0) {
              ldpp_dout(dpp, 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
            }
          }
          if (!need_rewrite) {
            formatter->dump_string("status", "Skipped");
          } else {
            RGWRados* store = static_cast<rgw::sal::RadosStore*>(driver)->getRados();
            r = store->rewrite_obj(bucket->get_info(), obj->get_obj(), dpp, null_yield);
            if (r == 0) {
              formatter->dump_string("status", "Success");
            } else {
              formatter->dump_string("status", cpp_strerror(-r));
            }
          }
        }
        formatter->dump_int("flags", entry.flags);

        formatter->close_section();
        formatter->flush(cout);
      }
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::OBJECT_UNLINK) {
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<rgw_obj_index_key> oid_list;
    rgw_obj_key key(object, object_version);
    rgw_obj_index_key index_key;
    key.get_index_key(&index_key);
    oid_list.push_back(index_key);

    // note: under rados this removes directly from rados index objects
    ret = bucket->remove_objs_from_index(dpp, oid_list);
    if (ret < 0) {
      cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (command == OPT::OBJECT_STAT) {
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);

    ret = obj->get_obj_attrs(null_yield, dpp);
    if (ret < 0) {
      cerr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
    formatter->open_object_section("object_metadata");
    formatter->dump_string("name", object);
    formatter->dump_unsigned("size", obj->get_size());

    map<string, bufferlist>::iterator iter;
    map<string, bufferlist> other_attrs;
    for (iter = obj->get_attrs().begin(); iter != obj->get_attrs().end(); ++iter) {
      bufferlist& bl = iter->second;
      bool handled = false;
      if (iter->first == RGW_ATTR_MANIFEST) {
	handled = rgw_admin_decode_dump<RGWObjManifest>("manifest", bl, formatter);
      } else if (iter->first == RGW_ATTR_ACL) {
        handled = rgw_admin_decode_dump<RGWAccessControlPolicy>("policy", bl, formatter);
      } else if (iter->first == RGW_ATTR_ID_TAG) {
        handled = rgw_admin_dump_string("tag", bl, formatter);
      } else if (iter->first == RGW_ATTR_ETAG) {
        handled = rgw_admin_dump_string("etag", bl, formatter);
      } else if (iter->first == RGW_ATTR_COMPRESSION) {
        handled = rgw_admin_decode_dump<RGWCompressionInfo>("compression", bl, formatter);
      } else if (iter->first == RGW_ATTR_DELETE_AT) {
        handled = rgw_admin_decode_dump<utime_t>("delete_at", bl, formatter);
      } else if (iter->first == RGW_ATTR_TORRENT) {
        // contains bencoded binary data which shouldn't be output directly
        // TODO: decode torrent info for display as json?
        formatter->dump_string("torrent", "<contains binary data>");
        handled = true;
      } else if (iter->first == RGW_ATTR_PG_VER) {
        handled = rgw_admin_decode_dump<uint64_t>("pg_ver", bl, formatter);
      } else if (iter->first == RGW_ATTR_SOURCE_ZONE) {
        handled = rgw_admin_decode_dump<uint32_t>("source_zone", bl, formatter);
      } else if (iter->first == RGW_ATTR_RESTORE_EXPIRY_DATE) {
        handled = rgw_admin_decode_dump<ceph::real_time>("restore_expiry_date", bl, formatter);
      } else if (iter->first == RGW_ATTR_RESTORE_TIME) {
        handled = rgw_admin_decode_dump<ceph::real_time>("restore_time", bl, formatter);
      } else if (iter->first == RGW_ATTR_RESTORE_TYPE) {
        rgw::sal::RGWRestoreType rt;
        decode(rt, bl);
        formatter->dump_string("RestoreType", rgw::sal::rgw_restore_type_dump(rt));
        handled = true;
      } else if (iter->first == RGW_ATTR_RESTORE_STATUS) {
        rgw::sal::RGWRestoreStatus rs;
        decode(rs, bl);
        formatter->dump_string("RestoreStatus", rgw::sal::rgw_restore_status_dump(rs));
        handled = true;
      } else if (iter->first == RGW_ATTR_TRANSITION_TIME) {
        handled = rgw_admin_decode_dump<utime_t>("transition_time", bl, formatter);
      }

      if (!handled)
        other_attrs[iter->first] = bl;
    }

    utime_t ut{obj->get_mtime()};
    ut.gmtime(formatter->dump_stream("mtime"));


    formatter->open_object_section("attrs");
    for (iter = other_attrs.begin(); iter != other_attrs.end(); ++iter) {
      bufferlist& bl = iter->second;
      if (iter->first == RGW_ATTR_OBJ_REPLICATION_TIMESTAMP) {
        rgw_admin_decode_dump<ceph::real_time>("user.rgw.replicated-at", bl, formatter);
      } else if (iter->first == RGW_ATTR_RESTORE_TIME) {
        rgw_admin_decode_dump<ceph::real_time>("user.rgw.restore-at", bl, formatter);
      } else if (iter->first == RGW_ATTR_INTERNAL_MTIME) {
        rgw_admin_decode_dump<ceph::real_time>("user.rgw.rgw-internal-mtime", bl, formatter);
      } else {
        rgw_admin_dump_string(iter->first.c_str(), iter->second, formatter);
      }
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  } // OPT::OBJECT_STAT

  if (command == OPT::OBJECT_MANIFEST) {
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
	std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);

    ret = obj->get_obj_attrs(null_yield, dpp);
    if (ret < 0) {
      cerr << "ERROR: failed to retrieve object metadata, returned error: " <<
	cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    formatter->open_object_section("outer");  // name not displayed since top level
    formatter->dump_unsigned("size", obj->get_size());

    auto attr_iter = obj->get_attrs().find(RGW_ATTR_MANIFEST);
    if (attr_iter == obj->get_attrs().end()) {
      cerr << "ERROR: unable to find object manifest" << std::endl;
      return ENOENT;
    }

    RGWObjManifest m;
    try {
      auto part_iter = attr_iter->second.cbegin();
      decode(m, part_iter);
    } catch (buffer::error& err) {
      cerr << "ERROR: unable to decode manifest" << std::endl;
      return EIO;
    }

    rgw::sal::RadosStore* store =
      dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr << "ERROR: this command (currently) only works with "
	"RADOS back-ends" << std::endl;
      return EINVAL;
    }

    RGWRados* rados = store->getRados();

    rgw_obj head_obj = obj->get_obj();
    rgw_raw_obj raw_head_obj;
    store->get_raw_obj(m.get_head_placement_rule(), head_obj, &raw_head_obj);
    
    formatter->open_array_section("objects");
    unsigned index = 0;
    for (auto p = m.obj_begin(dpp); p != m.obj_end(dpp); ++p, ++index) {
      rgw_raw_obj raw_obj =  p.get_location().get_raw_obj(rados);

      if (index == 0 && raw_obj != raw_head_obj) {
	// we have a head object without data, so let's include it
	formatter->open_object_section("object"); // name not displayed since in array

	formatter->dump_int("index", -1);
	formatter->dump_unsigned("offset", 0);
	formatter->dump_unsigned("size", 0);
	
	formatter->open_object_section("raw_obj");
	raw_head_obj.dump(formatter);
	formatter->close_section(); // raw_obj

	formatter->close_section(); // object
      }

      formatter->open_object_section("object"); // name not displayed since in array

      formatter->dump_unsigned("index", index);
      formatter->dump_unsigned("part_id", p.get_cur_part_id());
      formatter->dump_unsigned("stripe_id", p.get_cur_stripe());
      formatter->dump_unsigned("offset", p.get_ofs());
      formatter->dump_unsigned("size", p.get_stripe_size());

      formatter->open_object_section("raw_obj");
      raw_obj.dump(formatter);
      formatter->close_section(); // raw_obj

      formatter->close_section(); // object
    }
    formatter->close_section(); // objects array

    formatter->close_section(); // outer
    formatter->flush(cout);
  } // OPT::OBJECT_MANIFEST

  return 0;
}
