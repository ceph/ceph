// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/bi.h"
#include <iostream>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/buffer.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "radosgw-admin/bucket.h"
#include "radosgw-admin/admin_io.h"


using namespace rgw_admin;
using namespace std;

namespace {



} // anonymous namespace

int rgw_admin_bi(const DoutPrefixProvider* dpp,
                 rgw::sal::Driver* driver,
                 ceph::Formatter* formatter,
                 std::unique_ptr<rgw::sal::Bucket>& bucket,
                 const rgw_admin_bi_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& bucket_id = *opts.bucket_id;
  auto& object = *opts.object;
  auto& object_version = *opts.object_version;
  auto& infile = *opts.infile;
  auto& marker = *opts.marker;
  int max_entries = opts.max_entries;
  int shard_id = opts.shard_id;
#ifdef WITH_RADOSGW_RADOS
  BIIndexType bi_index_type = opts.bi_index_type;
#endif
  bool max_entries_specified = opts.max_entries_specified;
  bool specified_shard_id = opts.specified_shard_id;
  bool yes_i_really_mean_it = opts.yes_i_really_mean_it;

  if (command == OPT::BI_GET) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
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
    rgw_obj obj(bucket->get_key(), object);
    if (!object_version.empty()) {
      obj.key.set_instance(object_version);
    }

    rgw_cls_bi_entry entry;
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_get(dpp, bucket->get_info(), obj, bi_index_type, &entry, null_yield);
    if (ret < 0) {
      cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("entry", entry, formatter);
    formatter->flush(cout);
  }

  if (command == OPT::BI_PUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_cls_bi_entry entry;
    cls_rgw_obj_key key;
    ret = rgw_admin_read_decode_json(infile, entry, &key);
    if (ret < 0) {
      return 1;
    }

    rgw_obj obj(bucket->get_key(), key);

    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_put(dpp, bucket->get_key(), obj, entry, null_yield);
    if (ret < 0) {
      cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::BI_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: could not init bucket: " <<
	cpp_strerror(-ret) << dendl;
      return -ret;
    }

    std::list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    const auto& index = bucket->get_info().layout.current_index;
    if (index.layout.type == rgw::BucketIndexType::Indexless) {
      cerr << "Error: indexless bucket has no index to list" << std::endl;
      return EINVAL;
    }

    const int max_shards = rgw::num_shards(index);

    if (max_entries_specified) {
      max_entries = std::max(1, max_entries); // sanity
    } else {
      max_entries = 1000;
    }

    ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": max_entries=" <<
      max_entries << ", index=" << index << ", max_shards=" << max_shards <<
      dendl;

    formatter->open_array_section("entries");

    auto rados = static_cast<rgw::sal::RadosStore*>(driver)->getRados();
    int64_t entry_count = 0; // track number of entries displayed
    bool done = false;       // true once reached max_entries

    int i = (specified_shard_id ? shard_id : 0);
    for (; i < max_shards && !done; i++) {
      ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": starting shard=" <<
	i << dendl;
      marker.clear();

      RGWRados::BucketShard bs(rados);
      int ret = bs.init(dpp, bucket->get_info(), index, i, null_yield);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR: bs.init(bucket=" << bucket <<
	  ", shard=" << i << "): " << cpp_strerror(-ret) << dendl;
        return -ret;
      }

      do {
        entries.clear();
	// if object is specified, we use that as a filter to only
	// retrieve some entries
        ret = rados->bi_list(bs, object, marker, max_entries, &entries,
			     &is_truncated, false, null_yield);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: bi_list(): " <<
	    cpp_strerror(-ret) << dendl;
          return -ret;
        }

	for (const auto& entry : entries) {
          encode_json("entry", entry, formatter);
          marker = entry.idx;

          if (++entry_count >= max_entries) {
            done = true;
            ldpp_dout(dpp, 20) << "INFO: " << __func__ <<
              ": bi_list() stopped outputting entries after " << entry_count <<
              " entries given that max_entries=" << max_entries << dendl;
            break;
          }
        }
        formatter->flush(cout);

	ldpp_dout(dpp, 20) << "INFO: " << __func__ <<
	  ": bi_list() returned without error; entries.size()=" <<
	  entries.size() << ", is_truncated=" << is_truncated <<
	  ", next_marker=" << marker << dendl;
      } while (is_truncated && !done);

      formatter->flush(cout);

      if (specified_shard_id) {
        break;
      }
    } // shard loop
    ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": done" << dendl;

    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::BI_PURGE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Bucket> cur_bucket;
    ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, string(), &cur_bucket);
    if (ret == -ENOENT) {
      // no bucket entrypoint
    } else if (ret < 0) {
      cerr << "ERROR: could not init current bucket info for bucket_name=" << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    } else if (cur_bucket->get_bucket_id() == bucket->get_bucket_id() &&
               !yes_i_really_mean_it) {
      cerr << "specified bucket instance points to a current bucket instance" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EINVAL;
    }

    const auto& index = bucket->get_info().layout.current_index;
    if (index.layout.type == rgw::BucketIndexType::Indexless) {
      cerr << "ERROR: indexless bucket has no index to purge" << std::endl;
      return EINVAL;
    }

    const int max_shards = rgw::num_shards(index);
    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(driver)->getRados());
      int ret = bs.init(dpp, bucket->get_info(), index, i, null_yield);
      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << i << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_remove(dpp, bs);
      if (ret < 0) {
        cerr << "ERROR: failed to remove bucket index object: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  return 0;
}
