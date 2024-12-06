// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string>
#include <map>

#include "rgw_rados.h"
#include "rgw_usage.h"
#include "rgw_formats.h"
#include "rgw_sal.h"

using namespace std;

static void dump_usage_categories_info(Formatter *formatter, const rgw_usage_log_entry& entry, map<string, bool> *categories)
{
  formatter->open_array_section("categories");
  map<string, rgw_usage_data>::const_iterator uiter;
  for (uiter = entry.usage_map.begin(); uiter != entry.usage_map.end(); ++uiter) {
    if (categories && !categories->empty() && !categories->count(uiter->first))
      continue;
    const rgw_usage_data& usage = uiter->second;
    formatter->open_object_section("entry");
    formatter->dump_string("category", uiter->first);
    formatter->dump_unsigned("bytes_sent", usage.bytes_sent);
    formatter->dump_unsigned("bytes_received", usage.bytes_received);
    formatter->dump_unsigned("ops", usage.ops);
    formatter->dump_string("storage_class", usage.storage_class);
    formatter->dump_unsigned("successful_ops", usage.successful_ops);
    formatter->close_section(); // entry
  }
  formatter->close_section(); // categories
}

int RGWUsage::show(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		  rgw::sal::User* user , rgw::sal::Bucket* bucket,
		   uint64_t start_epoch, uint64_t end_epoch, bool show_log_entries,
		   bool show_log_sum,
		   map<string, bool> *categories, RGWFormatterFlusher& flusher)
{
  uint32_t max_entries = 1000;

  bool is_truncated = true;

  RGWUsageIter usage_iter;
  Formatter *formatter = flusher.get_formatter();

 
  flusher.start(0);
  map<rgw_user_bucket, rgw_usage_log_entry> usage;
  map<rgw_bucket_dir_entry_meta, rgw_usage_log_entry> usage1;
  map<string, rgw_bucket_dir_entry> test_map;
  map<string, rgw_bucket_dir_entry>::iterator tester;
  // ldpp_dout(dpp, 0) << "WARNING: could not decode policy for MIKATEST: " << usage << dendl; 
  
  formatter->open_object_section("usage");
  formatter->open_array_section("test");
    for (tester = test_map.begin(); tester != test_map.end(); ++tester) {
      const rgw_bucket_dir_entry& entry1 = tester->second;
      ldpp_dout(dpp, 0) << "WARNING: could not decode policy for MIKATEST: " << entry1.tag << dendl; 
      // formatter->dump_string("storage_classes", entry1.meta);
      formatter->open_object_section("user");
      formatter->dump_string("users", entry1.tag);
      formatter->close_section();
      formatter->dump_string("owner1", "11111");
  }
  formatter->close_section();
  flusher.flush();
  if (show_log_entries) {
    formatter->open_array_section("entries");
  }
  string last_owner;
  bool user_section_open = false;
  map<string, rgw_usage_log_entry> summary_map;
  int ret;

  while (is_truncated) {
    if (bucket) {
      ret = bucket->read_usage(dpp, start_epoch, end_epoch, max_entries, &is_truncated,
			       usage_iter, usage);
    } else if (user) {
      ret = user->read_usage(dpp, start_epoch, end_epoch, max_entries, &is_truncated,
			     usage_iter, usage);
    } else {
      ret = store->read_all_usage(dpp, start_epoch, end_epoch, max_entries, &is_truncated,
				  usage_iter, usage);
    }

    if (ret == -ENOENT) {
      ret = 0;
      is_truncated = false;
    }

    if (ret < 0) {
      return ret;
    }
  ldpp_dout(dpp, 0) << "WARNING: could not decode policy for Anhduc123: " << "123123" << dendl; 
  // ldpp_dout(dpp, 0) << "WARNING: could not decode policy for MIKATEST: " << test_map << dendl; 
  map<rgw_bucket_dir_entry_meta, rgw_usage_log_entry>::iterator iter1;
    for (iter1 = usage1.begin(); iter1 != usage1.end(); ++iter1) {
      const rgw_bucket_dir_entry_meta& ub = iter1->first;
      const rgw_usage_log_entry& entry = iter1->second;

      if (show_log_entries) {
        // if (ub.owner.compare(last_owner) != 0) {
        //   if (user_section_open) {
        //     formatter->close_section();
        //     formatter->close_section();
        //   }
        //   formatter->open_object_section("user");
        //   formatter->dump_string("user", ub.owner);
        //   formatter->open_array_section("buckets");
        //   user_section_open = true;
        //   last_owner = ub.owner;
        // }
        formatter->open_object_section("bucket");
        formatter->dump_string("bucket", ub.storage_class);
        formatter->dump_string("owner1", "11211");
        utime_t ut(entry.epoch, 0);
        ut.gmtime(formatter->dump_stream("time"));
        formatter->dump_int("epoch", entry.epoch);
        string owner = entry.owner.to_str();
        string payer = entry.payer.to_str();
        formatter->dump_string("owner", owner);
        if (!payer.empty() && payer != owner) {
          formatter->dump_string("payer", payer);
        }
        // dump_usage_categories_info(formatter, entry, categories);
        formatter->close_section(); // bucket
        flusher.flush();
      }

      // summary_map[ub.user].aggregate(entry, categories);
    }
  
    
    map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;
    for (iter = usage.begin(); iter != usage.end(); ++iter) {
      const rgw_user_bucket& ub = iter->first;
      const rgw_usage_log_entry& entry = iter->second;

      if (show_log_entries) {
        if (ub.user.compare(last_owner) != 0) {
          if (user_section_open) {
            formatter->close_section();
            formatter->close_section();
          }
          formatter->open_object_section("user");
          formatter->dump_string("user", ub.user);
          formatter->open_array_section("buckets");
          user_section_open = true;
          last_owner = ub.user;
        }
        formatter->open_object_section("bucket");
        formatter->dump_string("bucket", ub.bucket);
        formatter->dump_string("owner1", "11111");
        utime_t ut(entry.epoch, 0);
        ut.gmtime(formatter->dump_stream("time"));
        formatter->dump_int("epoch", entry.epoch);
        string owner = entry.owner.to_str();
        string payer = entry.payer.to_str();
        formatter->dump_string("owner", owner);
        if (!payer.empty() && payer != owner) {
          formatter->dump_string("payer", payer);
        }
        dump_usage_categories_info(formatter, entry, categories);
        formatter->close_section(); // bucket
        flusher.flush();
      }

      summary_map[ub.user].aggregate(entry, categories);
    }
  }
  if (show_log_entries) {
    if (user_section_open) {
      formatter->close_section(); // buckets
      formatter->close_section(); //user
    }
    formatter->close_section(); // entries
  }

  if (show_log_sum) {
    formatter->open_array_section("summary");
    map<string, rgw_usage_log_entry>::iterator siter;
    for (siter = summary_map.begin(); siter != summary_map.end(); ++siter) {
      const rgw_usage_log_entry& entry = siter->second;
      formatter->open_object_section("user");
      formatter->dump_string("user", siter->first);
      dump_usage_categories_info(formatter, entry, categories);
      rgw_usage_data total_usage;
      entry.sum(total_usage, *categories);
      formatter->open_object_section("total");
      encode_json("bytes_sent", total_usage.bytes_sent, formatter);
      encode_json("bytes_received", total_usage.bytes_received, formatter);
      encode_json("ops", total_usage.ops, formatter);
      encode_json("successful_ops", total_usage.successful_ops, formatter);
      formatter->close_section(); // total

      formatter->close_section(); // user

      flusher.flush();
    }

    formatter->close_section(); // summary
  }

  formatter->close_section(); // usage
  flusher.flush();

  return 0;
}

int RGWUsage::trim(const DoutPrefixProvider *dpp, rgw::sal::Store* store,
		   rgw::sal::User* user , rgw::sal::Bucket* bucket,
		   uint64_t start_epoch, uint64_t end_epoch)
{
  if (bucket) {
    return bucket->trim_usage(dpp, start_epoch, end_epoch);
  } else if (user) {
    return user->trim_usage(dpp, start_epoch, end_epoch);
  } else {
    return store->trim_all_usage(dpp, start_epoch, end_epoch);
  }
}

int RGWUsage::clear(const DoutPrefixProvider *dpp, rgw::sal::Store* store)
{
  return store->clear_usage(dpp);
}
