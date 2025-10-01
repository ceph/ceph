// JFW: TODO: "retreive values" fucntion; KV serialization; using a variant for the values and hanlding things like nested vectors, etc. might go
// a function; functions that catch and log, etc. should just be consolidated.

#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory.h"

#include "shared/d4n_data.h"

#include <ranges>
#include <concepts>
#include <functional>

namespace rgw::d4n {

using bucket_entries = std::map<std::uint64_t, std::string>;
using reverse_bucket_entries = std::map<std::uint64_t, std::string, std::std::greater>;

} // namespace rgw::d4n

namespace {

std::optional<bucket_entries> retrieve_values(lfdb::database_handle dbh, std::string& key)
{
 std::vector<std::uint8_t> values_out_buffer;

 if(not lfdb::get(lfdb::make_transaction(dbh), key, values_out_buffer)) {
    return {};
 }

 bucket_entries out;
// JFW:TODO: convert values to entries

 return { out };
}

/* Sadly, the layer above us (in rgw_sal_d4n.cc, etc.) does not appear to be written with exceptions in
mind, so we'll need to do some translating. There may be retry behaviors, etc. we'll want to fiddle with later, 
also: */
auto wrap_fdb_exception(const DoutPrefixProvider *dpp, std::invokable auto&& fn)
try
{
 return fn();
}
catch(const fdb_exception& e)
{
 return log_error(dpp, "{}", e.what()), -1;
}

} // namespace

namespace rgw::d4n {

int FDB_BucketDirectory::zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, bool multi)
try
{
 return wrap_fdb_exception(dpp, [&&]() {

 // JFW: I don't really speak Redis, but it *looks* like these "Z" commands basically manage weird sets. It
 // may be that I have to re-implement a bunch of stuff to get the correct behavior. I am GUESSING that the
 // mental model is something like:
 // using redis_entries = map<bucket_id, map<score, string_entry>>;
 // ...sadly, this may not be great news for performance, since to update a particular member I have to first retreieve the whole set and then update the
 // value, which is a bit unfortunate.
 // Note: the "multi" member doesn't appear to have much to do here... which makes me wonder if it's more Redis magic. I hope not!
 // I'm just not quite sure what's happening here; good to ask about!

 bucket_entries e; 

 auto txn = lfdb::make_transaction(fdb);

 lfdb::get(txn, bucket_id, e);

 // Add-or-update:
 e[score] = member;

 lfdb::set(lfdb::make_transaction(dbh), bucket_id, e, lfdb::commit_after_op::commit);

 log_msg(dpp, "{} => ({} => {})", bucket_id, score, member);
 
 });
}
catch(const fdb_exception& e)
{
 return log_error(dpp, "{}", e.what()), -1;
}

int FDB_BucketDirectory::zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y, bool multi)
try
{
 bucket_entries e; 

 auto txn = lfdb::make_transaction(fdb);

 lfdb::get(txn, bucket_id, e);

 // According to the documentation, trying to remove a key with there's no data at all is an error:
 if(e.empty())
  return -ENOENT;

 // We remove the requested /entry/; non-extant members are ignored:
 e.erase(e.find(bucket_id));
 
 lfdb::set(lfdb::make_transaction(dbh), bucket_id, e, lfdb::commit_after_op::commit);

 log_msg(dpp, "goodbye, {}", bucket_id);

 return 0;
}
catch(const fdb_exception& e)
{
 log_error(dpp, "{}", e.what());
 return -1;
}

int FDB_BucketDirectory::zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y)
try
{
 // JFW: I think the idea is that we get back the keys found in search range... might take a little fiddling in our case.
 // JFW: what I've written below probably isnt' exactly what we want, but we can work with it...

 auto txn = lfdb::make_transaction(dbh);

 if(0 == offset && 0 == count) {
  lfdb::get(txn, lfdb::select { start, stop }, std::back_inserter(members));
 }
 else {
  // JFW: this requires extensions in libfdb-- quite do-able, but some work. Let's see if we actually ever use this first...
  // JFW: limits are do-able, but will again be a project in libfdb...
  msg("JFW: I was too lazy to implement offset and limit. offset = {}, count limit == {}", offset, count);
  return -1;
 }

 if(members.empty()) {
   msg("no data returned");
   return -ENOENT;
 }

 return 0;
}
catch(const fdb_exception& e)
{
 log_error(dpp, "{}", e.what());
 return -1;
}

int FDB_BucketDirectory::zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y)
{
 // JFW: this will be hopefully not /too/ bad... I need to get an understanding of what's going on with the cursors here. I guess if we need to match a pattern
 // against every key, this will be pretty expensive.... I don't think FDB has anything like that built in...

 msg("JFW: looks like ZSCAN is supposed to return count items that match a pattern? I will look at this more closely later.");

/*JFW:
  try {
    boost::system::error_code ec;
    request req;

    req.push("ZSCAN", bucket_id, cursor, "MATCH", pattern, "COUNT", count);

    boost::redis::generic_response resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "FDB_BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    std::vector<boost::redis::resp3::basic_node<std::__cxx11::basic_string<char> > > root_array;
    if (resp.has_value()) {
      root_array = resp.value();
      ldpp_dout(dpp, 20) << "FDB_BucketDirectory::" << __func__ << "() aggregate size is: " << root_array.size() << dendl;
      auto size = root_array.size();
      if (size >= 2) {
        //Nothing of interest at index 0, index 1 has the next cursor value
        next_cursor = std::stoull(root_array[1].value);

        //skip the first 3 values to get the actual member, score
        for (uint64_t i = 3; i < size; i = i+2) {
          members.emplace_back(root_array[i].value);
          ldpp_dout(dpp, 20) << "FDB_BucketDirectory::" << __func__ << "() member is: " << root_array[i].value << dendl;
        }
      }
    } else {
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "FDB_BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
*/
  return 0;
}

int FDB_BucketDirectory::zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y)
try
{
 // JFW: This one's a little odd, but I believe what we do is get the bucket and then return the index value that /inner value/ "member" is found at in the
 // "rank" out-parameter:
 //  e.g.: given bucket_id => { { 0, "member 0" }, { 1, "member 1" }, ... }, ZRANK "member 1" would be 1.
 //  Very much entertaining to say "zrank", like "drank".
 
 bucket_entries e;
 
 if(not lfdb::get(lfdb::make_transaction(dbh), bucket_id, e)) {
    return -ENOENT;
 }

 for(const auto& [k, v] : e) {
   if(member == v) {
    rank = std::stoi(k);
    return 0;
   }
 } 

 // JFW: The extant function doesn't seem to capture "record not found" vs. member entry not found, so I'm guessing we
 // just return the same to both:
 return -ENOENT; 
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

std::string ObjectDirectory::build_index(CacheObj* object) 
{
  return object->bucketName + "_" + object->objName;
}

int ObjectDirectory::exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
try
{
 return lfdb::key_exists(lfdb::make_transaction(dbh), build_index(object));
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -1;
}

int ObjectDirectory::set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
try
{
 std::string key = build_index(object);

 std::map<int, std::string> values = {
 	{ "objName", object->objName },
	{ "bucketName", object->bucketName },
	{ "creationTime", object->creationTime },
	{ "dirty", object->dirty },
	{ "hosts", object->hostsList | std::views::join_with("_") },
	{ "etag", object->etag },
	{ "objSize", std::to_string(object->size) },
	{ "userId", object->user_id },
	{ "displayName", object->display_name }
 };

 lfdb::set(lfdb::make_transaction(dbh), key, values, lfdb::commit_after_op::commit);

 return 0; 
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -1;
}

/*JFW:
  // For existing keys, call get method beforehand. 
  //  Sets completely overwrite existing values. 
  std::string key = build_index(object);

  std::string endpoint;
  std::list<std::string> redisValues;
    
  // Creating a redisValues of the entry's properties 
  redisValues.push_back("objName");
  redisValues.push_back(object->objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(object->bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(object->creationTime); 
  redisValues.push_back("dirty");
  int ret = -1;
  if ((ret = check_bool(std::to_string(object->dirty))) != -EINVAL) {
    object->dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(object->dirty));
  redisValues.push_back("hosts");

  for (auto const& host : object->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint);
  redisValues.push_back("etag");
  redisValues.push_back(object->etag);
  redisValues.push_back("objSize");
  redisValues.push_back(std::to_string(object->size));
  redisValues.push_back("userId");
  redisValues.push_back(object->user_id);
  redisValues.push_back("displayName");
  redisValues.push_back(object->display_name);

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push_range("HSET", key, redisValues);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}*/

int ObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
 auto key = build_index(object);

 std::map<std::string, std::string> values;

 if(not lfdb::get(lfdb::make_transaction(dbh), key, values, lfdb::commit_after_op::no_commit)) {
  return -ENOENT;
 }

 return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

/*JFW:
{
  std::string key = build_index(object);
  std::vector<std::string> fields;
  ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");
  fields.push_back("etag");
  fields.push_back("objSize");
  fields.push_back("userId");
  fields.push_back("displayName");

  try {
    boost::system::error_code ec;
    response< std::vector<std::string> > resp;
    request req;
    req.push_range("HMGET", key, fields);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values returned." << dendl;
      return -ENOENT;
    }

    using Fields = rgw::d4n::ObjectFields;
    object->objName = std::get<0>(resp).value()[std::size_t(Fields::ObjName)];
    object->bucketName = std::get<0>(resp).value()[std::size_t(Fields::BucketName)];
    object->creationTime = std::get<0>(resp).value()[std::size_t(Fields::CreationTime)];
    object->dirty = (std::stoi(std::get<0>(resp).value()[std::size_t(Fields::Dirty)]) != 0);
    boost::split(object->hostsList, std::get<0>(resp).value()[std::size_t(Fields::Hosts)], boost::is_any_of("_"));
    object->etag = std::get<0>(resp).value()[std::size_t(Fields::Etag)];
    object->size = std::stoull(std::get<0>(resp).value()[std::size_t(Fields::ObjSize)]);
    object->user_id = std::get<0>(resp).value()[std::size_t(Fields::UserID)];
    object->display_name = std::get<0>(resp).value()[std::size_t(Fields::DisplayName)];
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}*/

int ObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
try
{
 // JFW: I'm kinda again guessing at what this does (and why it might not be compatible with Ubuntu??). As best as I can tell, it just
 // makes a copy of something at one key somewhere with a new key... but I'm not completely sure what the "CacheObj" stuff is doing. Maybe
 // taking something locally off disk..?

 auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };

 auto source_key = build_index(object);
 auto dest_key = build_index(&copyObj);

 map<string, string> values;

 // JFW: something to convert CacheObj into a map<string, string>, I guess...

 lfdb::set(lfdb::make_transaction(dbh), dest_$key, values, lfdb::commit_after_op::commit);
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

/* JFW:
// Note: This method is not compatible for use on Ubuntu systems. 
int ObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  std::string key = build_index(object);
  auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };
  std::string copyKey = build_index(&copyObj);

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>> 
    > resp;
    request req;
    req.push("MULTI");
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values copied." << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}*/

int ObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
 auto key = build_index(object);
 lfdb::erase(lfdb::make_transaction(dbh), key, lfdb::commit_after_op::commit);
}
catch(const fdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

/* JFW:
{
  std::string key = build_index(object);
  ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("DEL", key);

    redis_exec(conn, ec, req, resp, y);

    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values deleted." << dendl;
      return -ENOENT;
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0; 
}*/

int ObjectDirectory::update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y)
{
 std::map<std::string, std::string> record_values;

 std::vector<std::uint8_t> values_input_buffer;

 auto key = build_index(object);

 if(not lfdb::get(lfdb::make_transaction(dbh), key, values_input_buffer)) {
  return -ENOENT;
 }

 // There are some special rules that make certain fields "magical", let's handle those:
 if("dirty" == field) {
     if(-EINVAL == check_bool(value)) {
	return -EINVAL;
     }
 }

 if("hosts" == field) {
	if(value.empty()) {
	 return -1;
 }

// JFW: the "right" way to do this is to read these back into a vector, append, and re-serialize:
	auto hosts = record_values.at("hosts");
	record_values.at("hosts") += "_" + value;
 }

 // Anything else, it's a straightforward create-or-update:
 record_values[field] = value;

 lfdb::set(lfdb::make_transaction(dbh), key, record_values, lfdb::commit_after_op::commit);

 return 0;
}
catch(const fdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}


/* JFW:
{
  int ret = -1;
  std::string key = build_index(object);

  if ((ret = exist_key(dpp, object, y))) {
    try {
      if (field == "hosts") {
	// Append rather than overwrite 
	ldpp_dout(dpp, 20) << "ObjectDirectory::" << __func__ << "(): Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response<std::string> resp;
	request req;
	req.push("HGET", key, field);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	// If entry exists, it should have at least one host 
	std::get<0>(resp).value() += "_";
	std::get<0>(resp).value() += value;
	value = std::get<0>(resp).value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
          bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): Object does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
}*/

int ObjectDirectory::zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, bool multi)
{
 // JFW: as near as I can tell in here, the idea is to create or update an entry at the given "score" index:
 auto values = fdb_retrieve_values(object);

 if(!values)
  return -ENOENT;

 values[score] = member;

 lfdb::set(lfdb::make_transaction(dbh), key, record_values, lfdb::commit_after_op::commit);

 return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

/* JFW: {
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZADD", key, "CH", std::to_string(score), member);

    response<std::string> resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}*/

int ObjectDirectory::zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y)
try
{
 // JFW: I'm not exactly sure what's expected as return value here-- a vector containing all members in all sets in the
 // range, but with mapping as strings?
 // e.g.: std::vector<bucket_entries> values;
 // I guess? Seems weird...

 std::vector<bucket_entries> values;

// JFW: it may be that we need to make the key from the object and then concatenate start and stop to get the right key range--
// however, the current implementation doesn't do that...
 if(not lfdb::get(ldb::make_transaction(dbh), lfdb::select { start, stop }, std::back_inserter(values)) {
   return -ENOENT;
 }

 // JFW: This feels... weird:
 std::ranges::for_each(values, [&members](const auto& entries) {
    members.push_back(entries | std::views::join_with("_"));
 });

 return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}
/* JFW:
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
} */

int ObjectDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y)
{
// JFW: As above, I'm not exactly sure what's expected as return value here-- a vector containing all members in all sets in the
 // range, but with reverse-mapping as strings?
 // e.g.: std::vector<reverse_bucket_entries> values; -> std::vector<string>??
 // I guess? Seems weird...
 
 std::vector<reverse_bucket_entries> values;

 if(not lfdb::get(ldb::make_transaction(dbh), lfdb::select { start, stop }, std::back_inserter(values)) {
   return -ENOENT;
 }

 std::ranges::for_each(values, [&members](const auto& entries) {
    members.push_back(entries | std::views::join_with("_"));
 });


 return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

/*JFW:
{
  // "As of Redis version 6.2.0, this command is regarded as deprecated."
  // (https://redis.io/docs/latest/commands/zrevrange/)

  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", key, start, stop);

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}*/

int ObjectDirectory::zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y, bool multi)
try
{
  auto key = build_index(object);

  map<string, string> kvs;
    
  // Load the named object and if found remove the named field then re-store:
  auto txn = lfdb::make_transaction(dbh);

  if(not lfdb::get(txn, key, kvs, lfdb::commit_after_op::no_commit) {
   return -ENOENT;
  }

  // Drop the member from the set:
  kvs.erase(member);

  lfdb::set(txn, key, kvs, lfdb::commit_after_op::commit);

/*
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", key, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
*/

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int ObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y, bool multi)
try
{
 // I'm not really sure what this is supposed to do... or how and if we still need it..?
 
 msg("JFW: I haven't implemented this");
 
/* JFW:
void fdb_transaction_clear_range(FDBTransaction *transaction, uint8_t const *begin_key_name, int begin_key_name_length, uint8_t const *end_key_name, int end_key_name_length) */

/* JFW:
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREMRANGEBYSCORE", key, std::to_string(min), std::to_string(max));
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() == "0") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() No element removed!" << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
*/
  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int ObjectDirectory::incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
try
{
// JFW: I think in our model, we'll have to store the INCR as a member value? I guess we increment it on every
// mutating touch..? Or every access..?

  msg("JFW: TODO");

/*
  std::string key = build_index(object);
  key = key + "_versioned_epoch";
  uint64_t value;
  try {
    boost::system::error_code ec;
    request req;
    req.push("INCR", key);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    value = std::stoull(std::get<0>(resp).value());

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return value;
*/

// JFW: value is number of increments, NOT success/failure; unclear if this function handles non-extant keys or not..?
  return 1;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int ObjectDirectory::zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y)
try
{
  // "ZRANK returns the rank of a member in a sorted set, ordered from low to high scores. The rank is 0-based."
  // https://redis.io/docs/latest/commands/zrank/
  //
  // ...so, in effect this is finding the key for one of the member values. 
  
  std::vector<bucket_entries> values;

 if(not lfdb::get(ldb::make_transaction(dbh), build_index(object), std::back_inserter(values)) {
   return -ENOENT;
 }

 // Search the values and return the key ("index") if found:
 std::ranges::for_each(values, [&members](const auto& entry) {
    if(member == entry.second) {
        index = member.first;
        return 0;
    }
 });

 return -ENOENT;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}




/* JFW:
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANK", key, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    index = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}*/

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
}

int BlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
try
{
  return lfdb::key_exists(lfdb::make_transaction(dbh), build_index(block));

/*
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return false;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
*/
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
try
{

 std::string key = build_index(object);

 if(auto ret = check_pool(std::to_string(block->deleteMarker)); -EINVAL == ret) {
   JFW 
 }

 std::map<int, std::string> values = {
    { "blockID", std::to_string(block->blockID) },
    { "version", block->version },
    { "deleteMarker" },
 };
  
//JFW: use zpp-bits or other suitiable sln library here-- growing it "into" the DB *probably* isn't the best idea,
//but I'll need to think about it
 msg(fmt::format("JFW associative arrays aren't implemented; {} => {}", key, values));

 std::vector<std::uint8_t> values_out_buffer;
 lfdb::set(lfdb::make_transaction(dbh), key, values_out_buffer, lfdb::commit_after_op::commit);
 
 return 0;

}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}



  /* For existing keys, call get method beforehand. 
     Sets completely overwrite existing values. */
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;
    
  std::string hosts;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("blockID");
  redisValues.push_back(std::to_string(block->blockID));
  redisValues.push_back("version");
  redisValues.push_back(block->version);
  redisValues.push_back("deleteMarker");
  int ret = -1;
  if ((ret = check_bool(std::to_string(block->deleteMarker))) != -EINVAL) {
    block->deleteMarker = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value for delete marker" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block->deleteMarker));
  redisValues.push_back("size");
  redisValues.push_back(std::to_string(block->size));
  redisValues.push_back("globalWeight");
  redisValues.push_back(std::to_string(block->globalWeight));
  redisValues.push_back("objName");
  redisValues.push_back(block->cacheObj.objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(block->cacheObj.bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(block->cacheObj.creationTime); 
  redisValues.push_back("dirty");
  if ((ret = check_bool(std::to_string(block->cacheObj.dirty))) != -EINVAL) {
    block->cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block->cacheObj.dirty));
  redisValues.push_back("hosts");
  
  hosts.clear();
  for (auto const& host : block->cacheObj.hostsList) {
    if (hosts.empty())
    hosts = host + "_";
    else
    hosts = hosts + host + "_";
  }

  if (!hosts.empty())
  hosts.pop_back();

  redisValues.push_back(hosts);
  redisValues.push_back("etag");
  redisValues.push_back(block->cacheObj.etag);
  redisValues.push_back("objSize");
  redisValues.push_back(std::to_string(block->cacheObj.size));
  redisValues.push_back("userId");
  redisValues.push_back(block->cacheObj.user_id);
  redisValues.push_back("displayName");
  redisValues.push_back(block->cacheObj.display_name);

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push_range("HSET", key, redisValues);

    redis_exec(conn, ec, req, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}*/

/* JFW:
template<typename T, typename Seq>
struct expander;

template<typename T, std::size_t... Is>
struct expander<T, std::index_sequence<Is...>> {
template<typename E, std::size_t>
using elem = E;

using type = boost::redis::response<elem<T, Is>...>;
};

template <size_t N, class Type>
struct redis_response
{
  using type = typename expander<Type, std::make_index_sequence<N>>::type;
};

template <typename Integer, Integer ...I, typename F>
constexpr void constexpr_for_each(std::integer_sequence<Integer, I...>, F &&func)
{
    (func(std::integral_constant<Integer, I>{}) , ...);
}

template <auto N, typename F>
constexpr void constexpr_for(F &&func)
{
    if constexpr (N > 0)
    {
        constexpr_for_each(std::make_integer_sequence<decltype(N), N>{}, std::forward<F>(func));
    }
}

template <typename T>
void parse_response(T t, std::vector<std::vector<std::string>>& responses)
{
    constexpr_for<std::tuple_size_v<T>>([&](auto index)
    {
      std::vector<std::string> empty_vector;
      constexpr auto i = index.value;
      if (std::get<i>(t).value().has_value()) {
        if (std::get<i>(t).value().value().empty()) {
          responses.emplace_back(empty_vector);
        } else {
          responses.emplace_back(std::get<i>(t).value().value());
        }
      } else {
        responses.emplace_back(empty_vector);
      }
    });
}*/

int BlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
try
{
  request req;
  redis_response<100, std::optional<std::vector<std::string>>>::type resp;
  for (auto block : blocks) {
    std::string key = build_index(&block);
    std::vector<std::string> fields;
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    fields.push_back("blockID");
    fields.push_back("version");
    fields.push_back("deleteMarker");
    fields.push_back("size");
    fields.push_back("globalWeight");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("hosts");
    fields.push_back("etag");
    fields.push_back("objSize");
    fields.push_back("userId");
    fields.push_back("displayName");

    try {
      req.push_range("HMGET", key, fields);
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } //end - for

  try {
    boost::system::error_code ec;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  std::vector<std::vector<std::string>> responses;
  parse_response<decltype(resp)>(resp, responses);

  for (size_t i = 0; i < blocks.size(); i++) {
    CacheBlock* block = &blocks[i];
    auto vec = responses[i];
    if (vec.empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned for key=" << build_index(block) << dendl;
      continue;
    }

    using Fields = rgw::d4n::BlockFields;
    block->blockID = std::stoull(vec[std::size_t(Fields::BlockID)]);
    block->version = vec[std::size_t(Fields::Version)];
    block->deleteMarker = (std::stoi(vec[std::size_t(Fields::DeleteMarker)]) != 0);
    block->size = std::stoull(vec[std::size_t(Fields::Size)]);
    block->globalWeight = std::stoull(vec[std::size_t(Fields::GlobalWeight)]);
    block->cacheObj.objName = vec[std::size_t(Fields::ObjName)];
    block->cacheObj.bucketName = vec[std::size_t(Fields::BucketName)];
    block->cacheObj.creationTime = vec[std::size_t(Fields::CreationTime)];
    block->cacheObj.dirty = (std::stoi(vec[std::size_t(Fields::Dirty)]) != 0);
    boost::split(block->cacheObj.hostsList, vec[std::size_t(Fields::Hosts)], boost::is_any_of("_"));
    block->cacheObj.etag = vec[std::size_t(Fields::Etag)];
    block->cacheObj.size = std::stoull(vec[std::size_t(Fields::ObjSize)]);
    block->cacheObj.user_id = vec[std::size_t(Fields::UserID)];
    block->cacheObj.display_name = vec[std::size_t(Fields::DisplayName)];
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}


int BlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
try
{
  std::string key = build_index(block);
  std::vector<std::string> fields;
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  fields.push_back("blockID");
  fields.push_back("version");
  fields.push_back("deleteMarker");
  fields.push_back("size");
  fields.push_back("globalWeight");

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");
  fields.push_back("etag");
  fields.push_back("objSize");
  fields.push_back("userId");
  fields.push_back("displayName");

  try {
    boost::system::error_code ec;
    response< std::optional<std::vector<std::string>> > resp;
    request req;
    req.push_range("HMGET", key, fields);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().value().empty()) {
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values returned for key=" << key << dendl;
      return -ENOENT;
    } 

    block->blockID = std::stoull(std::get<0>(resp).value().value()[0]);
    block->version = std::get<0>(resp).value().value()[1];
    block->deleteMarker = (std::stoi(std::get<0>(resp).value().value()[2]) != 0);
    block->size = std::stoull(std::get<0>(resp).value().value()[3]);
    block->globalWeight = std::stoull(std::get<0>(resp).value().value()[4]);
    block->cacheObj.objName = std::get<0>(resp).value().value()[5];
    block->cacheObj.bucketName = std::get<0>(resp).value().value()[6];
    block->cacheObj.creationTime = std::get<0>(resp).value().value()[7];
    block->cacheObj.dirty = (std::stoi(std::get<0>(resp).value().value()[8]) != 0);
    boost::split(block->cacheObj.hostsList, std::get<0>(resp).value().value()[9], boost::is_any_of("_"));
    block->cacheObj.etag = std::get<0>(resp).value().value()[10];
    block->cacheObj.size = std::stoull(std::get<0>(resp).value().value()[11]);
    block->cacheObj.user_id = std::get<0>(resp).value().value()[12];
    block->cacheObj.display_name = std::get<0>(resp).value().value()[13];
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}


/* Note: This method is not compatible for use on Ubuntu systems. */
int BlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
try
{
  std::string key = build_index(block);
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>> 
    > resp;
    request req;
    req.push("MULTI");
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values copied." << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}


int BlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, bool multi) 
try
{
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    request req;
    req.push("DEL", key);
    if (!multi) {
      response<int> resp;
      redis_exec(conn, ec, req, resp, y);
      if (!std::get<0>(resp).value()) {
        ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values deleted for key=" << key << dendl;
        return -ENOENT;
      }
    } else { //if delete is called as part of a transaction, the command will be queued, hence the response will be a string
      response<std::string> resp;
      redis_exec(conn, ec, req, resp, y);
    }
    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << std::endl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0; 
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
try
{
  int ret = -1;
  std::string key = build_index(block);

  if ((ret = exist_key(dpp, block, y))) {
    try {
      if (field == "hosts") { 
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "BlockDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response< std::optional<std::string> > resp;
	request req;
	req.push("HGET", key, field);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	/* If entry exists, it should have at least one host */
	std::get<0>(resp).value().value() += "_";
	std::get<0>(resp).value().value() += value;
	value = std::get<0>(resp).value().value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
          bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
try
{
  std::string key = build_index(block);

  try {
    {
      boost::system::error_code ec;
      response< std::optional<std::string> > resp;
      request req;
      req.push("HGET", key, "hosts");

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      if (std::get<0>(resp).value().value().empty()) {
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
	return -ENOENT;
      }

      std::string result = std::get<0>(resp).value().value();
      auto it = result.find(value);
      if (it != std::string::npos) { 
	result.erase(result.begin() + it, result.begin() + it + value.size());
      } else {
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Host was not found." << dendl;
	return -ENOENT;
      }

      if (result[0] == '_') {
	result.erase(0, 1);
      } else if (result.length() && result[result.length() - 1] == '_') {
	result.erase(result.length() - 1, 1);
      }

      if (result.length() == 0) /* Last host, delete entirely */
	return del(dpp, block, y); 

  value = result;
    }

    {qs
      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, "hosts", value);

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y, bool multi)
try
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZADD", key, "CH", std::to_string(score), member);

    response<std::string> resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -EINVAL;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
try
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Empty response" << dendl;
      return -EINVAL;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
try
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Empty response" << dendl;
      return -EINVAL;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y)
try
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", key, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::watch(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
try
{
// JFW: not sure what I'm really going to do with this one...

  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("WATCH", key);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::exec(const DoutPrefixProvider* dpp, std::vector<std::string>& responses, optional_yield y)
try
{
// JFW: Unclear what the FDB equivalent would be, other than committing a transaction... this is probably best
// modeled as a NOP.

/*JFW:
  try {
    boost::system::error_code ec;
    request req;
    req.push("EXEC");
    boost::redis::generic_response resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << std::endl;
      return -ec.value();
    }

    for (uint64_t i = 0; i < resp.value().size(); i++) {
      ldpp_dout(dpp, 20) << "BlockDirectory::" << __func__ << "() MULTI: " << resp.value().front().value << dendl;
      responses.emplace_back(resp.value().front().value);
      boost::redis::consume_one(resp);
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << std::endl;
    return -EINVAL;
  }
*/
  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::multi(const DoutPrefixProvider* dpp, optional_yield y)
try
{
// JFW: in Redis, this starts a transaction... for us, it's basically a NOP.
/*
  try {
    boost::system::error_code ec;
    request req;
    req.push("MULTI");
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
*/
  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::discard(const DoutPrefixProvider* dpp, optional_yield y)
try
{
// In Redis-land, this abandons a transaction. I'll need to think about what this means for us, if anything.

/*JFW:
  try {
    boost::system::error_code ec;
    request req;
    req.push("DISCARD");
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
*/

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

int BlockDirectory::unwatch(const DoutPrefixProvider* dpp, optional_yield y)
try
{
// JFW: same as the other transaction-oriented operations... not entirely sure what to map this to in FDB.

/*
  try {
    boost::system::error_code ec;
    request req;
    req.push("UNWATCH");
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
*/

  return 0;
}
catch(const lfdb::exception& e)
{
 log_error(dpp, "{}", e.what());
 return -EINVAL;
}

} // namespace rgw::d4n
