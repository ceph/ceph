
#include "rgw/rgw_fdb.h"
#include "rgw/driver/fdb/fdb-cache-driver.h"

#include "common/dout.h"

namespace {

void msg(const DoutPrefixProvider *dpp, const std::string msg = "", const std::source_location& sl = std::source_location::current())
{
 ldpp_dout(dpp, 0) << fmt::format("FDB_CacheDriver [{} ({}, {})]: {}\n", sl.file_name(), sl.line(), sl.function_name(), msg) << dendl;
}

} // namespace

namespace rgw::cache {

FDB_CacheDriver::FDB_CacheDriver()
{
 // Set up database configuration.
}

FDB_CacheDriver::~FDB_CacheDriver()
{
 // It would be nice to log things here, but I need to cast
 // aside any "dout" about the situation first.

 // JFW: ...I don't think we have a lot to do here: our database handle will be destroyed along
 // with us.
}

int FDB_CacheDriver::initialize(const DoutPrefixProvider* dpp)
{
 msg(dpp, "initializing FDB...");
  dbh = lfdb::make_database();
 msg(dpp, "Ok!");

 return 0;
}

int FDB_CacheDriver::put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y)
{
 msg(dpp, fmt::format("set() \"{}\"...", key));
  lfdb::set(lfdb::make_transaction(dbh), key, bl, lfdb::commit_after_op::commit);
 msg(dpp, "Ok!");

 // JFW: TODO: not sure what to make of attrs

 return 0;
}

int FDB_CacheDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
 msg(dpp, fmt::format("get() \"{}\"", key));

 lfdb::get(lfdb::make_transaction(dbh), key, bl);

 // JFW: TODO: not sure what to make of attrs

 return 0;
}

rgw::AioResultList FDB_CacheDriver::get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
 msg(dpp);
 return {};
}

rgw::AioResultList FDB_CacheDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id)
{
 msg(dpp);
 return {};
}

int FDB_CacheDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y)
{
 msg(dpp);

 // As the semantics here are "update or create", we try to read the key and (regardless of success or failure) append
 // data to either the empty record (unsuccessful, or actually empty) or just write the new value:
 
 // JFW: efficiency note: we could eliminate extra copies here by producing output directly from the DB, or adding
 // a magic call for this:
 std::vector<std::uint8_t> value;

 auto txn = lfdb::make_transaction(dbh);

 lfdb::get(txn, key, value);

 // Using, for example, buffer::list::c_str() is non-const:
 const std::string data = bl_data.to_str();

 const auto& buffer = (const std::uint8_t *)data.data();
 value.insert(value.end(), buffer, data.size() + buffer);

 lfdb::set(txn, key, value, lfdb::commit_after_op::commit); 
 
 return 0;
}

int FDB_CacheDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y)
{
 msg(dpp);

 lfdb::erase(lfdb::make_transaction(dbh), key, lfdb::commit_after_op::commit);

 return 0;
}

int FDB_CacheDriver::rename(const DoutPrefixProvider* dpp, const std::string& oldKey, const std::string& newKey, optional_yield y)
{
 msg(dpp);

 // JFW: there appears to be no concern about overwriting extant destinatoin keys, so this is destructive:
 
 std::vector<std::uint8_t> data;
 
 auto txn = lfdb::make_transaction(dbh);

 lfdb::get(txn, oldKey, data);
 lfdb::set(txn, newKey, data, lfdb::commit_after_op::no_commit);
 lfdb::erase(txn, oldKey, lfdb::commit_after_op::commit); 

 return 0;
}

int FDB_CacheDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
 msg(dpp);

 // JFW: I don't entirely grok why attributes get their own special database handling, but I think they're just
 // ordinary DB entries that happen to have multiple values that need to be parsed back.

 return -1;
}

int FDB_CacheDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
 msg(dpp);
 return -1;
}

int FDB_CacheDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
 msg(dpp);
 return -1;
}

int FDB_CacheDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
 msg(dpp);
 return -1;
}

int FDB_CacheDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
 msg(dpp);
 return -1;
}

int FDB_CacheDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
 msg(dpp);
 return -1;
}

Partition FDB_CacheDriver::get_current_partition_info(const DoutPrefixProvider* dpp)
{
 msg(dpp);

 // JFW: I don't know what this is-- maybe from rgw_arn? An S3 thing?

 return {};
}

uint64_t FDB_CacheDriver::get_free_space(const DoutPrefixProvider* dpp)
{
 msg(dpp);

 // JFW: this may require us to get some stats from the FDB server and ask how much space it has?

 return -1;
}

void FDB_CacheDriver::shutdown()
{
 // JFW: I don't think this requires any special action from us, but if the FDB_CacheDriver object is meant
 // to destroy underlying handles we may need to change that-- I doubt it.
 // Unfortunately, without the "dpp" parameter I guess we don't get to write a log message...
}

int FDB_CacheDriver::restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func)
{
 msg(dpp);

 // JFW: as this always succeeds in the Redis implementation I'm referencing, we'll do the same-- I don't 
 // know what it's /supposed/ to do:

 return 0;
}

} // namespace rgw::cache

