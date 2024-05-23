// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2023 IBM
 *
 * See file COPYING for licensing information.
 */

#include <iostream>
#include <fstream>
#include <mutex>
#include <map>
#include <algorithm>

#include "arrow/type.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/reader.h"
#include "arrow/table.h"

#include "arrow/flight/server.h"

#include "parquet/arrow/reader.h"

#include "common/dout.h"
#include "rgw_op.h"

#include "rgw_flight.h"
#include "rgw_flight_frontend.h"


namespace rgw::flight {

// Ticket and FlightKey

std::atomic<FlightKey> next_flight_key = null_flight_key;

flt::Ticket FlightKeyToTicket(const FlightKey& key) {
  flt::Ticket result;
  result.ticket = std::to_string(key);
  return result;
}

arw::Result<FlightKey> TicketToFlightKey(const flt::Ticket& t) {
  try {
    return (FlightKey) std::stoul(t.ticket);
  } catch (std::invalid_argument const& ex) {
    return arw::Status::Invalid(
      "could not convert Ticket containing \"%s\" into a Flight Key",
      t.ticket);
  } catch (const std::out_of_range& ex) {
    return arw::Status::Invalid(
      "could not convert Ticket containing \"%s\" into a Flight Key due to range",
      t.ticket);
  }
}

// FlightData

FlightData::FlightData(const std::string& _uri,
		       const std::string& _tenant_name,
		       const std::string& _bucket_name,
		       const rgw_obj_key& _object_key,
		       uint64_t _num_records,
		       uint64_t _obj_size,
		       std::shared_ptr<arw::Schema>& _schema,
		       std::shared_ptr<const arw::KeyValueMetadata>& _kv_metadata,
		       rgw_user _user_id) :
  key(++next_flight_key),
  /* expires(coarse_real_clock::now() + lifespan), */
  uri(_uri),
  tenant_name(_tenant_name),
  bucket_name(_bucket_name),
  object_key(_object_key),
  num_records(_num_records),
  obj_size(_obj_size),
  schema(_schema),
  kv_metadata(_kv_metadata),
  user_id(_user_id)
{ }

/**** FlightStore ****/

FlightStore::FlightStore(const DoutPrefix& _dp) :
  dp(_dp)
{ }

FlightStore::~FlightStore() { }

/**** MemoryFlightStore ****/

MemoryFlightStore::MemoryFlightStore(const DoutPrefix& _dp) :
  FlightStore(_dp)
{ }

MemoryFlightStore::~MemoryFlightStore() { }

FlightKey MemoryFlightStore::add_flight(FlightData&& flight) {
  std::pair<decltype(map)::iterator,bool> result;
  {
    const std::lock_guard lock(mtx);
    result = map.insert( {flight.key, std::move(flight)} );
  }
  ceph_assertf(result.second,
	       "unable to add FlightData to MemoryFlightStore"); // temporary until error handling

  return result.first->second.key;
}

arw::Result<FlightData> MemoryFlightStore::get_flight(const FlightKey& key) const {
  const std::lock_guard lock(mtx);
  auto i = map.find(key);
  if (i == map.cend()) {
    return arw::Status::KeyError("could not find Flight with Key %" PRIu32,
				 key);
  } else {
    return i->second;
  }
}

// returns either the next FilghtData or, if at end, empty optional
std::optional<FlightData> MemoryFlightStore::after_key(const FlightKey& key) const {
  std::optional<FlightData> result;
  {
    const std::lock_guard lock(mtx);
    auto i = map.upper_bound(key);
    if (i != map.end()) {
      result = i->second;
    }
  }
  return result;
}

int MemoryFlightStore::remove_flight(const FlightKey& key) {
  return 0;
}

int MemoryFlightStore::expire_flights() {
  return 0;
}

/**** FlightServer ****/

FlightServer::FlightServer(RGWProcessEnv& _env,
			   FlightStore* _flight_store,
			   const DoutPrefix& _dp) :
  env(_env),
  driver(env.driver),
  dp(_dp),
  flight_store(_flight_store)
{ }

FlightServer::~FlightServer()
{ }


arw::Status FlightServer::ListFlights(const flt::ServerCallContext& context,
				      const flt::Criteria* criteria,
				      std::unique_ptr<flt::FlightListing>* listings) {

  // function local class to implement FlightListing interface
  class RGWFlightListing : public flt::FlightListing {

    FlightStore* flight_store;
    FlightKey previous_key;

  public:

    RGWFlightListing(FlightStore* flight_store) :
      flight_store(flight_store),
      previous_key(null_flight_key)
      { }

    arrow::Result<std::unique_ptr<flt::FlightInfo>> Next() override {
      std::optional<FlightData> fd = flight_store->after_key(previous_key);
      if (fd) {
	previous_key = fd->key;
	auto descriptor =
	  flt::FlightDescriptor::Path(
	    { fd->tenant_name, fd->bucket_name, fd->object_key.name, fd->object_key.instance, fd->object_key.ns });
	flt::FlightEndpoint endpoint;
	endpoint.ticket = FlightKeyToTicket(fd->key);
	std::vector<flt::FlightEndpoint> endpoints { endpoint };

	ARROW_ASSIGN_OR_RAISE(flt::FlightInfo info_obj,
			      flt::FlightInfo::Make(*fd->schema, descriptor, endpoints, fd->num_records, fd->obj_size));
	return std::make_unique<flt::FlightInfo>(std::move(info_obj));
      } else {
	return nullptr;
      }
    }
  }; // class RGWFlightListing

  *listings = std::make_unique<RGWFlightListing>(flight_store);
  return arw::Status::OK();
} // FlightServer::ListFlights


arw::Status FlightServer::GetFlightInfo(const flt::ServerCallContext &context,
					const flt::FlightDescriptor &request,
					std::unique_ptr<flt::FlightInfo> *info) {
  return arw::Status::OK();
} // FlightServer::GetFlightInfo


arw::Status FlightServer::GetSchema(const flt::ServerCallContext &context,
				    const flt::FlightDescriptor &request,
				    std::unique_ptr<flt::SchemaResult> *schema) {
  return arw::Status::OK();
} // FlightServer::GetSchema

  // A Buffer that owns its memory and frees it when the Buffer is
  // destructed
class OwnedBuffer : public arw::Buffer {

  uint8_t* buffer;

protected:

  OwnedBuffer(uint8_t* _buffer, int64_t _size) :
    Buffer(_buffer, _size),
    buffer(_buffer)
    { }

public:

  ~OwnedBuffer() override {
    delete[] buffer;
  }

  static arw::Result<std::shared_ptr<OwnedBuffer>> make(int64_t size) {
    uint8_t* buffer = new (std::nothrow) uint8_t[size];
    if (!buffer) {
      return arw::Status::OutOfMemory("could not allocated buffer of size %" PRId64, size);
    }

    OwnedBuffer* ptr = new OwnedBuffer(buffer, size);
    std::shared_ptr<OwnedBuffer> result;
    result.reset(ptr);
    return result;
  }

  // if what's read in is less than capacity
  void set_size(int64_t size) {
    size_ = size;
  }

  // pointer that can be used to write into buffer
  uint8_t* writeable_data() {
    return buffer;
  }
}; // class OwnedBuffer

#if 0 // remove classes used for testing and incrementally building

// make local to DoGet eventually
class LocalInputStream : public arw::io::InputStream {

  std::iostream::pos_type position;
  std::fstream file;
  std::shared_ptr<const arw::KeyValueMetadata> kv_metadata;
  const DoutPrefix dp;

public:

  LocalInputStream(std::shared_ptr<const arw::KeyValueMetadata> _kv_metadata,
		   const DoutPrefix _dp) :
    kv_metadata(_kv_metadata),
    dp(_dp)
    {}

  arw::Status Open() {
    file.open("/tmp/green_tripdata_2022-04.parquet", std::ios::in);
    if (!file.good()) {
      return arw::Status::IOError("unable to open file");
    }

    INFO << "file opened successfully" << dendl;
    position = file.tellg();
    return arw::Status::OK();
  }

  arw::Status Close() override {
    file.close();
    INFO << "file closed" << dendl;
    return arw::Status::OK();
  }

  arw::Result<int64_t> Tell() const override {
    if (position < 0) {
      return arw::Status::IOError(
	"could not query file implementaiton with tellg");
    } else {
      return int64_t(position);
    }
  }

  bool closed() const override {
    return file.is_open();
  }

  arw::Result<int64_t> Read(int64_t nbytes, void* out) override {
    INFO << "entered: asking for " << nbytes << " bytes" << dendl;
    if (file.read(reinterpret_cast<char*>(out),
		  reinterpret_cast<std::streamsize>(nbytes))) {
      const std::streamsize bytes_read = file.gcount();
      INFO << "Point A: read bytes " << bytes_read << dendl;
      position = file.tellg();
      return bytes_read;
    } else {
      ERROR << "unable to read from file" << dendl;
      return arw::Status::IOError("unable to read from offset %" PRId64,
				  int64_t(position));
    }
  }

  arw::Result<std::shared_ptr<arw::Buffer>> Read(int64_t nbytes) override {
    INFO << "entered: " << ": asking for " << nbytes << " bytes" << dendl;

    std::shared_ptr<OwnedBuffer> buffer;
    ARROW_ASSIGN_OR_RAISE(buffer, OwnedBuffer::make(nbytes));

    if (file.read(reinterpret_cast<char*>(buffer->writeable_data()),
		  reinterpret_cast<std::streamsize>(nbytes))) {
      const auto bytes_read = file.gcount();
      INFO << "Point B: read bytes " << bytes_read << dendl;
      // buffer->set_size(bytes_read);
      position = file.tellg();
      return buffer;
    } else if (file.rdstate() & std::ifstream::failbit &&
	       file.rdstate() & std::ifstream::eofbit) {
      const auto bytes_read = file.gcount();
      INFO << "3 read bytes " << bytes_read << " and reached EOF" << dendl;
      // buffer->set_size(bytes_read);
      position = file.tellg();
      return buffer;
    } else {
      ERROR << "unable to read from file" << dendl;
      return arw::Status::IOError("unable to read from offset %ld", position);
    }
  }

  arw::Result<std::string_view> Peek(int64_t nbytes) override {
    INFO << "called, not implemented" << dendl;
    return arw::Status::NotImplemented("peek not currently allowed");
  }

  bool supports_zero_copy() const override {
    return false;
  }

  arw::Result<std::shared_ptr<const arw::KeyValueMetadata>> ReadMetadata() override {
    INFO << "called" << dendl;
    return kv_metadata;
  }
}; // class LocalInputStream

class LocalRandomAccessFile : public arw::io::RandomAccessFile {

  FlightData flight_data;
  const DoutPrefix dp;

  std::iostream::pos_type position;
  std::fstream file;

public:
  LocalRandomAccessFile(const FlightData& _flight_data, const DoutPrefix _dp) :
    flight_data(_flight_data),
    dp(_dp)
    { }

  // implement InputStream

  arw::Status Open() {
    file.open("/tmp/green_tripdata_2022-04.parquet", std::ios::in);
    if (!file.good()) {
      return arw::Status::IOError("unable to open file");
    }

    INFO << "file opened successfully" << dendl;
    position = file.tellg();
    return arw::Status::OK();
  }

  arw::Status Close() override {
    file.close();
    INFO << "file closed" << dendl;
    return arw::Status::OK();
  }

  arw::Result<int64_t> Tell() const override {
    if (position < 0) {
      return arw::Status::IOError(
	"could not query file implementaiton with tellg");
    } else {
      return int64_t(position);
    }
  }

  bool closed() const override {
    return file.is_open();
  }

  arw::Result<int64_t> Read(int64_t nbytes, void* out) override {
    INFO << "entered: asking for " << nbytes << " bytes" << dendl;
    if (file.read(reinterpret_cast<char*>(out),
		  reinterpret_cast<std::streamsize>(nbytes))) {
      const std::streamsize bytes_read = file.gcount();
      INFO << "Point A: read bytes " << bytes_read << dendl;
      position = file.tellg();
      return bytes_read;
    } else {
      ERROR << "unable to read from file" << dendl;
      return arw::Status::IOError("unable to read from offset %" PRId64,
				  int64_t(position));
    }
  }

  arw::Result<std::shared_ptr<arw::Buffer>> Read(int64_t nbytes) override {
    INFO << "entered: asking for " << nbytes << " bytes" << dendl;

    std::shared_ptr<OwnedBuffer> buffer;
    ARROW_ASSIGN_OR_RAISE(buffer, OwnedBuffer::make(nbytes));

    if (file.read(reinterpret_cast<char*>(buffer->writeable_data()),
		  reinterpret_cast<std::streamsize>(nbytes))) {
      const auto bytes_read = file.gcount();
      INFO << "Point B: read bytes " << bytes_read << dendl;
      // buffer->set_size(bytes_read);
      position = file.tellg();
      return buffer;
    } else if (file.rdstate() & std::ifstream::failbit &&
	       file.rdstate() & std::ifstream::eofbit) {
      const auto bytes_read = file.gcount();
      INFO << "3 read bytes " << bytes_read << " and reached EOF" << dendl;
      // buffer->set_size(bytes_read);
      position = file.tellg();
      return buffer;
    } else {
      ERROR << "unable to read from file" << dendl;
      return arw::Status::IOError("unable to read from offset %ld", position);
    }
  }

  bool supports_zero_copy() const override {
    return false;
  }

  // implement Seekable

  arw::Result<int64_t> GetSize() override {
    return flight_data.obj_size;
  }

  arw::Result<std::string_view> Peek(int64_t nbytes) override {
    std::iostream::pos_type here = file.tellg();
    if (here == -1) {
      return arw::Status::IOError(
	"unable to determine current position ahead of peek");
    }

    ARROW_ASSIGN_OR_RAISE(OwningStringView result,
			  OwningStringView::make(nbytes));

    // read
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
			  Read(nbytes, (void*) result.writeable_data()));
    (void) bytes_read; // silence unused variable warnings

    // return offset to original
    ARROW_RETURN_NOT_OK(Seek(here));

    return result;
  }

  arw::Result<std::shared_ptr<const arw::KeyValueMetadata>> ReadMetadata() {
    return flight_data.kv_metadata;
  }

  arw::Future<std::shared_ptr<const arw::KeyValueMetadata>> ReadMetadataAsync(
    const arw::io::IOContext& io_context) override {
    return arw::Future<std::shared_ptr<const arw::KeyValueMetadata>>::MakeFinished(ReadMetadata());
  }

  // implement Seekable interface

  arw::Status Seek(int64_t position) {
    file.seekg(position);
    if (file.fail()) {
      return arw::Status::IOError(
	"error encountered during seek to %" PRId64, position);
    } else {
      return arw::Status::OK();
    }
  }
}; // class LocalRandomAccessFile
#endif

class RandomAccessObject : public arw::io::RandomAccessFile {

  FlightData flight_data;
  const DoutPrefix dp;

  int64_t position;
  bool is_closed;
  std::unique_ptr<rgw::sal::Object::ReadOp> op;

public:

  RandomAccessObject(const FlightData& _flight_data,
		     std::unique_ptr<rgw::sal::Object>& obj,
		     const DoutPrefix _dp) :
    flight_data(_flight_data),
    dp(_dp),
    position(-1),
    is_closed(false)
    {
      op = obj->get_read_op();
    }

  arw::Status Open() {
    int ret = op->prepare(null_yield, &dp);
    if (ret < 0) {
      return arw::Status::IOError(
	"unable to prepare object with error %d", ret);
    }
    INFO << "file opened successfully" << dendl;
    position = 0;
    return arw::Status::OK();
  }

  // implement InputStream

  arw::Status Close() override {
    position = -1;
    is_closed = true;
    (void) op.reset();
    INFO << "object closed" << dendl;
    return arw::Status::OK();
  }

  arw::Result<int64_t> Tell() const override {
    if (position < 0) {
      return arw::Status::IOError("could not determine position");
    } else {
      return position;
    }
  }

  bool closed() const override {
    return is_closed;
  }

  arw::Result<int64_t> Read(int64_t nbytes, void* out) override {
    INFO << "entered: asking for " << nbytes << " bytes" << dendl;

    if (position < 0) {
      ERROR << "error, position indicated error" << dendl;
      return arw::Status::IOError("object read op is in bad state");
    }

    // note: read function reads through end_position inclusive
    int64_t end_position = position + nbytes - 1;

    bufferlist bl;

    const int64_t bytes_read =
      op->read(position, end_position, bl, null_yield, &dp);
    if (bytes_read < 0) {
      const int64_t former_position = position;
      position = -1;
      ERROR << "read operation returned " << bytes_read << dendl;
      return arw::Status::IOError(
	"unable to read object at position %" PRId64 ", error code: %" PRId64,
	former_position,
	bytes_read);
    }

    // TODO: see if there's a way to get rid of this copy, perhaps
    // updating rgw::sal::read_op
    bl.cbegin().copy(bytes_read, reinterpret_cast<char*>(out));

    position += bytes_read;

    if (nbytes != bytes_read) {
      INFO << "partial read: nbytes=" << nbytes <<
	", bytes_read=" << bytes_read << dendl;
    }
    INFO << bytes_read << " bytes read" << dendl;
    return bytes_read;
  }

  arw::Result<std::shared_ptr<arw::Buffer>> Read(int64_t nbytes) override {
    INFO << "entered: asking for " << nbytes << " bytes" << dendl;

    std::shared_ptr<OwnedBuffer> buffer;
    ARROW_ASSIGN_OR_RAISE(buffer, OwnedBuffer::make(nbytes));

    ARROW_ASSIGN_OR_RAISE(const int64_t bytes_read,
			  Read(nbytes, buffer->writeable_data()));
    buffer->set_size(bytes_read);

    return buffer;
  }

  bool supports_zero_copy() const override {
    return false;
  }

  // implement Seekable

  arw::Result<int64_t> GetSize() override {
    INFO << "entered: " << flight_data.obj_size << " returned" << dendl;
    return flight_data.obj_size;
  }

  arw::Result<std::string_view> Peek(int64_t nbytes) override {
    INFO << "entered: " << nbytes << " bytes" << dendl;

    int64_t saved_position = position;

    ARROW_ASSIGN_OR_RAISE(OwningStringView buffer,
			  OwningStringView::make(nbytes));

    ARROW_ASSIGN_OR_RAISE(const int64_t bytes_read,
			  Read(nbytes, (void*) buffer.writeable_data()));

    // restore position for a peek
    position = saved_position;

    if (bytes_read < nbytes) {
      // create new OwningStringView with moved buffer
      return OwningStringView::shrink(std::move(buffer), bytes_read);
    } else {
      return buffer;
    }
  }

  arw::Result<std::shared_ptr<const arw::KeyValueMetadata>> ReadMetadata() {
    return flight_data.kv_metadata;
  }

  arw::Future<std::shared_ptr<const arw::KeyValueMetadata>> ReadMetadataAsync(
    const arw::io::IOContext& io_context) override {
    return arw::Future<std::shared_ptr<const arw::KeyValueMetadata>>::MakeFinished(ReadMetadata());
  }

  // implement Seekable interface

  arw::Status Seek(int64_t new_position) {
    INFO << "entered: position: " << new_position << dendl;
    if (position < 0) {
      ERROR << "error, position indicated error" << dendl;
      return arw::Status::IOError("object read op is in bad state");
    } else {
      position = new_position;
      return arw::Status::OK();
    }
  }
}; // class RandomAccessObject

arw::Status FlightServer::DoGet(const flt::ServerCallContext &context,
				const flt::Ticket &request,
				std::unique_ptr<flt::FlightDataStream> *stream) {
  int ret;

  ARROW_ASSIGN_OR_RAISE(FlightKey key, TicketToFlightKey(request));
  ARROW_ASSIGN_OR_RAISE(FlightData fd, get_flight_store()->get_flight(key));

#if 0
  /* load_bucket no longer requires a user parameter. Keep this code
   * around a bit longer until we fully figure out how permissions
   * will impact this code.
   */
  std::unique_ptr<rgw::sal::User> user = driver->get_user(fd.user_id);
  if (user->empty()) {
    INFO << "user is empty" << dendl;
  } else {
    // TODO: test what happens if user is not loaded
    ret = user->load_user(&dp, null_yield);
    if (ret < 0) {
      ERROR << "load_user returned " << ret << dendl;
      // TODO return something
    }
    INFO << "user is " << user->get_display_name() << dendl;
  }
#endif

  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = driver->load_bucket(&dp,
			    rgw_bucket(fd.tenant_name, fd.bucket_name),
                            &bucket, null_yield);
  if (ret < 0) {
    ERROR << "get_bucket returned " << ret << dendl;
    // TODO return something
  }

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(fd.object_key);

  auto input = std::make_shared<RandomAccessObject>(fd, object, dp);
  ARROW_RETURN_NOT_OK(input->Open());

  std::unique_ptr<parquet::arrow::FileReader> reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input,
					       arw::default_memory_pool(),
					       &reader));

  std::shared_ptr<arrow::Table> table;
  ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

  std::vector<std::shared_ptr<arw::RecordBatch>> batches;
  arw::TableBatchReader batch_reader(*table);
  while (true) {
    std::shared_ptr<arw::RecordBatch> p;
    auto s = batch_reader.ReadNext(&p);
    if (!s.ok()) {
      break;
    }
    batches.push_back(p);
  }

  ARROW_ASSIGN_OR_RAISE(auto owning_reader,
			arw::RecordBatchReader::Make(
			  std::move(batches), table->schema()));
  *stream = std::unique_ptr<flt::FlightDataStream>(
    new flt::RecordBatchStream(owning_reader));

  return arw::Status::OK();
} // flightServer::DoGet

} // namespace rgw::flight
