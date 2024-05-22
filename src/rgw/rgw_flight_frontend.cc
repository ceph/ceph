// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright 2023 IBM
 *
 * See file COPYING for licensing information.
 */

#include <cstdio>
#include <filesystem>
#include <sstream>

#include "arrow/type.h"
#include "arrow/flight/server.h"
#include "arrow/io/file.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/stream_reader.h"

#include "rgw_flight_frontend.h"
#include "rgw_flight.h"


// logging
constexpr unsigned dout_subsys = ceph_subsys_rgw_flight;
constexpr const char* dout_prefix_str = "rgw arrow_flight: ";


namespace rgw::flight {

const FlightKey null_flight_key = 0;

FlightFrontend::FlightFrontend(RGWProcessEnv& _env,
			       RGWFrontendConfig* _config,
			       int _port) :
  env(_env),
  config(_config),
  port(_port),
  dp(env.driver->ctx(), dout_subsys, dout_prefix_str)
{
  env.flight_store = new MemoryFlightStore(dp);
  env.flight_server = new FlightServer(env, env.flight_store, dp);
  INFO << "flight server started" << dendl;
}

FlightFrontend::~FlightFrontend() {
  delete env.flight_server;
  env.flight_server = nullptr;

  delete env.flight_store;
  env.flight_store = nullptr;

  INFO << "flight server shut down" << dendl;
}

int FlightFrontend::init() {
  if (port <= 0) {
    port = FlightServer::default_port;
  }
  const std::string url =
    std::string("grpc+tcp://localhost:") + std::to_string(port);
  auto r = flt::Location::Parse(url);
  if (!r.ok()) {
    ERROR << "could not parse server uri: " << url << dendl;
    return -EINVAL;
  }
  flt::Location location = *r;

  flt::FlightServerOptions options(location);
  options.verify_client = false;
  auto s = env.flight_server->Init(options);
  if (!s.ok()) {
    ERROR << "couldn't init flight server; status=" << s << dendl;
    return -EINVAL;
  }

  INFO << "FlightServer inited; will use port " << port << dendl;
  return 0;
}

int FlightFrontend::run() {
  try {
    flight_thread = make_named_thread(server_thread_name,
				      &FlightServer::ServeAlt,
				      env.flight_server);

    INFO << "FlightServer thread started, id=" <<
      flight_thread.get_id() <<
      ", joinable=" << flight_thread.joinable() << dendl;
    return 0;
  } catch (std::system_error& e) {
    ERROR << "FlightServer thread failed to start" << dendl;
    return -e.code().value();
  }
}

void FlightFrontend::stop() {
  arw::Status s;
  s = env.flight_server->Shutdown();
  if (!s.ok()) {
    ERROR << "call to Shutdown failed; status=" << s << dendl;
    return;
  }

  s = env.flight_server->Wait();
  if (!s.ok()) {
    ERROR << "call to Wait failed; status=" << s << dendl;
    return;
  }

  INFO << "FlightServer shut down" << dendl;
}

void FlightFrontend::join() {
  flight_thread.join();
  INFO << "FlightServer thread joined" << dendl;
}

void FlightFrontend::pause_for_new_config() {
  // ignore since config changes won't alter flight_server
}

void FlightFrontend::unpause_with_new_config() {
  // ignore since config changes won't alter flight_server
}

/* ************************************************************ */

FlightGetObj_Filter::FlightGetObj_Filter(const req_state* request,
					 RGWGetObj_Filter* next) :
  RGWGetObj_Filter(next),
  penv(request->penv),
  dp(request->cct->get(), dout_subsys, dout_prefix_str),
  current_offset(0),
  expected_size(request->obj_size),
  uri(request->decoded_uri),
  tenant_name(request->bucket->get_tenant()),
  bucket_name(request->bucket->get_name()),
  object_key(request->object->get_key()),
  // note: what about object namespace and instance?
  schema_status(arrow::StatusCode::Cancelled,
		"schema determination incomplete"),
  user_id(request->user->get_id())
{
#warning "TODO: fix use of tmpnam"
  char name[L_tmpnam];
  const char* namep = std::tmpnam(name);
  if (!namep) {
    //
  }
  temp_file_name = namep;

  temp_file.open(temp_file_name);
}

FlightGetObj_Filter::~FlightGetObj_Filter() {
  if (temp_file.is_open()) {
    temp_file.close();
  }
  std::error_code error;
  std::filesystem::remove(temp_file_name, error);
  if (error) {
    ERROR << "FlightGetObj_Filter got error when removing temp file; "
      "error=" << error.value() <<
      ", temp_file_name=" << temp_file_name << dendl;
  } else {
    INFO << "parquet/arrow schema determination status: " <<
      schema_status << dendl;
  }
}

int FlightGetObj_Filter::handle_data(bufferlist& bl,
				     off_t bl_ofs, off_t bl_len) {
  INFO << "flight handling data from offset " <<
    current_offset << " (" << bl_ofs << ") of size " << bl_len << dendl;

  current_offset += bl_len;

  if (temp_file.is_open()) {
    bl.write_stream(temp_file);

    if (current_offset >= expected_size) {
      INFO << "data read is completed, current_offset=" <<
	current_offset << ", expected_size=" << expected_size << dendl;
      temp_file.close();

      std::shared_ptr<const arw::KeyValueMetadata> kv_metadata;
      std::shared_ptr<arw::Schema> aw_schema;
      int64_t num_rows = 0;

      auto process_metadata = [&aw_schema, &num_rows, &kv_metadata, this]() -> arrow::Status {
	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::ReadableFile> file,
			      arrow::io::ReadableFile::Open(temp_file_name));
	const std::shared_ptr<parquet::FileMetaData> metadata = parquet::ReadMetaData(file);

	ARROW_RETURN_NOT_OK(file->Close());

	num_rows = metadata->num_rows();
	kv_metadata = metadata->key_value_metadata();
	const parquet::SchemaDescriptor* pq_schema = metadata->schema();
	ARROW_RETURN_NOT_OK(parquet::arrow::FromParquetSchema(pq_schema, &aw_schema));

	return arrow::Status::OK();
      };

      schema_status = process_metadata();
      if (!schema_status.ok()) {
	ERROR << "reading metadata to access schema, error=" << schema_status << dendl;
      } else {
	// INFO << "arrow_schema=" << *aw_schema << dendl;
	FlightStore* store = penv.flight_store;
	auto key =
	  store->add_flight(FlightData(uri, tenant_name, bucket_name,
				       object_key, num_rows,
				       expected_size, aw_schema,
				       kv_metadata, user_id));
	(void) key; // suppress unused variable warning
      }
    } // if last block
  } // if file opened

    // chain to next filter in stream
  int ret = RGWGetObj_Filter::handle_data(bl, bl_ofs, bl_len);

  return ret;
}

#if 0
void code_snippets() {
  INFO << "num_columns:" << md->num_columns() <<
    " num_schema_elements:" << md->num_schema_elements() <<
    " num_rows:" << md->num_rows() <<
    " num_row_groups:" << md->num_row_groups() << dendl;


  INFO << "file schema: name=" << schema1->name() << ", ToString:" << schema1->ToString() << ", num_columns=" << schema1->num_columns() << dendl;
  for (int c = 0; c < schema1->num_columns(); ++c) {
    const parquet::ColumnDescriptor* cd = schema1->Column(c);
    // const parquet::ConvertedType::type t = cd->converted_type;
    const std::shared_ptr<const parquet::LogicalType> lt = cd->logical_type();
    INFO << "column " << c << ": name=" << cd->name() << ", ToString=" << cd->ToString() << ", logical_type=" << lt->ToString() << dendl;
  }

  INFO << "There are " << md->num_rows() << " rows and " << md->num_row_groups() << " row groups" << dendl;
  for (int rg = 0; rg < md->num_row_groups(); ++rg) {
    INFO << "Row Group " << rg << dendl;
    auto rg_md = md->RowGroup(rg);
    auto schema2 = rg_md->schema();
  }
}
#endif

} // namespace rgw::flight
