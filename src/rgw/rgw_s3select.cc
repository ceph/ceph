// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_s3select_private.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::s3select {
RGWOp* create_s3select_op()
{
  return new RGWSelectObj_ObjStore_S3();
}
};

using namespace s3selectEngine;

std::string& aws_response_handler::get_sql_result()
{
  return sql_result;
}

uint64_t aws_response_handler::get_processed_size()
{
  return processed_size;
}

void aws_response_handler::update_processed_size(uint64_t value)
{
  processed_size += value;
  s->s3select_usage.bytes_processed = processed_size;
}

uint64_t aws_response_handler::get_total_bytes_returned()
{
  return total_bytes_returned;
}

void aws_response_handler::update_total_bytes_returned(uint64_t value)
{
  total_bytes_returned = value;
  s->s3select_usage.bytes_returned = total_bytes_returned;
}

void aws_response_handler::push_header(const char* header_name, const char* header_value)
{
  char x;
  short s;
  x = char(strlen(header_name));
  m_buff_header.append(&x, sizeof(x));
  m_buff_header.append(header_name);
  x = char(7);
  m_buff_header.append(&x, sizeof(x));
  s = htons(uint16_t(strlen(header_value)));
  m_buff_header.append(reinterpret_cast<char*>(&s), sizeof(s));
  m_buff_header.append(header_value);
}

#define IDX( x ) static_cast<int>( x )

int aws_response_handler::create_header_records()
{
  //headers description(AWS)
  //[header-name-byte-length:1][header-name:variable-length][header-value-type:1][header-value:variable-length]
  //1
  push_header(header_name_str[IDX(header_name_En::EVENT_TYPE)], header_value_str[IDX(header_value_En::RECORDS)]);
  //2
  push_header(header_name_str[IDX(header_name_En::CONTENT_TYPE)], header_value_str[IDX(header_value_En::OCTET_STREAM)]);
  //3
  push_header(header_name_str[IDX(header_name_En::MESSAGE_TYPE)], header_value_str[IDX(header_value_En::EVENT)]);
  return m_buff_header.size();
}

int aws_response_handler::create_header_continuation()
{
  //headers description(AWS)
  //1
  push_header(header_name_str[IDX(header_name_En::EVENT_TYPE)], header_value_str[IDX(header_value_En::CONT)]);
  //2
  push_header(header_name_str[IDX(header_name_En::MESSAGE_TYPE)], header_value_str[IDX(header_value_En::EVENT)]);
  return m_buff_header.size();
}

int aws_response_handler::create_header_progress()
{
  //headers description(AWS)
  //1
  push_header(header_name_str[IDX(header_name_En::EVENT_TYPE)], header_value_str[IDX(header_value_En::PROGRESS)]);
  //2
  push_header(header_name_str[IDX(header_name_En::CONTENT_TYPE)], header_value_str[IDX(header_value_En::XML)]);
  //3
  push_header(header_name_str[IDX(header_name_En::MESSAGE_TYPE)], header_value_str[IDX(header_value_En::EVENT)]);
  return m_buff_header.size();
}

int aws_response_handler::create_header_stats()
{
  //headers description(AWS)
  //1
  push_header(header_name_str[IDX(header_name_En::EVENT_TYPE)], header_value_str[IDX(header_value_En::STATS)]);
  //2
  push_header(header_name_str[IDX(header_name_En::CONTENT_TYPE)], header_value_str[IDX(header_value_En::XML)]);
  //3
  push_header(header_name_str[IDX(header_name_En::MESSAGE_TYPE)], header_value_str[IDX(header_value_En::EVENT)]);
  return m_buff_header.size();
}

int aws_response_handler::create_header_end()
{
  //headers description(AWS)
  //1
  push_header(header_name_str[IDX(header_name_En::EVENT_TYPE)], header_value_str[IDX(header_value_En::END)]);
  //2
  push_header(header_name_str[IDX(header_name_En::MESSAGE_TYPE)], header_value_str[IDX(header_value_En::EVENT)]);
  return m_buff_header.size();
}

int aws_response_handler::create_error_header_records(const char* error_message)
{
  //headers description(AWS)
  //[header-name-byte-length:1][header-name:variable-length][header-value-type:1][header-value:variable-length]
  //1
  push_header(header_name_str[IDX(header_name_En::ERROR_CODE)], header_value_str[IDX(header_value_En::ENGINE_ERROR)]);
  //2
  push_header(header_name_str[IDX(header_name_En::ERROR_MESSAGE)], error_message);
  //3
  push_header(header_name_str[IDX(header_name_En::MESSAGE_TYPE)], header_value_str[IDX(header_value_En::ERROR_TYPE)]);
  return m_buff_header.size();
}

int aws_response_handler::create_message(u_int32_t header_len)
{
  //message description(AWS):
  //[total-byte-length:4][header-byte-length:4][crc:4][headers:variable-length][payload:variable-length][crc:4]
  //s3select result is produced into sql_result, the sql_result is also the response-message, thus the attach headers and CRC
  //are created later to the produced SQL result, and actually wrapping the payload.
  auto push_encode_int = [&](u_int32_t s, int pos) {
    u_int32_t x = htonl(s);
    sql_result.replace(pos, sizeof(x), reinterpret_cast<char*>(&x), sizeof(x));
  };
  u_int32_t total_byte_len = 0;
  u_int32_t preload_crc = 0;
  u_int32_t message_crc = 0;
  total_byte_len = sql_result.size() + 4; //the total is greater in 4 bytes than current size
  push_encode_int(total_byte_len, 0);
  push_encode_int(header_len, 4);
  crc32.reset();
  crc32 = std::for_each(sql_result.data(), sql_result.data() + 8, crc32); //crc for starting 8 bytes
  preload_crc = crc32();
  push_encode_int(preload_crc, 8);
  crc32.reset();
  crc32 = std::for_each(sql_result.begin(), sql_result.end(), crc32); //crc for payload + checksum
  message_crc = crc32();
  u_int32_t x = htonl(message_crc);
  sql_result.append(reinterpret_cast<char*>(&x), sizeof(x));
  return sql_result.size();
}

void aws_response_handler::init_response()
{
  //12 positions for header-crc
  sql_result.resize(header_crc_size, '\0');
}

void aws_response_handler::init_success_response()
{
  m_buff_header.clear();
  header_size = create_header_records();
  sql_result.append(m_buff_header.c_str(), header_size);
#ifdef PAYLOAD_TAG
  sql_result.append(PAYLOAD_LINE);
#endif
}

void aws_response_handler::send_continuation_response()
{
  sql_result.resize(header_crc_size, '\0');
  m_buff_header.clear();
  header_size = create_header_continuation();
  sql_result.append(m_buff_header.c_str(), header_size);
  int buff_len = create_message(header_size);
  s->formatter->write_bin_data(sql_result.data(), buff_len);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void aws_response_handler::init_progress_response()
{
  sql_result.resize(header_crc_size, '\0');
  m_buff_header.clear();
  header_size = create_header_progress();
  sql_result.append(m_buff_header.c_str(), header_size);
}

void aws_response_handler::init_stats_response()
{
  sql_result.resize(header_crc_size, '\0');
  m_buff_header.clear();
  header_size = create_header_stats();
  sql_result.append(m_buff_header.c_str(), header_size);
}

void aws_response_handler::init_end_response()
{
  sql_result.resize(header_crc_size, '\0');
  m_buff_header.clear();
  header_size = create_header_end();
  sql_result.append(m_buff_header.c_str(), header_size);
  int buff_len = create_message(header_size);
  s->formatter->write_bin_data(sql_result.data(), buff_len);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void aws_response_handler::init_error_response(const char* error_message)
{
  //currently not in use. the headers in the case of error, are not extracted by AWS-cli.
  m_buff_header.clear();
  header_size = create_error_header_records(error_message);
  sql_result.append(m_buff_header.c_str(), header_size);
}

void aws_response_handler::send_success_response()
{
#ifdef PAYLOAD_TAG
  sql_result.append(END_PAYLOAD_LINE);
#endif
  int buff_len = create_message(header_size);
  s->formatter->write_bin_data(sql_result.data(), buff_len);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void aws_response_handler::send_error_response(const char* error_code,
    const char* error_message,
    const char* resource_id)
{
  set_req_state_err(s, 0);
  dump_errno(s, 400);
  end_header(s, m_rgwop, "application/xml", CHUNKED_TRANSFER_ENCODING);
  dump_start(s);
  s->formatter->open_object_section("Error");
  s->formatter->dump_string("Code", error_code);
  s->formatter->dump_string("Message", error_message);
  s->formatter->dump_string("Resource", "#Resource#");
  s->formatter->dump_string("RequestId", resource_id);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void aws_response_handler::send_progress_response()
{
  std::string progress_payload = fmt::format("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Progress><BytesScanned>{}</BytesScanned><BytesProcessed>{}</BytesProcessed><BytesReturned>{}</BytesReturned></Progress>"
                                 , get_processed_size(), get_processed_size(), get_total_bytes_returned());
  sql_result.append(progress_payload);
  int buff_len = create_message(header_size);
  s->formatter->write_bin_data(sql_result.data(), buff_len);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void aws_response_handler::send_stats_response()
{
  std::string stats_payload = fmt::format("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Stats><BytesScanned>{}</BytesScanned><BytesProcessed>{}</BytesProcessed><BytesReturned>{}</BytesReturned></Stats>"
                                          , get_processed_size(), get_processed_size(), get_total_bytes_returned());
  sql_result.append(stats_payload);
  int buff_len = create_message(header_size);
  s->formatter->write_bin_data(sql_result.data(), buff_len);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

RGWSelectObj_ObjStore_S3::RGWSelectObj_ObjStore_S3():
  m_buff_header(std::make_unique<char[]>(1000)),
  m_scan_range_ind(false),
  m_start_scan_sz(0),
  m_end_scan_sz(0),
  m_object_size_for_processing(0),
  m_parquet_type(false),
  m_json_type(false),
  chunk_number(0),
  m_requested_range(0),
  m_scan_offset(1024),
  m_skip_next_chunk(false),
  m_is_trino_request(false)
{
  set_get_data(true);
  fp_get_obj_size = [&]() {
    return get_obj_size();
  };
  fp_range_req = [&](int64_t start, int64_t len, void* buff, optional_yield* y) {
    ldout(s->cct, 10) << "S3select: range-request start: " << start << " length: " << len << dendl;
    auto status = range_request(start, len, buff, *y);
    return status;
  };
#ifdef _ARROW_EXIST
  m_rgw_api.set_get_size_api(fp_get_obj_size);
  m_rgw_api.set_range_req_api(fp_range_req);
#endif
  fp_result_header_format = [this](std::string& result) {
    m_aws_response_handler.init_response();
    m_aws_response_handler.init_success_response();
    return 0;
  };
  fp_s3select_result_format = [this](std::string& result) {
    fp_chunked_transfer_encoding();
    m_aws_response_handler.send_success_response();
    return 0;
  };

  fp_debug_mesg = [&](const char* mesg){
    ldpp_dout(this, 10) << mesg << dendl;
  };

  fp_chunked_transfer_encoding = [&](void){
    if (chunk_number == 0) { 
      if (op_ret < 0) { 
        set_req_state_err(s, op_ret); 
      } 
      dump_errno(s); 
      // Explicitly use chunked transfer encoding so that we can stream the result
      // to the user without having to wait for the full length of it.
      end_header(s, this, "application/xml", CHUNKED_TRANSFER_ENCODING); 
    } 
    chunk_number++; 
  };
}

RGWSelectObj_ObjStore_S3::~RGWSelectObj_ObjStore_S3()
{}

int RGWSelectObj_ObjStore_S3::get_params(optional_yield y)
{
  if(m_s3select_query.empty() == false) {
    return 0;
  }
#ifndef _ARROW_EXIST
    m_parquet_type = false;
    ldpp_dout(this, 10) << "arrow library is not installed" << dendl;
#endif
  
  //retrieve s3-select query from payload
  bufferlist data;
  int ret;
  int max_size = 4096;
  std::tie(ret, data) = read_all_input(s, max_size, false);
  if (ret != 0) {
    ldpp_dout(this, 10) << "s3-select query: failed to retrieve query; ret = " << ret << dendl;
    return ret;
  }
  m_s3select_query = data.to_str();
  if (m_s3select_query.length() > 0) {
    ldpp_dout(this, 10) << "s3-select query: " << m_s3select_query << dendl;
  } else {
    ldpp_dout(this, 10) << "s3-select query: failed to retrieve query;" << dendl;
    return -1;
  }
  const auto& m = s->info.env->get_map();
  auto user_agent = m.find("HTTP_USER_AGENT"); {
  if (user_agent != m.end()){
    if (user_agent->second.find("Trino") != std::string::npos){
	m_is_trino_request = true;
	ldpp_dout(this, 10) << "s3-select query: request sent by Trino." << dendl;
      }
    }
  }

  int status = handle_aws_cli_parameters(m_sql_query);
  if (status<0) {
    return status;
  }
  return RGWGetObj_ObjStore_S3::get_params(y);
}

int RGWSelectObj_ObjStore_S3::run_s3select_on_csv(const char* query, const char* input, size_t input_length)
{
  int status = 0;
  uint32_t length_before_processing, length_post_processing;
  csv_object::csv_defintions csv;
  const char* s3select_syntax_error = "s3select-Syntax-Error";
  const char* s3select_resource_id = "resource-id";
  const char* s3select_processTime_error = "s3select-ProcessingTime-Error";

  s3select_syntax.parse_query(query);
  if (m_row_delimiter.size()) {
    csv.row_delimiter = *m_row_delimiter.c_str();
  }
  if (m_column_delimiter.size()) {
    csv.column_delimiter = *m_column_delimiter.c_str();
  }
  if (m_quot.size()) {
    csv.quot_char = *m_quot.c_str();
  }
  if (m_escape_char.size()) {
    csv.escape_char = *m_escape_char.c_str();
  }
  if (output_row_delimiter.size()) {
    csv.output_row_delimiter = *output_row_delimiter.c_str();
  }
  if (output_column_delimiter.size()) {
    csv.output_column_delimiter = *output_column_delimiter.c_str();
  }
  if (output_quot.size()) {
    csv.output_quot_char = *output_quot.c_str();
  }
  if (output_escape_char.size()) {
    csv.output_escape_char = *output_escape_char.c_str();
  }
  if(output_quote_fields.compare("ALWAYS") == 0) {
    csv.quote_fields_always = true;
  } else if(output_quote_fields.compare("ASNEEDED") == 0) {
    csv.quote_fields_asneeded = true;
  }
  if(m_header_info.compare("IGNORE")==0) {
    csv.ignore_header_info=true;
  } else if(m_header_info.compare("USE")==0) {
    csv.use_header_info=true;
  }
  //m_s3_csv_object.set_external_debug_system(fp_debug_mesg);
  m_s3_csv_object.set_result_formatters(fp_s3select_result_format,fp_result_header_format);
  m_s3_csv_object.set_csv_query(&s3select_syntax, csv);
  if (s3select_syntax.get_error_description().empty() == false) {
    //error-flow (syntax-error)
    m_aws_response_handler.send_error_response(s3select_syntax_error,
        s3select_syntax.get_error_description().c_str(),
        s3select_resource_id);
    ldpp_dout(this, 10) << "s3-select query: failed to prase the following query {" << query << "}" << dendl;
    ldpp_dout(this, 10) << "s3-select query: syntax-error {" << s3select_syntax.get_error_description() << "}" << dendl;
    return -1;
  } else {
    if (input == nullptr) {
      input = "";
    }
    fp_result_header_format(m_aws_response_handler.get_sql_result());
    length_before_processing = m_s3_csv_object.get_return_result_size();
    //query is correct(syntax), processing is starting.
    status = m_s3_csv_object.run_s3select_on_stream(m_aws_response_handler.get_sql_result(), input, input_length, m_object_size_for_processing);
    length_post_processing = m_s3_csv_object.get_return_result_size();
    m_aws_response_handler.update_total_bytes_returned( m_s3_csv_object.get_return_result_size() );

    if (status < 0) {
      //error flow(processing-time)
      m_aws_response_handler.send_error_response(s3select_processTime_error,
          m_s3_csv_object.get_error_description().c_str(),
          s3select_resource_id);
      ldpp_dout(this, 10) << "s3-select query: failed to process query; {" << m_s3_csv_object.get_error_description() << "}" << dendl;
      return -1;
    }

  }
  if ((length_post_processing-length_before_processing) != 0) {
    ldpp_dout(this, 10) << "s3-select: sql-result-size = " << m_aws_response_handler.get_sql_result().size() << dendl;
  } else {
    m_aws_response_handler.send_continuation_response();
  }
  ldpp_dout(this, 10) << "s3-select: complete chunk processing : chunk length = " << input_length << dendl;
  if (enable_progress == true) {
    fp_chunked_transfer_encoding();
    m_aws_response_handler.init_progress_response();
    m_aws_response_handler.send_progress_response();
  }
  return status;
}

int RGWSelectObj_ObjStore_S3::run_s3select_on_parquet(const char* query)
{
  int status = 0;
#ifdef _ARROW_EXIST
  if (!m_s3_parquet_object.is_set()) {
    //parsing the SQL statement.
    s3select_syntax.parse_query(m_sql_query.c_str());
    //m_s3_parquet_object.set_external_debug_system(fp_debug_mesg);
    try {
      //at this stage the Parquet-processing requires for the meta-data that reside on Parquet object 
      m_s3_parquet_object.set_parquet_object(std::string("s3object"), &s3select_syntax, &m_rgw_api);
    } catch(base_s3select_exception& e) {
      ldpp_dout(this, 10) << "S3select: failed upon parquet-reader construction: " << e.what() << dendl;
      fp_result_header_format(m_aws_response_handler.get_sql_result());
      m_aws_response_handler.get_sql_result().append(e.what());
      fp_s3select_result_format(m_aws_response_handler.get_sql_result());
      return -1;
    }
  }
  if (s3select_syntax.get_error_description().empty() == false) {
    //the SQL statement failed the syntax parser
    fp_result_header_format(m_aws_response_handler.get_sql_result());
    m_aws_response_handler.get_sql_result().append(s3select_syntax.get_error_description().data());
    fp_s3select_result_format(m_aws_response_handler.get_sql_result());
    ldpp_dout(this, 10) << "s3-select query: failed to prase query; {" << s3select_syntax.get_error_description() << "}" << dendl;
    status = -1;
  } else {
    fp_result_header_format(m_aws_response_handler.get_sql_result());
    //at this stage the Parquet-processing "takes control", it keep calling to s3-range-request according to the SQL statement.
    status = m_s3_parquet_object.run_s3select_on_object(m_aws_response_handler.get_sql_result(), fp_s3select_result_format, fp_result_header_format);
    if (status < 0) {
      m_aws_response_handler.get_sql_result().append(m_s3_parquet_object.get_error_description());
      fp_s3select_result_format(m_aws_response_handler.get_sql_result());
      ldout(s->cct, 10) << "S3select: failure while execution" << m_s3_parquet_object.get_error_description() << dendl;
    }
  }
#endif
  return status;
}

int RGWSelectObj_ObjStore_S3::run_s3select_on_json(const char* query, const char* input, size_t input_length)
{
  int status = 0;
  
  const char* s3select_processTime_error = "s3select-ProcessingTime-Error";
  const char* s3select_syntax_error = "s3select-Syntax-Error";
  const char* s3select_resource_id = "resourcse-id";
  const char* s3select_json_error = "json-Format-Error";

  m_aws_response_handler.init_response();

  //the JSON data-type should be(currently) only DOCUMENT
  if (m_json_datatype.compare("DOCUMENT") != 0) {
    const char* s3select_json_error_msg = "s3-select query: wrong json dataType should use DOCUMENT; ";
    m_aws_response_handler.send_error_response(s3select_json_error,
      s3select_json_error_msg,
      s3select_resource_id);
    ldpp_dout(this, 10) << s3select_json_error_msg << dendl;
    return -EINVAL;
  } 

  //parsing the SQL statement
  s3select_syntax.parse_query(m_sql_query.c_str());
  if (s3select_syntax.get_error_description().empty() == false) {
  //SQL statement is wrong(syntax).
    m_aws_response_handler.send_error_response(s3select_syntax_error,
      s3select_syntax.get_error_description().c_str(),
      s3select_resource_id);
    ldpp_dout(this, 10) << "s3-select query: failed to prase query; {" << s3select_syntax.get_error_description() << "}" << dendl;
    return -EINVAL;
  }
    
  //initializing json processor
  json_object::csv_definitions output_definition;
  m_s3_json_object.set_json_query(&s3select_syntax,output_definition);

  if (input == nullptr) {
    input = "";
  }
  m_aws_response_handler.init_success_response();
  uint32_t length_before_processing = m_aws_response_handler.get_sql_result().size();
  //query is correct(syntax), processing is starting.
  try {
    status = m_s3_json_object.run_s3select_on_stream(m_aws_response_handler.get_sql_result(), input, input_length, m_object_size_for_processing);
  } catch(base_s3select_exception& e) {
    ldpp_dout(this, 10) << "S3select: failed to process JSON object: " << e.what() << dendl;
    m_aws_response_handler.get_sql_result().append(e.what());
    m_aws_response_handler.send_error_response(s3select_processTime_error,
	e.what(),
     	s3select_resource_id);
    return -EINVAL;
  }
  uint32_t length_post_processing = m_aws_response_handler.get_sql_result().size();
  m_aws_response_handler.update_total_bytes_returned(length_post_processing - length_before_processing);
  if (status < 0) {
    //error flow(processing-time)
    m_aws_response_handler.send_error_response(s3select_processTime_error,
	m_s3_json_object.get_error_description().c_str(),
     	s3select_resource_id);
    ldpp_dout(this, 10) << "s3-select query: failed to process query; {" << m_s3_json_object.get_error_description() << "}" << dendl;
    return -EINVAL;
  }
  fp_chunked_transfer_encoding();

  if (length_post_processing-length_before_processing != 0) {
    m_aws_response_handler.send_success_response();
  } else {
    m_aws_response_handler.send_continuation_response();
  }
  if (enable_progress == true) {
    m_aws_response_handler.init_progress_response();
    m_aws_response_handler.send_progress_response();
  }

  return status;
}

int RGWSelectObj_ObjStore_S3::handle_aws_cli_parameters(std::string& sql_query)
{
  std::string input_tag{"InputSerialization"};
  std::string output_tag{"OutputSerialization"};
  if (chunk_number !=0) {
    return 0;
  }
#define GT "&gt;"
#define LT "&lt;"
#define APOS "&apos;"

  if (m_s3select_query.find(GT) != std::string::npos) {
    boost::replace_all(m_s3select_query, GT, ">");
  }
  if (m_s3select_query.find(LT) != std::string::npos) {
    boost::replace_all(m_s3select_query, LT, "<");
  }
  if (m_s3select_query.find(APOS) != std::string::npos) {
    boost::replace_all(m_s3select_query, APOS, "'");
  }
  //AWS cli s3select parameters
  if (m_s3select_query.find(input_tag+"><CSV") != std::string::npos) {
    ldpp_dout(this, 10) << "s3select: engine is set to process CSV objects" << dendl;
  }
  else if (m_s3select_query.find(input_tag+"><JSON") != std::string::npos) {
    m_json_type=true;
    ldpp_dout(this, 10) << "s3select: engine is set to process JSON objects" << dendl;
  } else if (m_s3select_query.find(input_tag+"><Parquet") != std::string::npos) {
    m_parquet_type=true;
    ldpp_dout(this, 10) << "s3select: engine is set to process Parquet objects" << dendl;
  }

  extract_by_tag(m_s3select_query, "Expression", sql_query);
  extract_by_tag(m_s3select_query, "Enabled", m_enable_progress);
  size_t _qi = m_s3select_query.find("<" + input_tag + ">", 0);
  size_t _qe = m_s3select_query.find("</" + input_tag + ">", _qi);
  m_s3select_input = m_s3select_query.substr(_qi + input_tag.size() + 2, _qe - (_qi + input_tag.size() + 2));
  extract_by_tag(m_s3select_input, "FieldDelimiter", m_column_delimiter);
  extract_by_tag(m_s3select_input, "QuoteCharacter", m_quot);
  extract_by_tag(m_s3select_input, "RecordDelimiter", m_row_delimiter);
  extract_by_tag(m_s3select_input, "FileHeaderInfo", m_header_info);
  extract_by_tag(m_s3select_input, "Type", m_json_datatype);
  if (m_row_delimiter.size()==0) {
    m_row_delimiter='\n';
  } else if (m_row_delimiter.compare("&#10;") == 0) {
    //presto change
    m_row_delimiter='\n';
  }
  extract_by_tag(m_s3select_input, "QuoteEscapeCharacter", m_escape_char);
  extract_by_tag(m_s3select_input, "CompressionType", m_compression_type);
  size_t _qo = m_s3select_query.find("<" + output_tag + ">", 0);
  size_t _qs = m_s3select_query.find("</" + output_tag + ">", _qi);
  m_s3select_output = m_s3select_query.substr(_qo + output_tag.size() + 2, _qs - (_qo + output_tag.size() + 2));
  extract_by_tag(m_s3select_output, "FieldDelimiter", output_column_delimiter);
  extract_by_tag(m_s3select_output, "QuoteCharacter", output_quot);
  extract_by_tag(m_s3select_output, "QuoteEscapeCharacter", output_escape_char);
  extract_by_tag(m_s3select_output, "QuoteFields", output_quote_fields);
  extract_by_tag(m_s3select_output, "RecordDelimiter", output_row_delimiter);
  if (output_row_delimiter.size()==0) {
    output_row_delimiter='\n';
  } else if (output_row_delimiter.compare("&#10;") == 0) {
    //presto change
    output_row_delimiter='\n';
  }
  if (m_compression_type.length()>0 && m_compression_type.compare("NONE") != 0) {
    ldpp_dout(this, 10) << "RGW supports currently only NONE option for compression type" << dendl;
    return -1;
  }
  extract_by_tag(m_s3select_query, "Start", m_start_scan);
  extract_by_tag(m_s3select_query, "End", m_end_scan);
  if (m_start_scan.size() || m_end_scan.size()) {
    m_scan_range_ind = true;
    if (m_start_scan.size()) {
      m_start_scan_sz = std::stol(m_start_scan);
    }
    if (m_end_scan.size()) {
      m_end_scan_sz = std::stol(m_end_scan);
    } else {
      m_end_scan_sz = std::numeric_limits<std::int64_t>::max();
    } 
  }
  if (m_enable_progress.compare("true")==0) {
    enable_progress = true;
  } else {
    enable_progress = false;
  }
  return 0;
}

int RGWSelectObj_ObjStore_S3::extract_by_tag(std::string input, std::string tag_name, std::string& result)
{
  result = "";
  size_t _qs = input.find("<" + tag_name + ">", 0);
  size_t qs_input = _qs + tag_name.size() + 2;
  if (_qs == std::string::npos) {
    return -1;
  }
  size_t _qe = input.find("</" + tag_name + ">", qs_input);
  if (_qe == std::string::npos) {
    return -1;
  }
  result = input.substr(qs_input, _qe - qs_input);
  return 0;
}

size_t RGWSelectObj_ObjStore_S3::get_obj_size()
{
  return s->obj_size;
}

int RGWSelectObj_ObjStore_S3::range_request(int64_t ofs, int64_t len, void* buff, optional_yield y)
{
  //purpose: implementation for arrow::ReadAt, this may take several async calls.
  //send_response_date(call_back) accumulate buffer, upon completion control is back to ReadAt.
  range_req_str = "bytes=" + std::to_string(ofs) + "-" + std::to_string(ofs+len-1);
  range_str = range_req_str.c_str();
  range_parsed = false;
  RGWGetObj::parse_range();
  requested_buffer.clear();
  m_request_range = len;
  ldout(s->cct, 10) << "S3select: calling execute(async):" << " request-offset :" << ofs << " request-length :" << len << " buffer size : " << requested_buffer.size() << dendl;
  RGWGetObj::execute(y);
  if (buff) {
    memcpy(buff, requested_buffer.data(), len);
  }
  ldout(s->cct, 10) << "S3select: done waiting, buffer is complete buffer-size:" << requested_buffer.size() << dendl;
  return len;
}

void RGWSelectObj_ObjStore_S3::execute(optional_yield y)
{
  int status = 0;
  char parquet_magic[4];
  static constexpr uint8_t parquet_magic1[4] = {'P', 'A', 'R', '1'};
  static constexpr uint8_t parquet_magicE[4] = {'P', 'A', 'R', 'E'};
  get_params(y);
#ifdef _ARROW_EXIST
  m_rgw_api.m_y = &y;
#endif
  if (m_parquet_type) {
    //parquet processing
    range_request(0, 4, parquet_magic, y);
    if (memcmp(parquet_magic, parquet_magic1, 4) && memcmp(parquet_magic, parquet_magicE, 4)) {
      ldout(s->cct, 10) << s->object->get_name() << " does not contain parquet magic" << dendl;
      op_ret = -ERR_INVALID_REQUEST;
      return;
    }
    s3select_syntax.parse_query(m_sql_query.c_str());
    status = run_s3select_on_parquet(m_sql_query.c_str());
    if (status) {
      ldout(s->cct, 10) << "S3select: failed to process query <" << m_sql_query << "> on object " << s->object->get_name() << dendl;
      op_ret = -ERR_INVALID_REQUEST;
    } else {
      ldout(s->cct, 10) << "S3select: complete query with success " << dendl;
    }
    } else { 
	//CSV or JSON processing
	if (m_scan_range_ind) {

	  m_requested_range = (m_end_scan_sz - m_start_scan_sz);
	    
	  if(m_is_trino_request){
	  // fetch more than requested(m_scan_offset), that additional bytes are scanned for end of row, 
	  // thus the additional length will be processed, and no broken row for Trino.
	  // assumption: row is smaller than m_scan_offset. (a different approach is to request for additional range)
	    range_request(m_start_scan_sz, m_requested_range + m_scan_offset, nullptr, y);
	  } else {
	    range_request(m_start_scan_sz, m_requested_range, nullptr, y);
	  }

	} else {
	  RGWGetObj::execute(y);
	}
  }//if (m_parquet_type)
}

int RGWSelectObj_ObjStore_S3::parquet_processing(bufferlist& bl, off_t ofs, off_t len)
{
    fp_chunked_transfer_encoding();
    size_t append_in_callback = 0;
    int part_no = 1;
    //concat the requested buffer
    for (auto& it : bl.buffers()) {
      if (it.length() == 0) {
        ldout(s->cct, 10) << "S3select: get zero-buffer while appending request-buffer " << dendl;
      }
      append_in_callback += it.length();
      ldout(s->cct, 10) << "S3select: part " << part_no++ << " it.length() = " << it.length() << dendl;
      requested_buffer.append(&(it)[0]+ofs, len);
    }
    ldout(s->cct, 10) << "S3select:append_in_callback = " << append_in_callback << dendl;
    if (requested_buffer.size() < m_request_range) {
      ldout(s->cct, 10) << "S3select: need another round buffe-size: " << requested_buffer.size() << " request range length:" << m_request_range << dendl;
      return 0;
    } else {//buffer is complete
      ldout(s->cct, 10) << "S3select: buffer is complete " << requested_buffer.size() << " request range length:" << m_request_range << dendl;
      m_request_range = 0;
    }
    return 0;
}

void RGWSelectObj_ObjStore_S3::shape_chunk_per_trino_requests(const char* it_cp, off_t& ofs, off_t& len)
{
//in case it is a scan range request and sent by Trino client.
//this routine chops the start/end of chunks.
//the purpose is to return "perfect" results, with no broken or missing lines.

  off_t new_offset = 0;
  if(m_scan_range_ind){//only upon range-scan
  int64_t sc=0;
  int64_t start =0;
  const char* row_delimiter = m_row_delimiter.c_str();
   
    ldpp_dout(this, 10) << "s3select query: per Trino request the first and last chunk should modified." << dendl;

    //chop the head of the first chunk and only upon the slice does not include the head of the object.
    if(m_start_scan_sz && (m_aws_response_handler.get_processed_size()==0)){
      char* p = const_cast<char*>(it_cp+ofs);
      while(strncmp(row_delimiter,p,1) && (p - (it_cp+ofs)) < len)p++;
      if(!strncmp(row_delimiter,p,1)){
	new_offset += (p - (it_cp+ofs))+1;
      } 
    }

    //RR : end of the range-request. the original request sent by Trino client
    //RD : row-delimiter
    //[ ... ] : chunk boundaries 

    //chop the end of the last chunk for this request
    //if it's the last chunk, search for first row-delimiter for the following different use-cases
    if((m_aws_response_handler.get_processed_size()+len) >= m_requested_range){ 
    //had pass the requested range, start to search for first delimiter
      if(m_aws_response_handler.get_processed_size()>m_requested_range){
	//the previous chunk contain the complete request(all data) and an extra bytes.
	//thus, search for the first row-delimiter
	//[:previous (RR) ... ][:current (RD) ]
	start = 0;
      } else if(m_aws_response_handler.get_processed_size()){
	//the *current* chunk contain the complete request in the middle of the chunk. 
	//thus, search for the first row-delimiter after the complete request position
	//[:current (RR) .... (RD) ]
	start = m_requested_range -  m_aws_response_handler.get_processed_size();
      } else {
	//the current chunk is the first chunk and it contains complete request
	//[:current:first-chunk (RR) .... (RD) ]
	start = m_requested_range;
      }

      for(sc=start;sc<len;sc++)//assumption : row-delimiter must exist or its end ebject
      {
	char* p = const_cast<char*>(it_cp) + ofs + sc;
	if(!strncmp(row_delimiter,p,1)){
	      ldout(s->cct, 10) << "S3select: found row-delimiter on " << sc << " get_processed_size = " << m_aws_response_handler.get_processed_size() <<  dendl;
	      len = sc + 1;//+1 is for delimiter.  TODO what about m_object_size_for_processing (to update according to len)
	      //the end of row exist in current chunk.
	      //thus, the next chunk should be skipped
	      m_skip_next_chunk = true; 
	      break;
	}
      }
    }
    ofs += new_offset;
  }

  ldout(s->cct, 10) << "S3select: shape_chunk_per_trino_requests:update progress len = " << len << dendl;
  len -= new_offset;
}

int RGWSelectObj_ObjStore_S3::csv_processing(bufferlist& bl, off_t ofs, off_t len)
{
  int status = 0;
  if(m_skip_next_chunk == true){
    return status;
  } 

  if (s->obj_size == 0 || m_object_size_for_processing == 0) {
    status = run_s3select_on_csv(m_sql_query.c_str(), nullptr, 0);
    if (status<0){
      return -EINVAL;
    }
  } else {
    auto bl_len = bl.get_num_buffers();
    int buff_no=0;
    for(auto& it : bl.buffers()) {
      ldpp_dout(this, 10) << "s3select :processing segment " << buff_no << " out of " << bl_len << " off " << ofs
                          << " len " << len << " obj-size " << m_object_size_for_processing << dendl;
      if (it.length() == 0 || len == 0) {
        ldpp_dout(this, 10) << "s3select :it->_len is zero. segment " << buff_no << " out of " << bl_len
                            <<  " obj-size " << m_object_size_for_processing << dendl;
        continue;
      }

      if((ofs + len) > it.length()){
	ldpp_dout(this, 10) << "offset and length may cause invalid read: ofs = " << ofs << " len = " << len << " it.length() = " << it.length() << dendl;
	ofs = 0;
	len = it.length();
      }

    if(m_is_trino_request){
      shape_chunk_per_trino_requests(&(it)[0], ofs, len); 
    }

    ldpp_dout(this, 10) << "s3select: chunk:  ofs = " << ofs << " len = " << len << " it.length() = " << it.length() << " m_object_size_for_processing = " << m_object_size_for_processing << dendl;
    
    m_aws_response_handler.update_processed_size(it.length());//NOTE : to run analysis to validate len is aligned with m_processed_bytes
    status = run_s3select_on_csv(m_sql_query.c_str(), &(it)[0] + ofs, len);
    if (status<0) {
	  return -EINVAL;
    }
    if (m_s3_csv_object.is_sql_limit_reached()) {
	  break;
    }
    buff_no++;
  }//for
  }//else

  ldpp_dout(this, 10) << "s3select : m_aws_response_handler.get_processed_size() " << m_aws_response_handler.get_processed_size() 
  << " m_object_size_for_processing " << uint64_t(m_object_size_for_processing) << dendl;

  if (m_aws_response_handler.get_processed_size() >= uint64_t(m_object_size_for_processing) || m_s3_csv_object.is_sql_limit_reached()) {
    if (status >=0) {
      m_aws_response_handler.init_stats_response();
      m_aws_response_handler.send_stats_response();
      m_aws_response_handler.init_end_response();
      ldpp_dout(this, 10) << "s3select : reached the end of query request : aws_response_handler.get_processed_size() " << m_aws_response_handler.get_processed_size()
      << "m_object_size_for_processing : " << m_object_size_for_processing << dendl;
    }
    if (m_s3_csv_object.is_sql_limit_reached()) {
    //stop fetching chunks
    ldpp_dout(this, 10) << "s3select : reached the limit :" << m_aws_response_handler.get_processed_size()  << dendl;
    status = -ENOENT;
    }
  }

  return status;
}

int RGWSelectObj_ObjStore_S3::json_processing(bufferlist& bl, off_t ofs, off_t len)
{
  int status = 0;
  
  if (s->obj_size == 0 || m_object_size_for_processing == 0) {
    //in case of empty object the s3select-function returns a correct "empty" result(for aggregation and non-aggregation queries).
    status = run_s3select_on_json(m_sql_query.c_str(), nullptr, 0);
    if (status<0)
      return -EINVAL;
  } else {
    //loop on buffer-list(chunks)
    auto bl_len = bl.get_num_buffers();
    int i=0;
    for(auto& it : bl.buffers()) {
      ldpp_dout(this, 10) << "processing segment " << i << " out of " << bl_len << " off " << ofs
                          << " len " << len << " obj-size " << m_object_size_for_processing << dendl;
      //skipping the empty chunks
      if (len == 0) {
        ldpp_dout(this, 10) << "s3select:it->_len is zero. segment " << i << " out of " << bl_len
                            <<  " obj-size " << m_object_size_for_processing << dendl;
        continue;
      }

      if((ofs + len) > it.length()){
	ldpp_dout(this, 10) << "s3select: offset and length may cause invalid read: ofs = " << ofs << " len = " << len << " it.length() = " << it.length() << dendl;
	ofs = 0;
	len = it.length();
      }

      m_aws_response_handler.update_processed_size(len);
      status = run_s3select_on_json(m_sql_query.c_str(), &(it)[0] + ofs, len);
      if (status<0) {
	status = -EINVAL;
        break;
      }
      if (m_s3_json_object.is_sql_limit_reached()) {
	break;
      }
      i++;
    }//for
  }//else

  if (status>=0 && (m_aws_response_handler.get_processed_size() == uint64_t(m_object_size_for_processing) || m_s3_json_object.is_sql_limit_reached())) {
    //flush the internal JSON buffer(upon last chunk)
    status = run_s3select_on_json(m_sql_query.c_str(), nullptr, 0);
    if (status<0) {
      return -EINVAL;
    }
    if (status >=0) {
      m_aws_response_handler.init_stats_response();
      m_aws_response_handler.send_stats_response();
      m_aws_response_handler.init_end_response();
    }
    if (m_s3_json_object.is_sql_limit_reached()){
      //stop fetching chunks
      status = -ENOENT;
      ldpp_dout(this, 10) << "s3select : reached the limit :" << m_aws_response_handler.get_processed_size()  << dendl;
    }
  }
  return status;
}

int RGWSelectObj_ObjStore_S3::send_response_data(bufferlist& bl, off_t ofs, off_t len)
{
  if (m_scan_range_ind == false){
    m_object_size_for_processing = s->obj_size;
  }
  if (m_scan_range_ind == true){
      if (m_end_scan_sz == -1){
       	m_end_scan_sz = s->obj_size;
      }
      if (static_cast<uint64_t>((m_end_scan_sz - m_start_scan_sz))>s->obj_size){ //in the case user provides range bigger than object-size
	m_object_size_for_processing = s->obj_size;
      } else {
	m_object_size_for_processing = m_end_scan_sz - m_start_scan_sz;
      }
  }
  if (!m_aws_response_handler.is_set()) {
    m_aws_response_handler.set(s, this);
  }
  if (len == 0 && s->obj_size != 0) {
    return 0;
  }
  if (m_parquet_type) {
    return parquet_processing(bl,ofs,len);
  }
  if (m_json_type) {
    return json_processing(bl,ofs,len);
  }
  return csv_processing(bl,ofs,len);
}

