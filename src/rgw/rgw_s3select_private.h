// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
//
#pragma once

#include <errno.h>
#include <array>
#include <string.h>
#include <string_view>

#include "common/ceph_crypto.h"
#include "common/split.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "auth/Crypto.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/tokenizer.hpp>
#define BOOST_BIND_GLOBAL_PLACEHOLDERS
#ifdef HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
#endif
#ifdef HAVE_WARN_IMPLICIT_CONST_INT_FLOAT_CONVERSION
#pragma clang diagnostic pop
#endif
#undef BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <liboath/oath.h>


#pragma GCC diagnostic push
#pragma clang diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated"
#pragma clang diagnostic ignored "-Wdeprecated"
#include <s3select/include/s3select.h>
#pragma GCC diagnostic pop
#pragma clang diagnostic pop

#include "rgw_rest_s3.h"
#include "rgw_s3select.h"

class aws_response_handler
{

private:
  std::string sql_result;//SQL result buffer
  std::string continue_result;//CONT-MESG buffer
  std::string error_result;//SQL error buffer
  req_state* s;
  uint32_t header_size;
  // the parameters are according to CRC-32 algorithm and its aligned with AWS-cli checksum
  boost::crc_optimal<32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true> crc32;
  RGWOp* m_rgwop;
  std::string m_buff_header_;//response buffer
  std::string m_buff_continue;//response buffer
  //m_buff_ptr : a switch between m_buff_header_ and m_buff_continue
  std::string* m_buff_ptr=nullptr;
  uint64_t total_bytes_returned;
  uint64_t processed_size;
  uint32_t m_success_header_size;


  enum class header_name_En {
    EVENT_TYPE,
    CONTENT_TYPE,
    MESSAGE_TYPE,
    ERROR_CODE,
    ERROR_MESSAGE
  };

  enum class header_value_En {
    RECORDS,
    OCTET_STREAM,
    EVENT,
    CONT,
    PROGRESS,
    END,
    XML,
    STATS,
    ENGINE_ERROR,
    ERROR_TYPE
  };

  const char* PAYLOAD_LINE= "\n<Payload>\n<Records>\n<Payload>\n";
  const char* END_PAYLOAD_LINE= "\n</Payload></Records></Payload>";
  const char* header_name_str[5] =  {":event-type", ":content-type", ":message-type", ":error-code", ":error-message"};
  const char* header_value_str[10] = {"Records", "application/octet-stream", "event", "Cont", "Progress", "End", "text/xml", "Stats", "s3select-engine-error", "error"};
  static constexpr size_t header_crc_size = 12;

  void push_header(const char* header_name, const char* header_value);

  int create_message(u_int32_t header_len,std::string*);

public:
  aws_response_handler(req_state* ps, RGWOp* rgwop) : s(ps), m_rgwop(rgwop), total_bytes_returned{0}, processed_size{0}
  {}

  aws_response_handler() : s(nullptr), m_rgwop(nullptr), total_bytes_returned{0}, processed_size{0}
  {}

  bool is_set()
  {
    if(s==nullptr || m_rgwop == nullptr){
      return false;
    } 
    return true;
  }

  void set(req_state* ps, RGWOp* rgwop)
  {
    s = ps;
    m_rgwop = rgwop;
  }

  std::string& get_sql_result();

  uint64_t get_processed_size();

  void update_processed_size(uint64_t value);

  uint64_t get_total_bytes_returned();

  void update_total_bytes_returned(uint64_t value);

  int create_header_records();

  int create_header_continuation();

  int create_header_progress();

  int create_header_stats();

  int create_header_end();

  int create_error_header_records(const char* error_message);

  void init_response();

  void init_success_response();

  void send_continuation_response();

  void init_progress_response();

  void init_end_response();

  void init_stats_response();

  void send_error_response(const char* error_message);

  void send_success_response();

  void send_progress_response();

  void send_stats_response();

  void send_error_response_rgw_formatter(const char* error_code,
                           const char* error_message,
                           const char* resource_id);

  std::string* get_buffer()
  {
    if(!m_buff_ptr) set_main_buffer();
    return m_buff_ptr;
  }

  void set_continue_buffer()
  {
    m_buff_ptr = &m_buff_continue;
  }

  void set_main_buffer()
  {
    m_buff_ptr = &m_buff_header_;
  }

}; //end class aws_response_handler

class RGWSelectObj_ObjStore_S3 : public RGWGetObj_ObjStore_S3
{

private:
  s3selectEngine::s3select s3select_syntax;
  std::string m_s3select_query;
  std::string m_s3select_input;
  std::string m_s3select_output;
  s3selectEngine::csv_object m_s3_csv_object;
#ifdef _ARROW_EXIST
  s3selectEngine::parquet_object m_s3_parquet_object;
#endif
  s3selectEngine::json_object m_s3_json_object;
  std::string m_column_delimiter;
  std::string m_quot;
  std::string m_row_delimiter;
  std::string m_compression_type;
  std::string m_escape_char;
  std::string m_header_info;
  std::string m_sql_query;
  std::string m_enable_progress;
  std::string output_column_delimiter;
  std::string output_quot;
  std::string output_escape_char;
  std::string output_quote_fields;
  std::string output_row_delimiter;
  std::string m_start_scan;
  std::string m_end_scan;
  bool m_scan_range_ind;
  int64_t m_start_scan_sz;
  int64_t m_end_scan_sz;
  int64_t m_object_size_for_processing;
  aws_response_handler m_aws_response_handler;
  bool enable_progress;

  //parquet request
  bool m_parquet_type;
  //json request
  std::string m_json_datatype;
  bool m_json_type;
#ifdef _ARROW_EXIST
  s3selectEngine::rgw_s3select_api m_rgw_api;
#endif
  //a request for range may satisfy by several calls to send_response_date;
  size_t m_request_range;
  std::string requested_buffer;
  std::string range_req_str;
  std::function<int(std::string&)> fp_result_header_format;
  std::function<int(std::string&)> fp_s3select_result_format;
  std::function<int(std::string&)> fp_s3select_continue;
  std::function<void(const char*)> fp_debug_mesg;
  std::function<void(void)> fp_chunked_transfer_encoding;
  int m_header_size;

  const char* s3select_processTime_error = "s3select-ProcessingTime-Error";
  const char* s3select_syntax_error = "s3select-Syntax-Error";
  const char* s3select_resource_id = "resourcse-id";
  const char* s3select_json_error = "json-Format-Error";

public:
  unsigned int chunk_number;
  size_t m_requested_range;
  size_t m_scan_offset;
  bool m_skip_next_chunk;
  bool m_is_trino_request;

  RGWSelectObj_ObjStore_S3();
  virtual ~RGWSelectObj_ObjStore_S3();

  virtual int send_response_data(bufferlist& bl, off_t ofs, off_t len) override;

  virtual int get_params(optional_yield y) override;

  virtual void execute(optional_yield) override;

private:

  int csv_processing(bufferlist& bl, off_t ofs, off_t len);

  int parquet_processing(bufferlist& bl, off_t ofs, off_t len);

  int json_processing(bufferlist& bl, off_t ofs, off_t len);

  int run_s3select_on_csv(const char* query, const char* input, size_t input_length);

  int run_s3select_on_parquet(const char* query);

  int run_s3select_on_json(const char* query, const char* input, size_t input_length);

  int extract_by_tag(std::string input, std::string tag_name, std::string& result);

  void convert_escape_seq(std::string& esc);

  int handle_aws_cli_parameters(std::string& sql_query);

  int range_request(int64_t start, int64_t len, void*, optional_yield);

  size_t get_obj_size();
  std::function<int(int64_t, int64_t, void*, optional_yield*)> fp_range_req;
  std::function<size_t(void)> fp_get_obj_size;

  void shape_chunk_per_trino_requests(const char*, off_t& ofs, off_t& len);
};

