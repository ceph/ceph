// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef PROJECT_RGW_REST_CALLBACK_S3_H
#define PROJECT_RGW_REST_CALLBACK_S3_H
#include <errno.h>
#include <string.h>

#include "common/ceph_crypto.h"
#include "common/safe_io.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>

#include <openssl/pem.h>

#include "rgw_rest.h"
#include "rgw_rest_s3.h"

#include "rgw/rgw_b64.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

class RGWCallBackClient : public RGWPostHTTPData {
    int http_status;
    bufferlist * const read_bl;
    bool callback_contentlength_valid;
    bool callback_contenttype_valid;
    size_t max_response;
  public:
    RGWCallBackClient(CephContext * const cct,
    const string& method,
    const string& url,
    bufferlist * const read_bl,
    const header_spec_t intercept_headers = {})
    : RGWPostHTTPData(cct, method, url, read_bl, intercept_headers),
      relevant_headers(intercept_headers),
      callback_contentlength_valid(false),
      callback_contenttype_valid(false),
      read_bl(read_bl) {
    }
    void set_max_response (size_t _max_response) {
        max_response = _max_response;
    }
    int receive_header(void * const ptr, const size_t len);
    void set_http_status(long _http_status);
    long get_http_status() const {
        return http_status;
    }
    /* Throws std::out_of_range */
    const header_value_t& get_header_value(const header_name_t& name) const {
        return found_headers.at(name);
    }
    bool is_valid_content_lenth() const {
        return callback_contentlength_valid;
    }
    bool is_valid_content_type() const {
        return callback_contenttype_valid;
    }
  private:
    const std::set<header_name_t, ltstr_nocase> relevant_headers;
    std::map<header_name_t, header_value_t, ltstr_nocase> found_headers;

  protected:
    int receive_data(void *ptr, size_t len, bool *pause) override;
};

std::string get_mimetype_from_name(struct req_state *s, const std::string name);
std::string load_rgw_rsa_pri_key(CephContext *ctx, const std::string path);
RSA *createPrivateRSA(std::string key);
bool RSASign(RSA *rsa, const unsigned char *Msg, size_t MsgLen, unsigned char **EncMsg, size_t *MsgLenEnc);
boost::optional <std::string> signMessage(std::string privateKey, std::string plainText);
boost::optional <std::string> generate_auth_header_for_callback_request(const boost::string_ref url,
                                                                        const string body,
                                                                        const string privateKey);
void process_callback_err(struct req_state *s, RGWOp* op, int err_no, const char* msg);
int process_callback(struct req_state *s,
                     RGWOp* op,
                     int op_ret,
                     std::string etag,
                     boost::optional<std::map<std::string, RGWPostObj_ObjStore::post_form_part, 
					 const ltstr_nocase>> parts);

#endif //PROJECT_RGW_REST_CALLBACK_S3_H
