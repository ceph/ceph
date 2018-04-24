// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Daniel Oliveira <doliveira@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef LDAP_UTILS_HPP
#define LDAP_UTILS_HPP

/* Include order and names:
 * a) Immediate related header
 * b) C libraries (if any),
 * c) C++ libraries,
 * d) Other support libraries
 * e) Other project's support libraries
 *
 * Within each section the includes should
 * be ordered alphabetically.
 */

#include <ldap.h>
#include <lber.h>
#include <ldap_schema.h>
#include <openssl/bio.h>
#include <openssl/evp.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common_utils.hpp"


namespace ldap_utils {

using LDAPPtr_t = LDAP*;

std::vector<char> base64_encode(const char*, const size_t);
auto base64_encode(const std::string&);
std::vector<char> base64_decode(const char*, const size_t);
auto base64_decode(const std::string&);

static constexpr int32_t LDAP_AUTH_OK(0);
static constexpr int32_t LDAP_AUTH_FAILED(-1);
static constexpr int32_t LDAP_PROTOCOL_VERSION(3);
static constexpr u_short LDAP_DEFAULT_PORT_NUM(389);
static constexpr u_short LDAPS_DEFAULT_PORT_NUM(636);
static constexpr uint32_t LDAP_ENTRY_DEFAULT_VECTOR_SIZE(64);
static constexpr uint32_t LDAP_MODIFY_DEFAULT_SIZE(4);

//-- TODO: Convert (where possible) std::string to std::string_view.
static const std::string
      LDAP_DEFAULT_PORT_STR(std::to_string(LDAP_DEFAULT_PORT_NUM));
static const std::string
      LDAPS_DEFAULT_PORT_STR(std::to_string(LDAPS_DEFAULT_PORT_NUM));
static const std::string
      LDAP_SERVICE_NAME("ldap");
static const std::string
      LDAPS_SERVICE_NAME("ldaps");
static const std::string
      LDAP_DEFAULT_SEARCH_FILTER("objectClass=*");


class LDAPExceptionHandler : public std::exception
{
  public:
    LDAPExceptionHandler() throw() : m_ldap_connection_handler(nullptr),
                                     m_ldap_status(common_utils::ZERO) { }
    LDAPExceptionHandler(LDAPPtr_t ldap_connection_handler,
                         int ldap_status,
                         const char* ldap_func =
                            common_utils::EMPTY_STR.c_str()) throw();

    ~LDAPExceptionHandler() throw() override = default;
    const char* what() const throw() override;


    LDAPPtr_t m_ldap_connection_handler{nullptr};
    int m_ldap_status{0};
};


// Possible/valid gss authentication options.
enum class LDAPAuthenticationOptions {
  SIMPLE,
  SASL,
};

// Possible states for the authentication
enum class LDAPRequestType {
  NONE    = -1,
  BIND    =  0,
  UNBIND  =  2,
  SEARCH  =  3,
  MODIFY  =  7,
  ADD     =  8,
  DELETE  = 10,
  COMPARE = 14,
};

enum class LDAPSearchScope {
  NONE = -1,
  BASE =  0,
  ONE,
  SUB,
};

enum class LDAPMsgType {
  // Response Message Types (constants)
  ADD_RESPONSE        = LDAP_RES_ADD,
  BIND_RESPONSE       = LDAP_RES_BIND,
  COMPARE_RESPONSE    = LDAP_RES_COMPARE,
  DEL_RESPONSE        = LDAP_RES_DELETE,
  EXTENDED_RESPONSE   = LDAP_RES_EXTENDED,
  MODDN_RESPONSE      = LDAP_RES_MODDN,
  MODIFY_RESPONSE     = LDAP_RES_MODIFY,
  SEARCH_DONE         = LDAP_RES_SEARCH_RESULT,
  SEARCH_ENTRY        = LDAP_RES_SEARCH_ENTRY,
  SEARCH_REFERENCE    = LDAP_RES_SEARCH_REFERENCE,

  // Request Message Types (constants)
  ABANDON_REQUEST     = LDAP_REQ_ABANDON,
  ADD_REQUEST         = LDAP_REQ_ADD,
  BIND_REQUEST        = LDAP_REQ_BIND,
  COMPARE_REQUEST     = LDAP_REQ_COMPARE,
  DELETE_REQUEST      = LDAP_REQ_DELETE,
  EXTENDED_REQUEST    = LDAP_REQ_EXTENDED,
  MODIFY_REQUEST      = LDAP_REQ_MODIFY,
  MODRDN_REQUEST      = LDAP_REQ_MODRDN,
  SEARCH_REQUEST      = LDAP_REQ_SEARCH,
  UNBIND_REQUEST      = LDAP_REQ_UNBIND,
};

enum class LDAPSSLOption {
    OFF,
    ON
};

enum class LDAPModifyOperation {
  NONE        = -1,
  ADD         = LDAP_MOD_ADD,
  DELETE      = LDAP_MOD_DELETE ,
  REPLACE     = LDAP_MOD_REPLACE,
  INCREMENT   = LDAP_MOD_INCREMENT,
  OPERATION   = LDAP_MOD_OP,
  BIN_VALUES  = LDAP_MOD_BVALUES,
};

// ldap://host:port/baseDN[?attr[?scope[?filter]]]
enum class LDAPUrlComponent {
  BASE,
  ATTRIBUTES,
  SCOPE,
  FILTER,
  EXTENSIONS,
};

enum class LDAPUrlErrorType {
  ERROR_SCHEME,
  ERROR_PORT,
  ERROR_SCOPE,
  ERROR_URL,
  ERROR_DECODING,
};

// ldap.conf(5) man-page
enum class LDAPTlsOption {
  CACERTFILE,
  CACERTDIR,
  CERTFILE,
  KEYFILE,
  REQUIRE_CERT,
  PROTOCOL_MIN,
  CIPHER_SUITE,
  RANDOM_FILE,
  CRL_CHECK,
  DHFILE,
};

enum class LDAPTlsRequireCertOption {
  NEVER,
  HARD,
  DEMAND,
  ALLOW,
  TRY,
};

enum class LDAPTlsCrlCheckOption {
  CRL_NONE,
  CRL_PEER,
  CRL_ALL,
};

// based on RFC 2251
enum class LDAPResultType {
  NONE                           = -1,
  SUCCESS                        = 0,
  OPERATIONS_ERROR               = 1,
  PROTOCOL_ERROR                 = 2,
  TIME_LIMIT_EXCEEDED            = 3,
  SIZE_LIMIT_EXCEEDED            = 4,
  COMPARE_FALSE                  = 5,
  COMPARE_TRUE                   = 6,
  AUTH_METHOD_NOT_SUPPORTED      = 7,
  STRONG_AUTH_REQUIRED           = 8,

  REFERRAL                       = 10,
  ADMIN_LIMIT_EXCEEDED           = 11,
  UNAVAILABLE_CRITICAL_EXTENSION = 12,
  CONFIDENTIALITY_REQUIRED       = 13,
  SASL_BIND_IN_PROGRESS          = 14,

  NO_SUCH_ATTRIBUTE              = 16,
  UNDEFINED_ATTRIBUTE_TYP        = 17,
  INAPPROPRIATE_MATCHING         = 18,
  CONSTRAINT_VIOLATION           = 19,
  ATTRIBUTE_OR_VALUE_EXISTS      = 20,
  INVALID_ATTRIBUTE_SYNTAX       = 21,

  NO_SUCH_OBJECT                 = 32,
  ALIAS_PROBLEM                  = 33,
  INVALID_DN_SYNTAX              = 34,

  ALIAS_DEREFERENCING_PROBLEM    = 36,

  INAPPROPRIATE_AUTENTICATION    = 48,
  INVALID_CREDENTIALS            = 49,
  INSUFFICIENT_ACCESS            = 50,
  BUSY                           = 51,
  UNAVAILABLE                    = 52,
  UNWILLING_TO_PERFORM           = 53,
  LOOP_DETECT                    = 54,

  NAMING_VIOLATION               = 64,
  OBJECT_CLASS_VIOLATION         = 65,
  NOT_ALLOWED_ON_NONLEAF         = 66,
  NOT_ALLOWED_ON_RDN             = 67,
  ENTRY_ALREADY_EXISTS           = 68,
  OBJECT_CLASS_MODS_PROHIBITED   = 69,

  AFFECTS_MULTIPLE_DSAS          = 71,

  // defined in the LDAP C API DRAFT
  OTHER                          = 80,
  SERVER_DOWN                    = 81,
  LOCAL_ERROR                    = 82,
  ENCODING_ERROR                 = 83,
  DECODING_ERROR                 = 84,
  TIMEOUT                        = 85,
  AUTH_UNKNOWN                   = 86,
  FILTER_ERROR                   = 87,
  USER_CANCELLED                 = 88,
  PARAM_ERROR                    = 89,
  NO_MEMORY                      = 90,
  CONNECT_ERROR                  = 91,
  NOT_SUPPORTED                  = 92,
  CONTROL_NOT_FOUND              = 93,
  NO_RESULTS_RETURNED            = 94,
  MORE_RESULTS_TO_RETURN         = 95,
  CLIENT_LOOP                    = 96,
  REFERRAL_LIMIT_EXCEEDED        = 97,
};

enum class LDAPAttributeUsageType {
  NONE                    = -1,
  USER_APPLICATION        = 0,
  DIRECTORY_OPERATION,
  DISTRIBUTED_OPERATION,
  DSA_OPERATION,
};

enum class LDAPObjectClassType {
  NONE = -1,
  ABSTRACT = 0,
  STRUCTURAL,
  AUXILIARY,
};

enum class LDAPAliasType {
  DEREF_NEVER       = 0x00,
  DEREF_SEARCHIUNG  = 0x01,
  DEREF_FINDING     = 0x02,
  DEREF_ALWAYS      = 0x04,
};

const std::map<LDAPRequestType, std::string> ldap_request_type_lookup
{
  {LDAPRequestType::NONE,    "Request Type: None "},
  {LDAPRequestType::ADD,     "Request Type: Add "},
  {LDAPRequestType::BIND,    "Request Type: Bind "},
  {LDAPRequestType::COMPARE, "Request Type: Compare "},
  {LDAPRequestType::DELETE,  "Request Type: Delete "},
  {LDAPRequestType::MODIFY,  "Request Type: Modify "},
  {LDAPRequestType::SEARCH,  "Request Type: Search "},
  {LDAPRequestType::UNBIND,  "Request Type: Unbind "},
};

const std::map<LDAPSearchScope, std::string> ldap_scope_type_lookup
{
  {LDAPSearchScope::NONE, "Scope: None "},
  {LDAPSearchScope::BASE, "Scope: Base "},
  {LDAPSearchScope::ONE,  "Scope: One "},
  {LDAPSearchScope::SUB,  "Scope: Sub "},
};

const std::map<LDAPMsgType, std::string> ldap_msg_type_lookup
{
  // Response Message Types (constants)
  {LDAPMsgType::ABANDON_REQUEST,   "Request: Abandon() "},
  {LDAPMsgType::ADD_REQUEST,       "Request: Add() "},
  {LDAPMsgType::BIND_REQUEST,      "Request: Bind() "},
  {LDAPMsgType::COMPARE_REQUEST,   "Request: Compare() "},
  {LDAPMsgType::DELETE_REQUEST,    "Request: Delete() "},
  {LDAPMsgType::EXTENDED_REQUEST,  "Request: Extended() "},
  {LDAPMsgType::MODIFY_REQUEST,    "Request: Modify() "},
  {LDAPMsgType::MODRDN_REQUEST,    "Request: ModRDN() "},
  {LDAPMsgType::SEARCH_REQUEST,    "Request: Search() "},
  {LDAPMsgType::UNBIND_REQUEST,    "Request: Unbind() "},

  {LDAPMsgType::ADD_RESPONSE,      "Response: Add() "},
  {LDAPMsgType::BIND_RESPONSE,     "Response: Bind() "},
  {LDAPMsgType::COMPARE_RESPONSE,  "Response: Compare() "},
  {LDAPMsgType::DEL_RESPONSE,      "Response: Delete() "},
  {LDAPMsgType::EXTENDED_RESPONSE, "Response: Extended() "},
  {LDAPMsgType::MODDN_RESPONSE,    "Response: ModDN() "},
  {LDAPMsgType::MODIFY_RESPONSE,   "Response: Modify() "},
  {LDAPMsgType::SEARCH_DONE,       "Response: Search_Done() "},
  {LDAPMsgType::SEARCH_ENTRY,      "Response: Search_Entry() "},
  {LDAPMsgType::SEARCH_REFERENCE,  "Response: Search_Reference() "},
};

template<typename K, typename V>
bool find_in_lookup_by_value(const std::vector<K>& lookup_table,
                             const V& value)
{
  auto is_in_lookup_table(false);
  std::for_each(begin(lookup_table),
                end(lookup_table), [&](K val) {
    if (val == value) {
      is_in_lookup_table = true;
    }
  });
  return is_in_lookup_table;
}

template<typename K, typename V>
bool sort_find_in_lookup_by_value(const std::vector<K>& lookup_table,
                                  const V& value)
{
  //TO DO: Get a more efficient way to copy stuff.
  //std::map<K, K> ordered_lookup(begin(lookup_table), end(lookup_table));
  std::map<K, K> ordered_lookup;
  for (const auto& elem : lookup_table) {
    ordered_lookup.emplace(std::make_pair(elem, elem));
  }
  auto search_value = ordered_lookup.find(value);
  return search_value != end(ordered_lookup);
}

template<typename K, typename V>
bool find_in_lookup_tbl_by_value(const std::map<K,V>& lookup_table,
                                 const V& value)
{

  for (const auto& elem : lookup_table) {
    K& key(elem.first);
    V& val(elem.second);

    if (val == value) {
      return true;
    }
  }
  return false;
}

}   //-- namespace ldap_utils

#endif    //-- LDAP_UTILS_HPP

// ----------------------------- END-OF-FILE --------------------------------//

