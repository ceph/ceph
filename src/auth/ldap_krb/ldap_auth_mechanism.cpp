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

#include "ldap_auth_mechanism.hpp"

#include <algorithm>
#include <cstring>
#include <functional>
#include <iomanip>


namespace ldap_client_auth {

const std::string BIN_CONTENT_STR(";binary");
const auto NEWLINE("\n");


int32_t LDAPMechanismBase::
ldap_connect(const std::string& ldap_uri,
             const ldap_utils::LDAPSSLOption& ldap_ssl_option)
{
  if (ldap_uri.empty()) {
    return common_utils::auth_trap_error(LDAP_URL_ERR_BADURL);
  }

  auto ldap_uri_tmp(ldap_build_uri(ldap_uri, ldap_ssl_option));
  if (m_ldap_connection_handler) {
    ldap_disconnect();
  }

  /*  It allocates an LDAP structure but does not open an initial
      connection.

    int ldap_initialize(ldp, uri)
    LDAP **ldp;
    char *uri;
  */
  constexpr auto ldap_protocol_version(3);
  m_ldap_status = ldap_initialize(&m_ldap_connection_handler,
                                  ldap_uri_tmp.c_str());
  if (m_ldap_connection_handler) {

    /*
      int ldap_set_option(LDAP *ld, int option, const void *invalue)
    */
    ldap_set_option(m_ldap_connection_handler,
                    LDAP_OPT_PROTOCOL_VERSION,
                    &ldap_protocol_version);
  }
  return common_utils::auth_trap_error(static_cast<const uint32_t>
                                                  (m_ldap_status));
}

void LDAPMechanismBase::ldap_disconnect()
{
  if (m_ldap_connection_handler) {
    ldap_unbind();
  }
  m_ldap_uri.clear();
}

std::string LDAPMechanismBase::
ldap_build_uri(const std::string& ldap_uri,
               const ldap_utils::LDAPSSLOption& ldap_ssl_option)
{
  if (ldap_uri.empty()) {
    return ldap_uri;
  }

  std::string new_ldap_uri(ldap_uri);
  if (ldap_uri.find(common_utils::COLON) == std::string::npos) {
    new_ldap_uri = ldap_utils::LDAP_SERVICE_NAME;
    if (ldap_ssl_option == ldap_utils::LDAPSSLOption::ON) {
      new_ldap_uri = ldap_utils::LDAPS_SERVICE_NAME;
    }
    new_ldap_uri += (common_utils::PROTO_SEP + ldap_uri);
  }
  return new_ldap_uri;
}

LDAPMechanismBase::LDAPMechanismBase() : m_ldap_connection_handler(nullptr),
                                         m_ldap_status(0) { }

LDAPMechanismBase::
LDAPMechanismBase(const auth_mechanisms::AuthOptions& authOptions) :
    LDAPMechanismBase()
{
  m_authentication_options = authOptions;
}

LDAPMechanismBase::
LDAPMechanismBase(const std::string& ldap_uri,
                  const ldap_utils::LDAPSSLOption& ldap_ssl_option) :
    LDAPMechanismBase()
{
  m_ldap_uri    = ldap_uri; 
  m_ssl_option  = ldap_ssl_option;
  ldap_build_uri(ldap_uri, ldap_ssl_option);
}

LDAPMechanismBase::~LDAPMechanismBase()
{
  if (m_ldap_connection_handler) {
    ldap_disconnect();
  }
}

auth_mechanisms::AuthenticationStatus
LDAPMechanismBase::get_auth_status() const
{
  return m_ldap_connection_handler ?
         auth_mechanisms::AuthenticationStatus::HANDSHAKING :
         auth_mechanisms::AuthenticationStatus::NONE;
}

int32_t LDAPMechanismBase::ldap_bind()
{
  if (!m_ldap_connection_handler) {
    ldap_connect();
  }

  struct berval credentials;
  credentials.bv_val = const_cast<charPtr_t>(m_ldap_bind_passwd.c_str());
  credentials.bv_len = m_ldap_bind_passwd.size();

  /*
    int ldap_sasl_bind_s(LDAP *ld, const char *dn, const char *mechanism,
    struct berval *cred, LDAPControl *sctrls[],
        LDAPControl *cctrls[], struct berval **servercredp);
  */
  m_ldap_status = ldap_sasl_bind_s(m_ldap_connection_handler,
                                   m_ldap_bind_dn.c_str(),
                                   nullptr,
                                   &credentials,
                                   nullptr,
                                   nullptr,
                                   nullptr);
  return common_utils::auth_trap_error(m_ldap_status);
}

int32_t LDAPMechanismBase::ldap_bind(const std::string& ldap_bind_dn,
                                     const std::string& ldap_bind_passwd)
{
  m_ldap_bind_dn      = ldap_bind_dn;
  m_ldap_bind_passwd  = ldap_bind_passwd;
  return ldap_bind();
}

void LDAPMechanismBase::ldap_unbind()
{
  if (m_ldap_connection_handler) {

    /*
      int ldap_unbind_ext_s(LDAP *ld, LDAPControl *sctrls[],
                            LDAPControl *cctrls[]);
    */
    m_ldap_status = ldap_unbind_ext_s(m_ldap_connection_handler,
                                      nullptr,
                                      nullptr);
  }

  m_ldap_connection_handler = nullptr;
  m_ldap_bind_dn.clear();
  m_ldap_bind_passwd.clear();
}

bool LDAPMechanismBase::ldap_ping() const
{
  if (m_ldap_connection_handler) {
    LDAPMsgPtr_t ldap_results = nullptr;

    /*  Perform LDAP search operations synchronously.
    int ldap_search_ext_s(
        LDAP *ld,
        char *base,
        int scope,
        char *filter,
        char *attrs[],
        int attrsonly,
        LDAPControl **serverctrls,
        LDAPControl **clientctrls,
        struct timeval *timeout,
        int sizelimit,
        LDAPMessage **res );
    */
    auto ldap_error = ldap_search_ext_s(m_ldap_connection_handler,
                                        nullptr,
                                        static_cast<int>(
                                            ldap_utils::LDAPSearchScope::BASE),
                                        nullptr,
                                        nullptr,
                                        common_utils::ZERO,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        common_utils::ZERO,
                                        &ldap_results);
    if (ldap_results) {
      ldap_msgfree(ldap_results);
    }
    return common_utils::auth_trap_error((ldap_error != LDAP_SERVER_DOWN) ||
                                         (ldap_error != LDAP_TIMEOUT));
  }
  return common_utils::auth_trap_error(false);
}

LDAPEntries_t LDAPMechanismBase::
ldap_search(const std::string& ldap_base_search,
            const std::string& ldap_search_filter,
            const ldap_utils::LDAPSearchScope& ldap_search_scope,
            const LDAPAttributesPtr_t ldap_search_attributes)
{
  LDAPEntries_t ldap_search_result{};
  charPtr_t* ldap_attributes = (ldap_search_attributes &&
      ldap_search_attributes->size()) ?
          (new char* [ldap_search_attributes->size() + 1]) : nullptr;

  if (m_ldap_connection_handler && ldap_search_attributes) {
    charPtr_t* place_holder(ldap_attributes);
    for (auto itr(ldap_search_attributes->begin());
         itr != (ldap_search_attributes->end()); ++itr) {
      *place_holder++ = const_cast<charPtr_t>((*itr).c_str());
    }
  }

  /*  Perform LDAP search operations synchronously.
  int ldap_search_ext_s(
      LDAP *ld,
      char *base,
      int scope,
      char *filter,
      char *attrs[],
      int attrsonly,
      LDAPControl **serverctrls,
      LDAPControl **clientctrls,
      struct timeval *timeout,
      int sizelimit,
      LDAPMessage **res );
  */
  LDAPMsgPtr_t ldap_result = nullptr;
  m_ldap_status = ldap_search_ext_s(m_ldap_connection_handler,
                                   (ldap_base_search.empty() ? nullptr :
                                    ldap_base_search.c_str()),
                                   static_cast<int>(ldap_search_scope),
                                   (ldap_search_filter.empty() ? nullptr :
                                    ldap_search_filter.c_str()),
                                   ldap_attributes,
                                   common_utils::ZERO,
                                   nullptr,
                                   nullptr,
                                   nullptr,
                                   common_utils::ZERO,
                                   &ldap_result);

  //-- Add entries into the vector.
  if ((m_ldap_status == LDAP_SUCCESS) &&
      ldap_count_entries(m_ldap_connection_handler,
                         ldap_result) != common_utils::ZERO) {

    /*  It is used to return the count of the number of entries in the
        search result

        int ldap_count_entries(LDAP *ld, LDAPMessage *result)
     */

    /*  It is used to retrieve the first entry in a chain  of search results.
        LDAPMessage *ldap_first_entry(LDAP *ld, LDAPMessage *result)
     */

    /*  It is used to get the next entry, the result of which should  be
        supplied to the next call to ldap_next_entry().
        It will return NULL when there are no more entries.

        LDAPMessage *ldap_next_entry(LDAP *ld, LDAPMessage *entry)
     */
    for (LDAPMsgPtr_t ldap_entry(ldap_first_entry(m_ldap_connection_handler,
                                                  ldap_result));
         ldap_entry;
         ldap_entry = ldap_next_entry(m_ldap_connection_handler,
                                      ldap_entry)) {

      /*  It is used to return a copy of the entry's DN.

          char *ldap_get_dn( LDAP *ld, LDAPMessage *entry )
       */
      charPtr_t ldap_dn(ldap_get_dn(m_ldap_connection_handler, ldap_entry));
      ldap_search_result.push_back(LDAPEntry(ldap_dn));
      LDAPEntry& last_ldap_dn = ldap_search_result.back();

      /* They are used to step through the attributes in an LDAP entry.

          char *ldap_first_attribute(
            LDAP *ld, LDAPMessage *entry, BerElement **berptr)

          char *ldap_next_attribute(
            LDAP *ld, LDAPMessage *entry, BerElement *ber)
       */
      LDAPBerElemPtr_t ldap_basic_encode_rules(nullptr);
      for (charPtr_t ldap_attribute(ldap_first_attribute(
                                        m_ldap_connection_handler,
                                        ldap_entry,
                                        &ldap_basic_encode_rules));
           ldap_attribute;
           ldap_attribute = ldap_next_attribute(
                                m_ldap_connection_handler,
                                ldap_entry,
                                ldap_basic_encode_rules)) {
        if (LDAPBerValPtr_t* ldap_values =
            ldap_get_values_len(m_ldap_connection_handler,
                                ldap_entry,
                                ldap_attribute)) {
          /*  It takes the entry and the attribute attr whose values
              are desired and returns a NULL-terminated array of the
              attribute's values.

              char **ldap_get_values(ld, entry, attr)
              LDAP *ld;
              LDAPMessage *entry;
              char *attr;
           */
          /*  If the attribute values are binary in nature, and thus
              not suitable to be returned as an array of char *'s,
              we use ldap_get_values_len() instead.

              struct berval **ldap_get_values_len(ld, entry, attr)
              LDAP *ld;
              LDAPMessage *entry;
              char *attr;
           */
          /*  It returns the number/length of values in the values
              array.

              int ldap_count_values(vals)
              char **vals;

              int ldap_count_values_len(vals)
              struct berval **vals;
           */
          auto value_count = ldap_count_values_len(ldap_values);
          if (value_count) {
            auto itr = last_ldap_dn.ldap_entry_push_back(
                new LDAPModify(ldap_attribute,
                               ldap_utils::LDAPModifyOperation::ADD));
            for (auto i(0); i < value_count; ++i) {
              std::string ldap_value(ldap_values[i]->bv_val);
              (*itr)->ldap_modify_append(ldap_value);
              //(*itr)->ldap_modify_append(ldap_values[i]->bv_val);
              //(*itr)->ldap_modify_append(ldap_values[i]->bv_val,
              //                           ldap_values[i]->bv_len);
            }
          }
          ldap_value_free_len(ldap_values);
        }
        ldap_memfree(ldap_attribute);
      }
      if (ldap_basic_encode_rules) {
        ber_free(ldap_basic_encode_rules, common_utils::ZERO);
      }
      if (ldap_dn) {
        ldap_memfree(ldap_dn);
      }
    }
  }
  if (ldap_result) {
    ldap_msgfree(ldap_result);
  }
  if (ldap_attributes) {
    delete[] ldap_attributes;
  }

  return ldap_search_result;
}

int32_t LDAPMechanismBase::ldap_add_entry(const LDAPEntry& ldap_entry )
{
  /*  Perform an LDAP add operation synchronously.
    int ldap_add_ext_s(
    LDAP *ld,
    const char *dn,
    LDAPMod **attrs,
    LDAPControl *sctrls,
    LDAPControl *cctrls );
  */
  if (m_ldap_connection_handler) {
    LDAPModPtr_t* ldap_modify = reinterpret_cast<LDAPModPtr_t*>(
        const_cast<LDAPModificationsPtr_t*>(&ldap_entry[0]));
    m_ldap_status = ldap_add_ext_s(m_ldap_connection_handler,
                                   ldap_entry.ldap_get_entry_dn().c_str(),
                                   ldap_modify,
                                   nullptr,
                                   nullptr);
    return common_utils::auth_trap_error(m_ldap_status == LDAP_SUCCESS);
  }
  return false;
}

int32_t LDAPMechanismBase::ldap_modify_entry(const LDAPEntry& ldap_entry)
{
  /*  Perform an LDAP modify operation synchronously.
    int ldap_modify_ext_s(
    LDAP *ld,
    char *dn,
    LDAPMod *mods[],
    LDAPControl **sctrls,
    LDAPControl **cctrls );
  */
  if (m_ldap_connection_handler) {
    LDAPModPtr_t* ldap_modify = reinterpret_cast<LDAPModPtr_t*>(
        const_cast<LDAPModificationsPtr_t*>(&ldap_entry[0]));
    m_ldap_status = ldap_modify_ext_s(m_ldap_connection_handler,
                                      ldap_entry.ldap_get_entry_dn().c_str(),
                                      ldap_modify,
                                      nullptr,
                                      nullptr);
    return common_utils::auth_trap_error(m_ldap_status == LDAP_SUCCESS);
  }
  return common_utils::auth_trap_error(false);
}

int32_t LDAPMechanismBase::ldap_delete_entry(const std::string& ldap_entry_dn)
{
  /*  Performs an LDAP delete operation synchronously.
    int ldap_delete_ext_s(ld, dn, serverctrls, clientctrls)
    LDAP *ld;
    char *dn;
    LDAPControl **serverctrls, **clientctrls;
  */
  return common_utils::auth_trap_error(LDAP_SUCCESS == (m_ldap_status =
                              ldap_delete_ext_s(m_ldap_connection_handler,
                                                ldap_entry_dn.c_str(),
                                                nullptr,
                                                nullptr)) &&
      m_ldap_connection_handler);
}

int32_t LDAPMechanismBase::
ldap_modify_entry_dn(const std::string& ldap_old_entry_dn,
                     const std::string& ldap_new_entry_dn)
{
  /*  Performs an LDAP rename operation synchronously.
    int ldap_rename_s( ld, dn, newrdn, newparent, deleteoldrdn, sctrls[], cctrls[] );
    LDAP *ld;
    const char *dn, *newrdn, *newparent;
    int deleteoldrdn;
    LDAPControl *sctrls[], *cctrls[];
  */
  return common_utils::auth_trap_error(LDAP_SUCCESS == (m_ldap_status =
                              ldap_rename_s(m_ldap_connection_handler,
                                            ldap_old_entry_dn.c_str(),
                                            ldap_new_entry_dn.c_str(),
                                            nullptr,
                                            common_utils::ONE,
                                            nullptr,
                                            nullptr)) &&
      m_ldap_connection_handler);
}

int32_t LDAPMechanismBase::
ldap_compare_attribute(const std::string& ldap_attribute,
                       const std::string& ldap_attribute_value) const
{
  /*  Performs an LDAP compare operation synchronously.
    int ldap_compare_ext_s(
    LDAP *ld,
    char *dn,
    char *attr,
    const struct berval *bvalue,
    LDAPControl **serverctrls,
    LDAPControl **clientctrls );
  */
  return common_utils::auth_trap_error(
      LDAP_COMPARE_TRUE == (ldap_compare_ext_s(m_ldap_connection_handler,
                                               ldap_attribute.c_str(),
                                               ldap_attribute_value.c_str(),
                                               nullptr,
                                               nullptr,
                                               nullptr)) &&
          m_ldap_connection_handler);
}


LDAPModify::
LDAPModify(const std::string& ldap_modification_type,
           const ldap_utils::LDAPModifyOperation& ldap_modification_operation) :
    m_ldap_modify_size(0)
{
  mod_op      = static_cast<int>(ldap_modification_operation);
  mod_type    = const_cast<charPtr_t>(ldap_modification_type.c_str());
  mod_bvalues = nullptr;
}

LDAPModify::~LDAPModify()
{
  ldap_modify_clear();
}

template<typename T>
void LDAPModify::ldap_modify_append(const T* ldap_value)
{
  static_assert(std::is_same_v<char, T>, "Not supported type!");

  if constexpr (std::is_same_v<char, T>) {
    if (mod_values) {
      charPtr_t* itr_begin(mod_values);
      charPtr_t* itr_end((mod_values + m_ldap_modify_size) - 1); //-- Last one always nullptr
      charPtr_t* itr_position(std::find(itr_begin, itr_end,
                                  static_cast<charPtr_t>(common_utils::ZERO)));
      if ((itr_end > itr_begin) && (itr_end != itr_begin)) {
        *itr_position = ldap_value;   //duplicate_ptr_char()
      }
      else {
        auto new_size(m_ldap_modify_size * common_utils::VECTOR_GROWTH_FACTOR);
        charPtr_t* new_values = (new char* [new_size]);
        std::memset(new_values,
                    common_utils::ZERO,
                    sizeof(charPtr_t) * new_size);
        std::copy(itr_begin, itr_end, new_values);
        delete[] mod_values;
        mod_values = new_values;
        mod_values[m_ldap_modify_size - 1] = ldap_value;  //duplicate_ptr_char()
        m_ldap_modify_size = new_size;
      }
    }
    else {
      //-- TODO:
      m_ldap_modify_size = ldap_utils::LDAP_MODIFY_DEFAULT_SIZE;
      mod_values = (new char* [m_ldap_modify_size]);
      std::memset(mod_values,
                  common_utils::ZERO,
                  sizeof(charPtr_t) * m_ldap_modify_size);
      mod_values[common_utils::ZERO] = ldap_value;  //duplicate_ptr_char()
    }
  }
}

template<typename T>
void LDAPModify::ldap_modify_append(const T& ldap_value)
{
  static_assert(std::is_same_v<std::string, T>, "Not supported type!");

  if constexpr (std::is_same_v<std::string, T>) {
    if (mod_bvalues) {
      /* LDAPBerValPtr_t* */
      auto itr_begin(mod_bvalues);
      auto itr_end((mod_bvalues + m_ldap_modify_size) - 1); //-- Last one always nullptr
      auto itr_position(std::find(itr_begin, itr_end,
                                  static_cast<LDAPBerValPtr_t>(nullptr)));
      if ((itr_end > itr_begin) && (itr_end != itr_begin)) {
        *itr_position = (new LDAPBasicEncodeRuleValue(ldap_value));
      }
      else {
        auto new_size(m_ldap_modify_size * common_utils::VECTOR_GROWTH_FACTOR);
        LDAPBerValPtr_t* new_bvalues = (new berval* [new_size]);
        std::memset(new_bvalues,
                    common_utils::ZERO,
                    sizeof(LDAPBerValPtr_t) * new_size);
        std::copy(itr_begin, itr_end, new_bvalues);
        delete[] mod_bvalues;
        mod_bvalues = new_bvalues;
        mod_bvalues[m_ldap_modify_size - 1] =
            (new LDAPBasicEncodeRuleValue(ldap_value));
        m_ldap_modify_size = new_size;
      }
    }
    else {
      //-- TODO:
      m_ldap_modify_size = ldap_utils::LDAP_MODIFY_DEFAULT_SIZE;
      mod_bvalues = (new berval *[m_ldap_modify_size]);
      std::memset(mod_bvalues,
                  common_utils::ZERO,
                  sizeof(LDAPBerValPtr_t) * m_ldap_modify_size);
      mod_bvalues[common_utils::ZERO] =
          (new LDAPBasicEncodeRuleValue(ldap_value));
      mod_op |= static_cast<int>(ldap_utils::LDAPModifyOperation::BIN_VALUES);
    }
  }
}

void LDAPModify::ldap_modify_clear()
{
  if (mod_bvalues) {
    if (mod_op &
        static_cast<int>(ldap_utils::LDAPModifyOperation::BIN_VALUES)) {
      for (LDAPBerValPtr_t* bvalue = mod_bvalues;
           bvalue && *bvalue; ++bvalue) {
        delete static_cast<LDAPBervalPtr_t>(*bvalue);
      }
      delete[] mod_bvalues;
      mod_bvalues = nullptr;
    }
    else {
      for (charPtr_t* cvalue = mod_values; cvalue && *cvalue; ++cvalue) {
        delete[] mod_values;
        mod_values = nullptr;
      }
    }
  }
  if (mod_type) {
    delete[] mod_type;
    mod_type = nullptr;
  }
  mod_op = common_utils::ZERO;
}

bool LDAPModify::is_it_type(const std::string& ldap_modification_type) const
{
  return (ldap_modification_type.c_str() == mod_type);
}

bool LDAPModify::is_it_operation(
    const ldap_utils::LDAPModifyOperation& ldap_modification_operation) const
{
  return ((mod_op & static_cast<int>(
                        ldap_utils::LDAPModifyOperation::OPERATION)) ==
          static_cast<int>(ldap_modification_operation));
}

const charPtr_t* LDAPModify::get_string_value() const
{
  return (mod_op & static_cast<int>(
                       ldap_utils::LDAPModifyOperation::BIN_VALUES)) ?
         nullptr : mod_values;
}

const LDAPBerValPtr_t* LDAPModify::get_binary_value() const
{
  return (mod_op & static_cast<int>(
                       ldap_utils::LDAPModifyOperation::BIN_VALUES)) ?
         mod_bvalues : nullptr;
}


LDAPEntry::LDAPEntry(const std::string& ldap_entry) :
    m_ldap_entry_dn(ldap_entry)
{
  reserve(ldap_utils::LDAP_ENTRY_DEFAULT_VECTOR_SIZE);
  push_back(nullptr);
}

LDAPEntry::~LDAPEntry()
{
  for (auto itr(begin()); itr != end(); ++itr) {
    delete *itr;
  }
}

template<typename T>
void LDAPEntry::ldap_entry_append(
    const ldap_utils::LDAPModifyOperation& ldap_modification_operation,
    const std::string& ldap_attribute,
    const T& value)
{

  static_assert(std::is_same_v<std::string, T> ||
                std::is_same_v<std::vector<std::string>, T> ||
                std::is_same_v<std::vector<char>, T> ||
                std::is_same_v<std::vector<std::vector<char>>, T>,
                "Not supported type!");

  using const_itr_vect_str        = std::vector<std::string>::const_iterator;
  using const_itr_vect_vect_char  = std::vector<std::vector<
                                                  char>>::const_iterator;

  if (ldap_attribute.empty() && value.empty()) {
    return;
  }

  iterator itr_begin(begin());
  for (; itr_begin != end(); ++itr_begin) {
    if (*itr_begin && (*itr_begin)->is_it_type(ldap_attribute) &&
        (*itr_begin)->is_it_operation(ldap_modification_operation)) {
      break;
    }
    if (itr_begin == end()) {
      itr_begin = ldap_entry_push_back(
          new LDAPModify(ldap_attribute,
                         ldap_modification_operation));
    }
  }

  if constexpr (std::is_same_v<std::string, T>) {
    (*itr_begin)->ldap_modify_append(value);
  }
  if constexpr (std::is_same_v<std::vector<std::string>, T>) {
    const_itr_vect_str itr_vect_str(value.begin());
    for (; itr_vect_str != value.end(); ++itr_vect_str) {
      (*itr_begin)->ldap_modify_append(*itr_vect_str);
    }
  }
  if constexpr (std::is_same_v<std::vector<char>, T>) {
    (*itr_begin)->ldap_modify_append(&value[0]);
  }
  if constexpr (std::is_same_v<std::vector<std::vector<char>>, T>) {
    const_itr_vect_vect_char itr_vect_char(value.begin());
    for (; itr_vect_char != value.end(); ++itr_vect_char) {
      (*itr_begin)->ldap_modify_append<char>(&(*itr_vect_char)[0]);
    }
  }
}

template<typename T>
T LDAPEntry::get_string_value(
    const std::string& ldap_attribute) const
{

  static_assert(std::is_same_v<std::string, T> ||
                std::is_same_v<std::vector<std::string>, T>,
                "Not supported type!");

  if (!ldap_attribute.empty()) {
    auto itr_begin =
        std::find_if(begin(), end(),
                     [&] (const auto& elem) {
                        return (elem.is_it_type(ldap_attribute));
                     }
        );

    if (*itr_begin && itr_begin != end()) {
      if constexpr (std::is_same_v<std::string, T>) {
        if (const LDAPBerValPtr_t* bvalue = (*itr_begin)->
            get_binary_value()) {
          if (bvalue && bvalue[0]) {
            return T(bvalue[0]->bv_val, bvalue[0]->bv_len);
          }
          else {
            if (const charPtr_t* value = (*itr_begin)->
                get_string_value()) {
              if (value && value[0]) {
                return T(value[0]);
              }
            }
          }
        }
        return common_utils::EMPTY_STR;
      }

      if constexpr (std::is_same_v<std::vector<std::string>, T>) {
        T ldap_results{};
        if (const LDAPBerValPtr_t* bvalue = (*itr_begin)->
                                                    get_binary_value()) {
          while (bvalue && *bvalue) {
          ldap_results.push_back(std::string((*bvalue)->bv_val,
                                             (*bvalue)->bv_len));
          ++bvalue;
          }
        }
        else {
          if (const charPtr_t* value = (*itr_begin)->
                                                get_string_value()) {
            while (value && *value) {
              ldap_results.push_back(std::string(*value));
              ++value;
            }
          }
        }
        return ldap_results;
      }
    }
  }
  return T();
}

template<typename T>
T LDAPEntry::get_binary_value(
    const std::string& ldap_attribute) const
{

  static_assert(std::is_same_v<std::vector<char>, T> ||
                std::is_same_v<std::vector<std::vector<char>>, T>,
                "Not supported type!");

  if (!ldap_attribute.empty()) {
    auto itr_begin =
        std::find_if(begin(), end(),
                     [&] (const auto& elem) {
                         return (*itr_begin)->is_it_type(ldap_attribute);
                     }
        );

    if (*itr_begin && itr_begin != end()) {
      if constexpr (std::is_same_v<std::vector<char>, T>) {
        if (const LDAPBerValPtr_t* bvalue = (*itr_begin)->
            get_binary_value()) {
          if (bvalue && bvalue[0]) {
            return T(bvalue[0]->bv_val,
                     (bvalue[0]->bv_val + bvalue[0]->bv_len));
          }
          else {
            if (const charPtr_t* value = (*itr_begin)->
                get_string_value()) {
              if (value && value[0]) {
                return T(value[0],
                         (value[0] + std::strlen(value[0])));
              }
            }
          }
        }
        return common_utils::EMPTY_STR;
      }

      if constexpr (std::is_same_v<std::vector<std::vector<char>>, T>) {
        T ldap_results{};
        if (const LDAPBerValPtr_t* bvalue = (*itr_begin)->
            get_binary_value()) {
          while (bvalue && *bvalue) {
            ldap_results.push_back(std::vector<char>(
                (*bvalue)->bv_val,
                ((*bvalue)->bv_val + (*bvalue)->bv_len)));
            ++bvalue;
          }
        }
        else {
          if (const charPtr_t* value = (*itr_begin)->
              get_string_value()) {
            while (value && *value) {
              ldap_results.push_back(std::vector<char>(
                  *value, *value + std::strlen(*value)));
              ++value;
            }
          }
        }
        return ldap_results;
      }
    }
  }
  return T();
}

LDAPEntry::iterator LDAPEntry::ldap_entry_push_back(
    LDAPModificationsPtr_t ldap_modification_list)
{
  push_back(nullptr);
  auto itr(std::find(begin(), end(),
                     static_cast<LDAPModificationsPtr_t>(nullptr)));
  *itr = ldap_modification_list;
  return itr;
}


std::ostream& operator<< (std::ostream& out_stream,
                          const LDAPEntries_t& ldap_entries)
{
  std::copy(std::begin(ldap_entries),
            std::end(ldap_entries),
            std::ostream_iterator<LDAPEntry> (out_stream, NEWLINE));
  return out_stream;
}

std::ostream& operator<< (std::ostream& out_stream,
                          const LDAPEntry& ldap_entry)
{
  const std::string DN_STR("dn: ");

  if (ldap_entry.m_ldap_entry_dn.empty()) {
    return out_stream;
  }

  out_stream << DN_STR << ldap_entry.m_ldap_entry_dn << NEWLINE;
  for (auto itr_begin(ldap_entry.cbegin());
       itr_begin != ldap_entry.cend(); ++itr_begin) {
    if (*itr_begin) {
      out_stream << (**itr_begin);
    }
  }
  return out_stream;
}

std::ostream& operator<< (std::ostream& out_stream,
                          const LDAPModify& ldap_modification)
{
  if (ldap_modification.mod_bvalues) {
    if (ldap_modification.mod_op &
        static_cast<int>(ldap_utils::LDAPModifyOperation::BIN_VALUES)) {
      auto is_it_binary(std::strlen(ldap_modification.mod_type) >
                        std::strlen(BIN_CONTENT_STR.c_str()) &&
                        (&ldap_modification.mod_type[
                            std::strlen(ldap_modification.mod_type) -
                            std::strlen(BIN_CONTENT_STR.c_str())
                        ] == BIN_CONTENT_STR)
      );
      for (LDAPBerValPtr_t *bvalue = ldap_modification.mod_bvalues;
           bvalue && *bvalue; ++bvalue) {
        out_stream << ldap_modification.mod_type
                   << (common_utils::COLON + common_utils::BLANK);
        if (!is_it_binary) {
          charPtr_t itr_end((*bvalue)->bv_val + ((*bvalue)->bv_len - 1));
          is_it_binary = (std::find_if((*bvalue)->bv_val, itr_end,
                                       [&](const auto& elem) {
                                           /*TODO: Check this 20 for 1ST Printable */
                                           return (elem < common_utils::
                                                            FIRST_PRINTABLE_CHAR);
                                       }) != itr_end);
        }

        if (is_it_binary) {
          std::vector<char> base64_tmp(ldap_utils::base64_encode(
                                          (*bvalue)->bv_val,
                                          (*bvalue)->bv_len));
          std::copy(std::begin(base64_tmp),
                    std::end(base64_tmp),
                    std::ostream_iterator<char>(
                        out_stream, common_utils::EMPTY_STR.c_str()));
        }
        else {
          std::copy((*bvalue)->bv_val,
                    ((*bvalue)->bv_val + (*bvalue)->bv_len),
                    std::ostream_iterator<char>(
                        out_stream, common_utils::EMPTY_STR.c_str()));
          out_stream << NEWLINE;
        }
      }
    }
    else {
      for (charPtr_t* cvalue = ldap_modification.mod_values;
           cvalue && *cvalue; ++cvalue) {
        out_stream << ldap_modification.mod_type
                   << (common_utils::COLON + common_utils::BLANK)
                   << *cvalue << NEWLINE;
      }
    }
  }
  else {
    out_stream << ldap_modification.mod_type << common_utils::COLON << NEWLINE;
  }
  return out_stream;
}

}   //-- namespace ldap_client_auth

// ----------------------------- END-OF-FILE --------------------------------//
