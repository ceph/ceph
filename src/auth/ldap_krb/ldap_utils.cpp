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

#include "ldap_utils.hpp"


/*
 * A BIO is an I/O stream abstraction; essentially OpenSSL's answer to the
 * C library's FILE *. OpenSSL comes with a number of useful BIO types
 * predefined, or you can create your own.
 *
 *  BIOs come in two flavors: source/sink, or filter. BIOs can be chained
 *  together. Each chain always has exactly one source/sink, but can have
 *  any number (zero or more) of filters.
 *
 * Good References:
 *  - http://www.herongyang.com/Cryptography/
 *  - https://www.ibm.com/developerworks/linux/library/l-openssl/index.html
 *  - https://wiki.openssl.org/index.php/BIO
 *
 *
 */

namespace ldap_utils {

std::vector<char> base64_encode(const char* charPtr, const size_t size) {
  BIO* base64_tmp   = BIO_new(BIO_f_base64());
  BIO* base_mem_tmp = BIO_new(BIO_s_mem());
  base64_tmp        = BIO_push(base64_tmp, base_mem_tmp);

  BIO_write(base64_tmp, charPtr, size);
  BIO_flush(base64_tmp);

  char* base_ptr_tmp     = nullptr;
  auto base_ptr_tmp_size = BIO_get_mem_data(base64_tmp, &base_ptr_tmp);
  std::vector<char> ldap_result((base_ptr_tmp_size + 1), common_utils::ZERO);
  std::copy(base_ptr_tmp,
            (base_ptr_tmp + base_ptr_tmp_size),
            std::begin(ldap_result));
  BIO_free_all(base64_tmp);

  return ldap_result;
}

auto base64_encode(const std::string& ldap_value) {
  return base64_encode(ldap_value.c_str(), ldap_value.size());
}

std::vector<char> base64_decode(const char* charPtr, const size_t size) {
  BIO* base64_tmp   = BIO_new(BIO_f_base64());
  BIO* base_mem_tmp = BIO_new_mem_buf(const_cast<char*>(charPtr), size);
  base_mem_tmp      = BIO_push(base64_tmp, base_mem_tmp);

  std::vector<char> ldap_result(size, 0);
  BIO_read(base_mem_tmp, &ldap_result[0], size);
  ldap_result.resize(
      std::distance(ldap_result.begin(),
                    std::find_if(ldap_result.begin(),
                                 ldap_result.end(),
                                 [&] (const auto& elem) {
                                    return elem == common_utils::ZERO;
                                 }
                    )
      )
  );

  BIO_free_all(base_mem_tmp);
  return ldap_result;
}

auto base64_decode(const std::string& ldap_value) {
  return base64_decode(ldap_value.c_str(), ldap_value.size());
}


/*  LDAPExceptionHandler Implementation.
*/
LDAPExceptionHandler::
LDAPExceptionHandler(LDAPPtr_t ldap_connection_handler,
                    int ldap_status,
                    const char* ldap_func) throw() :
    m_ldap_connection_handler(ldap_connection_handler),
    m_ldap_status(ldap_status)
{
  /*
  std::strncpy(m_ldap_func, ldap_func, (sizeof(m_ldap_func) - 1));
  m_ldap_func[(sizeof(m_ldap_func) - 1)] = 0;

  try {
    show_msg_helper(ldap_status, LDAP_ERR_STATUS,
        m_ldap_msg, sizeof(m_ldap_msg));
  }
  catch (LDAPExceptionHandler& except) {
    std::strcpy(m_ldap_msg, except.m_ldap_msg);
  }
  */
}

const char* LDAPExceptionHandler::what() const throw()
{
  /*
  if (!m_ldap_connection_handler) {
    return;
  }
  return m_ldap_status;
  */
  return std::string().c_str();
}

}   //-- namespace ldap_utils

// ----------------------------- END-OF-FILE --------------------------------//
