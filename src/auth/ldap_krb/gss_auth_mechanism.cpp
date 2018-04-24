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

#include "gss_auth_mechanism.hpp"

#include <cstring>
#include <iostream>


namespace gss_client_auth {


GSSMechanismBase::
GSSMechanismBase(const std::string& gss_mechanism) { }

std::string GSSMechanismBase::get_string() const
{
  auto str_position(0);
  auto gss_str_size_limit((get_size() * 4) + 1);

  auto* char_buff = new char[gss_str_size_limit];
  for (auto itr(0ul); itr < m_gss_oid->length; ++itr) {
    str_position += std::sprintf(
        (char_buff + str_position),
        "%d.",
        ((unsigned char*) (m_gss_oid->elements))[itr]);
  }
  std::string str_tmp(char_buff,
                      static_cast<uint32_t>(str_position - 1));
  delete[] char_buff;
  return str_tmp;
}

GSSMechanismList::GSSMechanismList() : m_gss_oid_set(GSS_C_NO_OID_SET)
{
  OM_uint32 gss_minor_status;

  /*  OM_uint32 gss_create_empty_oid_set
   *    (OM_uint32 *minor_status, gss_OID_set *oid_set);
   *
   *  Create an object-identifier set containing no object identifiers, to
   *  which members may be subsequently added using the gss_add_oid_set_member()
   *  routine. These routines are intended to be used to construct sets of
   *  mechanism object identifiers, for input to gss_acquire_cred()
   */
  gss_create_empty_oid_set(&gss_minor_status, &m_gss_oid_set);
}

void GSSMechanismList::
gss_mechanism_add(const GSSMechanismBase& gss_mechanism)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_add_oid_set_member
   *    (OM_uint32 *minor_status, const gss_OID member_oid,
   *      gss_OID_set *oid_set);
   *
   *  Add an Object Identifier to an Object Identifier set. This routine is
   *  intended for use in conjunction with gss_create_empty_oid_set when
   *  constructing a set of mechanism OIDs for input to gss_acquire_cred.
   *  The oid_set parameter must refer to an OID-set that
   *  GNU Generic Security Service (GSS) API Reference Manual GSS-API
   *  (e.g. a set returned by gss_create_empty_oid_set())
   */
  gss_major_status =
      gss_add_oid_set_member(&gss_minor_status,
                             const_cast<GSSMechanismBase&>(gss_mechanism),
                             &m_gss_oid_set);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_add_oid_set_member() failed!");
  }
}

bool GSSMechanismList::
is_it_gss_mechanism_set(const GSSMechanismBase& gss_mechanism) const
{
  auto is_it_member(0);
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_test_oid_set_member
   *    (OM_uint32 *minor_status, const gss_OID member,
   *     const gss_OID_set set, int *present);
   *
   *  Interrogate an Object Identifier set to determine whether a specified
   *  Object Identifier is a member. This routine is intended to be used
   *  with OID sets returned by gss_indicate_mechs(), gss_acquire_cred(),
   *  and gss_inquire_cred(), but will also work with user-generated sets
   */
  gss_major_status =
      gss_test_oid_set_member(&gss_minor_status,
                              const_cast<GSSMechanismBase&>(gss_mechanism),
                              m_gss_oid_set,
                              &is_it_member);

  return ((gss_major_status == GSS_S_COMPLETE) && is_it_member);
}

const GSSMechanismBase GSSMechanismList::at(size_t idx) const
{
  if (idx >= m_gss_oid_set->count) {
    throw std::out_of_range(
        "ERROR: GSSMechanismList[] idx out of range!");
  }
  return ((*this)[idx]);
}

void GSSMechanismList::gss_mechanism_get()
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  gss_mechanism_clear();

  /*  OM_uint32 gss_indicate_mechs
   *    (OM_uint32 *minor_status, gss_OID_set *mech_set);
   *
   *  Allows an application to determine which underlying security
   *  mechanisms are available.
   */
  gss_major_status = gss_indicate_mechs(&gss_minor_status,
                                        &m_gss_oid_set);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_indicate_mechs() failed!");
  }
}

void GSSMechanismList::gss_mechanism_clear()
{
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_release_oid_set
   *    (OM_uint32 *minor_status, gss_OID_set *set);
   *
   *  Free storage associated with a GSSAPI-generated gss_OID_set object.
   */
  gss_release_oid_set(&gss_minor_status,
                      &m_gss_oid_set);
}


/*  GSSNameType Implementation.
*/

/*  MAX_NAME_OPTIONS must be updated with the proper number of options
 *  being used:
 *    static constexpr auto MAX_NAME_OPTIONS = 7;
*/
const std::array<gss_OID, GSSNameType::MAX_NAME_OPTIONS>
    GSSNameType::m_gss_oid_types {
  GSS_C_NO_OID,
  GSS_C_NT_USER_NAME,
  GSS_C_NT_MACHINE_UID_NAME,
  GSS_C_NT_STRING_UID_NAME,
  GSS_C_NT_HOSTBASED_SERVICE,
  GSS_C_NT_ANONYMOUS,
  GSS_C_NT_EXPORT_NAME,
};

GSSNameType::GSSNameType(const GSSNameType& gss_type)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_duplicate_name
   *    (OM_uint32 *minor_status, const gss_name_t src_name,
   *     gss_name_t *dest_name);
   *
   *  Create an exact duplicate of the existing internal name src_name.
   *  The new dest_name will be independent of src_name.
   *  src_name and dest_name must both be released, and the release
   *  of one shall not affect the validity of the other)
   */
  gss_major_status = gss_duplicate_name(&gss_minor_status,
                                        gss_type.m_gss_type_name,
                                        &m_gss_type_name);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_duplicate_name() failed!");
  }
}

GSSNameType::GSSNameType(const gss_name_t& gss_type)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  gss_major_status = gss_duplicate_name(&gss_minor_status,
                                        gss_type,
                                        &m_gss_type_name);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_duplicate_name() failed!");
  }
}

GSSNameType::
GSSNameType(const GSSDataBuffer& gss_buff,
            GSSNameOption gss_name_type)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_import_name
   *    (OM_uint32 *minor_status,
   *    const gss_buffer_t input_name_buffer,
   *    const gss_OID input_name_type,
   *    gss_name_t *output_name);
   *
   *  Convert a contiguous string name to internal form.
   */
  gss_major_status =
      gss_import_name(&gss_minor_status,
                      const_cast<GSSDataBuffer&>(gss_buff),
                      m_gss_oid_types[static_cast<int>(gss_name_type)],
                      &m_gss_type_name);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_import_name() failed!");
  }
}

GSSNameType::GSSNameType(const char* gss_name,
                         GSSNameOption gss_name_type) :
    m_gss_type_name(GSS_C_NO_NAME)
{
  GSSDataBuffer gss_buff(gss_name);
  GSSNameType gss_name_type_tmp(gss_buff, gss_name_type);
  swap(gss_name_type_tmp);
}

GSSNameType::GSSNameType(const std::string& gss_name,
                         GSSNameOption gss_name_type) :
    m_gss_type_name(GSS_C_NO_NAME)
{
  GSSDataBuffer gss_buff(gss_name);
  GSSNameType gss_name_type_tmp(gss_buff, gss_name_type);
  swap(gss_name_type_tmp);
}

void GSSNameType::gss_name_type_set(const GSSNameType& gss_type)
{
  GSSNameType gss_name_type_tmp(gss_type);
  swap(gss_name_type_tmp);
}

void GSSNameType::gss_name_type_set(const gss_name_t& gss_type)
{
  GSSNameType gss_name_type_tmp(gss_type);
  swap(gss_name_type_tmp);
}

void GSSNameType::
gss_name_type_set(const GSSDataBuffer& gss_buff,
                  GSSNameOption gss_name_type)
{
  GSSNameType gss_name_type_tmp(gss_buff, gss_name_type);
  swap(gss_name_type_tmp);
}

void GSSNameType::gss_name_type_set(const char* gss_name,
                                    GSSNameOption gss_name_type)
{
  GSSNameType gss_name_type_tmp(gss_name, gss_name_type);
  swap(gss_name_type_tmp);
}

void GSSNameType::gss_name_type_set(const std::string& gss_name,
                                    GSSNameOption gss_name_type)
{
  GSSNameType gss_name_type_tmp(gss_name, gss_name_type);
  swap(gss_name_type_tmp);
}

void GSSNameType::swap(GSSNameType& gss_type)
{
  std::swap(m_gss_type_name,
            gss_type.m_gss_type_name);
}

void GSSNameType::swap(gss_name_t& gss_type)
{
  std::swap(m_gss_type_name,
            gss_type);
}

void GSSNameType::gss_name_type_clear()
{
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_release_name
   *    (OM_uint32 *minor_status, gss_name_t *name);
   *
   *  Free GSSAPI-allocated storage associated with an
   *  internal-form name.
   */
  gss_release_name(&gss_minor_status, &m_gss_type_name);
}

std::string GSSNameType::get_string(GSSNameOption* gss_name_type) const
{
  GSSDataBuffer gss_buff;
  gss_OID gss_oid;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_display_name
   *    (OM_uint32 *minor_status, const gss_name_t input_name,
   *     gss_buffer_t output_name_buffer, gss_OID *output_name_type);
   *
   *  Allows an application to obtain a textual representation of an
   *  opaque internal-form name for display purposes.
   */
  gss_major_status = gss_display_name(&gss_minor_status,
                                      m_gss_type_name,
                                      gss_buff,
                                      &gss_oid);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_display_name() failed!");
  }

  if (gss_name_type) {
    for (const auto& elem : m_gss_oid_types) {
      //-- todo: this needs to be checked and validated.
      //std::cout << "debug: " << (elem) << " " << gss_oid << "\n";
      if (elem == gss_oid) {
        gss_name_type = reinterpret_cast<GSSNameOption*>(elem);
        break;
      }
    }
  }
  return gss_buff.get_string();
}

GSSNameType GSSNameType::
standardize_type_name(const GSSMechanismBase& gss_mechanism) const
{
  GSSNameType gss_out_name;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_canonicalize_name
   *    (OM_uint32 *minor_status,
   *     const gss_name_t input_name,
   *     const gss_OID mech_type,
   *     gss_name_t *output_name);
   *
   *  Generate a canonical mechanism name (MN) from an arbitrary
   *  internal name.
   */
  gss_major_status =
      gss_canonicalize_name(&gss_minor_status,
                            m_gss_type_name,
                            const_cast<GSSMechanismBase&>(gss_mechanism),
                            gss_out_name);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_canonicalize_name() failed!");
  }
  return gss_out_name;
}

GSSDataBuffer GSSNameType::
gss_name_export(const GSSMechanismBase& gss_mechanism) const
{
  GSSDataBuffer gss_buff;
  GSSNameType gss_name_type_tmp(standardize_type_name(gss_mechanism));
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_export_name
   *    (OM_uint32 *minor_status, const gss_name_t input_name,
   *     gss_buffer_t exported_name);
   *
   *  To produce a canonical contiguous string representation of
   *  a mechanism name (MN), suitable for direct comparison
   *  (e.g. with std::memcmp) for use in authorization functions
   *  (e.g. matching entries in an access-control list).
   */
  gss_major_status = gss_export_name(&gss_minor_status,
                                     gss_name_type_tmp,
                                     gss_buff);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_export_name() failed!");
  }
  return gss_buff;
}

void GSSNameType::
gss_name_import(const GSSDataBuffer& gss_buff,
                GSSNameOption& gss_name_type)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_import_name
   *    (OM_uint32 *minor_status,
   *    const gss_buffer_t input_name_buffer,
   *    const gss_OID input_name_type,
   *    gss_name_t *output_name);
   *
   *  Convert a contiguous string name to internal form. In general,
   *  the internal name returned (via the output_name parameter) will
   *  not be an MN; the exception to this is if the input_name_type
   *  indicates that the contiguous string provided via the
   *  input_name_buffer parameter is of type GSS_C_NT_EXPORT_NAME,
   *  in which case the returned internal name will be an MN for
   *  the mechanism that exported the name.
   */
  gss_major_status =
      gss_import_name(&gss_minor_status,
                      const_cast<GSSDataBuffer&>(gss_buff),
                      m_gss_oid_types[static_cast<int>(gss_name_type)],
                      &m_gss_type_name);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_import_name() failed!");
  }
}


/*  GSSSecCredential Implementation
*/
GSSSecCredential::GSSSecCredential(const GSSNameType& gss_name_type)
{
  /* TODO: We might need a different constructor in order to return (std::tuple):
   * - gss_OID_set *actual_mechs
   * - OM_uint32 *time_rec
  */
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_acquire_cred
   *    (OM_uint32 *minor_status,
   *    const gss_name_t desired_name,
   *    OM_uint32 time_req,
   *    const gss_OID_set desired_mechs,
   *    gss_cred_usage_t cred_usage,
   *    gss_cred_id_t *output_cred_handle,
   *    gss_OID_set *actual_mechs,
   *    OM_uint32 *time_rec);
   *
   *  Allows an application to acquire a handle for a pre-existing
   *  credential by name. GSS-API implementations must impose a local
   *  access-control policy on callers of this routine to prevent
   *  unauthorized callers from acquiring credentials to which they are
   *  not entitled.
   */
  gss_major_status =
      gss_acquire_cred(&gss_minor_status,
                       gss_name_type,
                       GSS_C_INDEFINITE,
                       0,
                       0,
                       &m_gss_credential,
                       0,
                       0);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_acquire_cred() failed!");
  }
}

void GSSSecCredential::gss_credential_clear()
{
  OM_uint32 gss_minor_status;

  /*  OM_uint32 gss_release_cred
   *    (OM_uint32 *minor_status, gss_cred_id_t *cred_handle);
   *
   *  Informs GSS-API that the specified credential handle is no longer
   *  required by the application, and frees associated resources.
   */
  gss_release_cred(&gss_minor_status, &m_gss_credential);
}


/*  GSSSecurityCtx Implementation.
*/
GSSSecurityCtx::
GSSSecurityCtx(const GSSDataBuffer& gss_buff)
{
  gss_ctx_import(gss_buff);
}

void GSSSecurityCtx::gss_ctx_clear()
{
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_delete_sec_context
   *    (OM_uint32 *minor_status, gss_ctx_id_t *context_handle,
   *    gss_buffer_t output_token);
   *
   *  Delete a security context. gss_delete_sec_context will delete
   *  the local data structures associated with the specified security
   *  context, and may generate an output_token, which when passed to
   *  the peer gss_process_context_token will instruct it to do
   *  likewise.
   */
  gss_delete_sec_context(&gss_minor_status, &m_gss_ctx, GSS_C_NO_BUFFER);
}

bool GSSSecurityCtx::
gss_initiate_ctx(GSSDataBuffer& gss_buff_in,
                 const GSSNameType& gss_name_type,
                 const GSSSecurityCtxFlag& gss_ctx_flag,
                 const GSSSecCredential& gss_credential,
                 GSSMechanismBase gss_mechanism,
                 OM_uint32 gss_required_time,
                 gss_channel_bindings_t gss_channel_bindings)
{
  GSSDataBuffer gss_buff_recv_tok;
  GSSDataBuffer gss_buff_send_tok;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);
  OM_uint32 gss_ctx_flag_result(0);

  /*  OM_uint32 gss_init_sec_context
   *    (OM_uint32 *minor_status,
   *    const gss_cred_id_t initiator_cred_handle,
   *    gss_ctx_id_t *context_handle,
   *    const gss_name_t target_name,
   *    const gss_OID mech_type,
   *    OM_uint32 req_flags,
   *    OM_uint32 time_req,
   *    const gss_channel_bindings_t input_chan_bindings,
   *    const gss_buffer_t input_token,
   *    gss_OID *actual_mech_type,
   *    gss_buffer_t output_token,
   *    OM_uint32 *ret_flags,
   *    OM_uint32 *time_rec);
   *
   *  Initiates the establishment of a security context between the
   *  application and a remote peer. Initially, the input_token parameter
   *  should be specified either as GSS_C_NO_BUFFER, or as a pointer to a
   *  gss_buffer_desc object whose length field contains the value zero.
   */
  gss_major_status =
      gss_init_sec_context(&gss_minor_status,
                           const_cast<GSSSecCredential&>(gss_credential),
                           &m_gss_ctx,
                           gss_name_type,
                           gss_mechanism,
                           gss_ctx_flag,
                           gss_required_time,
                           gss_channel_bindings,
                           gss_buff_in,
                           0,
                           gss_buff_send_tok,
                           &gss_ctx_flag_result,
                           0);

  if (!is_it_valid_ctx() ||
      (gss_major_status != GSS_S_COMPLETE &&
       gss_major_status != GSS_S_CONTINUE_NEEDED)) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_init_sec_context() failed!");
  }
  gss_buff_in.swap(gss_buff_send_tok);
  return (gss_major_status == GSS_S_CONTINUE_NEEDED);
}

bool GSSSecurityCtx::
gss_allow_ctx(GSSDataBuffer& gss_buff_in,
              const GSSSecCredential& gss_credential,
              gss_channel_bindings_t gss_channel_bindings)
{
  GSSDataBuffer gss_buff_out;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);
  //OM_uint32 gss_ctx_flag_result(0);

  /*  OM_uint32 gss_accept_sec_context
   *    (OM_uint32 *minor_status,
   *    gss_ctx_id_t *context_handle,
   *    const gss_cred_id_t acceptor_cred_handle,
   *    const gss_buffer_t input_token_buffer,
   *    const gss_channel_bindings_t input_chan_bindings,
   *    gss_name_t *src_name,
   *    gss_OID *mech_type,
   *    gss_buffer_t output_token,
   *    OM_uint32 *ret_flags,
   *    OM_uint32 *time_rec,
   *    gss_cred_id_t *delegated_cred_handle);
   *
   *  Allows a remotely initiated security context between the
   *  application and a remote peer to be established. The routine
   *  may return a output_token which should be transferred to the
   *  peer application, where the peer application will present it
   *  to gss_init_sec_context().
   */
  gss_major_status =
      gss_accept_sec_context(&gss_minor_status,
                             &m_gss_ctx,
                             const_cast<GSSSecCredential&>(gss_credential),
                             gss_buff_in,
                             gss_channel_bindings,
                             0,
                             0,
                             gss_buff_out,
                             0,
                             0,
                             0);

  if (!is_it_valid_ctx() ||
      (gss_major_status != GSS_S_COMPLETE &&
       gss_major_status != GSS_S_CONTINUE_NEEDED)) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_accept_sec_context() failed!");
  }
  gss_buff_in.swap(gss_buff_out);
  return (gss_major_status == GSS_S_CONTINUE_NEEDED);
}

void GSSSecurityCtx::gss_ctx_import(const GSSDataBuffer& gss_buff)
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_import_sec_context
   *    (OM_uint32 *minor_status,
   *    const gss_buffer_t interprocess_token,
   *    gss_ctx_id_t *context_handle);
   *
   *  Allows a process to import a security context established by
   *  another process. A given interprocess token may be imported
   *  only once.
   */
  gss_major_status =
      gss_import_sec_context(&gss_minor_status,
                             const_cast<GSSDataBuffer&>(gss_buff),
                             &m_gss_ctx);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_import_sec_context() failed!");
  }
}

GSSDataBuffer GSSSecurityCtx::gss_ctx_export()
{
  GSSDataBuffer gss_buff_export;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_export_sec_context
   *    (OM_uint32 *minor_status,
   *    gss_ctx_id_t *context_handle,
   *    gss_buffer_t interprocess_token);
   *
   *  Provided to support the sharing of work between multiple processes.
   *  This routine will typically be used by the context-acceptor, in an
   *  application where a single process receives incoming connection
   *  requests and accepts security contexts over them, then passes the
   *  established context to one or more other processes for message
   *  exchange.
   */
  gss_export_sec_context(&gss_minor_status, &m_gss_ctx, gss_buff_export);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_export_sec_context() failed");
  }
  return gss_buff_export;
}

GSSDataBuffer
GSSSecurityCtx::get_gss_ctx_mic(const GSSDataBuffer& gss_buff_msg,
                                gss_qop_t gss_qop) const
{
  GSSDataBuffer gss_mic;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_get_mic
   *    (OM_uint32 *minor_status,
   *     const gss_ctx_id_t context_handle,
   *     gss_qop_t qop_req,
   *     const gss_buffer_t message_buffer,
   *     gss_buffer_t message_token);
   *
   *  Generates a cryptographic MIC for the supplied message, and places
   *  the MIC in a token for transfer to the peer application.
   *  The qop_req parameter allows a choice between several
   *  cryptographic algorithms, if supported by the chosen mechanism.
   */
  gss_major_status = gss_get_mic(&gss_minor_status,
                                 m_gss_ctx,
                                 gss_qop,
                                 const_cast<GSSDataBuffer&>(gss_buff_msg),
                                 gss_mic);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_get_mic() failed!");
  }
  return gss_mic;
}

bool GSSSecurityCtx::gss_ctx_mic_check(const GSSDataBuffer& gss_buff_msg,
                                       const GSSDataBuffer& gss_buff_mic,
                                       gss_qop_t* qop_state) const
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_verify_mic
   *    (OM_uint32 *minor_status,
   *    const gss_ctx_id_t context_handle,
   *    const gss_buffer_t message_buffer,
   *    const gss_buffer_t token_buffer,
   *    gss_qop_t *qop_state);
   *
   *  Verifies that a cryptographic MIC, contained in the token
   *  parameter, fits the supplied message. The qop_state parameter
   *  allows a message recipient to determine the strength of protection
   *  that was applied to the message.
   */
  gss_major_status = gss_verify_mic(&gss_minor_status,
                                    m_gss_ctx,
                                    const_cast<GSSDataBuffer&>(gss_buff_msg),
                                    const_cast<GSSDataBuffer&>(gss_buff_mic),
                                    qop_state);

  if ((gss_major_status == GSS_S_COMPLETE) ||
      (gss_major_status == GSS_S_BAD_SIG)) {
    return (gss_major_status == GSS_S_COMPLETE);
  }
  throw gss_utils::GSSExceptionHandler(
      gss_major_status,
      gss_minor_status,
      "ERROR: gss_verify_mic() failed!");
}

GSSDataBuffer
GSSSecurityCtx::gss_msg_wrap(const GSSDataBuffer& gss_buff_msg,
                             bool flag_encryption,
                             gss_qop_t qop) const
{
  return gss_msg_wrap(*this, gss_buff_msg, flag_encryption, qop);
}

GSSDataBuffer
GSSSecurityCtx::gss_msg_wrap(const GSSSecurityCtx& gss_ctx,
                             const GSSDataBuffer& gss_buff_msg,
                             bool flag_encryption,
                             gss_qop_t qop)
{
  GSSDataBuffer gss_buff_wrapped;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_wrap
   *    (OM_uint32 *minor_status,
   *    const gss_ctx_id_t context_handle,
   *    int conf_req_flag,
   *    gss_qop_t qop_req,
   *    const gss_buffer_t input_message_buffer,
   *    int *conf_state,
   *    gss_buffer_t output_message_buffer);
   *
   *  Attaches a cryptographic MIC and optionally encrypts the specified
   *  input_message. The output_message contains both the MIC and the
   *  message.
   */
  gss_major_status = gss_wrap(&gss_minor_status,
                              gss_ctx,
                              flag_encryption,
                              qop,
                              const_cast<GSSDataBuffer&>(gss_buff_msg),
                              0,
                              gss_buff_wrapped);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_wrap() failed!");
  }
  return gss_buff_wrapped;
}

void GSSSecurityCtx::gss_msg_wrap_in_place(GSSDataBuffer& gss_buff_msg,
                                           bool flag_encryption,
                                           gss_qop_t qop) const
{
  gss_msg_wrap_in_place(*this, gss_buff_msg, flag_encryption, qop);
}

void GSSSecurityCtx::gss_msg_wrap_in_place(const GSSSecurityCtx& gss_ctx,
                                           GSSDataBuffer& gss_buff_msg,
                                           bool flag_encryption,
                                           gss_qop_t qop)
{
  GSSDataBuffer gss_buff_tmp(gss_msg_wrap(gss_ctx, gss_buff_msg,
                                          flag_encryption,
                                          qop));
  gss_buff_msg.swap(gss_buff_tmp);
}

GSSDataBuffer
GSSSecurityCtx::gss_msg_unwrap(const GSSDataBuffer& gss_buff_wrapped_msg,
                               bool* flag_confidential,
                               gss_qop_t* qop_state) const
{
  return gss_msg_unwrap(*this, gss_buff_wrapped_msg,
                        flag_confidential, qop_state);
}

GSSDataBuffer
GSSSecurityCtx::gss_msg_unwrap(const GSSSecurityCtx& gss_ctx,
                               const GSSDataBuffer& gss_buff_msg,
                               bool* flag_confidential,
                               gss_qop_t* qop_state)
{
  GSSDataBuffer gss_buff_unwrapped;
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);
  auto confidentiality_result(0);

  /*  OM_uint32 gss_unwrap
   *    (OM_uint32 *minor_status,
   *    const gss_ctx_id_t context_handle,
   *    const gss_buffer_t input_message_buffer,
   *    gss_buffer_t output_message_buffer,
   *    int *conf_state,
   *    gss_qop_t *qop_state);
   *
   *  Converts a message previously protected by gss_wrap back to a
   *  usable form, verifying the embedded MIC.
   */
  gss_major_status = gss_unwrap(&gss_minor_status,
                                gss_ctx,
                                const_cast<GSSDataBuffer&>(gss_buff_msg),
                                gss_buff_unwrapped,
                                &confidentiality_result,
                                qop_state);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_unwrap() failed!");
  }
  if (flag_confidential) {
    *flag_confidential = static_cast<bool>(confidentiality_result);
  }
  return gss_buff_unwrapped;
}

void GSSSecurityCtx::
gss_msg_unwrap_in_place(GSSDataBuffer& gss_buff_wrapped_msg,
                        bool* flag_confidential,
                        gss_qop_t* qop_state) const
{
  gss_msg_unwrap_in_place(*this, gss_buff_wrapped_msg,
                          flag_confidential, qop_state);
}

void GSSSecurityCtx::
gss_msg_unwrap_in_place(const GSSSecurityCtx& gss_ctx,
                        GSSDataBuffer& gss_buff_msg,
                        bool* flag_confidential,
                        gss_qop_t* qop_state)
{
  GSSDataBuffer gss_buff_tmp(gss_msg_unwrap(gss_ctx,
                                            gss_buff_msg,
                                            flag_confidential,
                                            qop_state));
  gss_buff_msg.swap(gss_buff_tmp);
}

size_t GSSSecurityCtx::gss_msg_wrap_size_limit(size_t gss_msg_max_size,
                                               bool flag_encryption,
                                               gss_qop_t qop) const
{
  OM_uint32 gss_major_status(0);
  OM_uint32 gss_minor_status(0);
  OM_uint32 gss_size_limit(0);

  /*  OM_uint32 gss_wrap_size_limit
   *    (OM_uint32 *minor_status,
   *    const gss_ctx_id_t context_handle,
   *    int conf_req_flag,
   *    gss_qop_t qop_req,
   *    OM_uint32 req_output_size,
   *    OM_uint32 *max_input_size);
   *
   *  Allows an application to determine the maximum message size that,
   *  if presented to gss_wrap with the same conf_req_flag and qop_req
   *  parameters, will result in an output token containing no more
   *  than req_output_size bytes.
   *
   *  This call is intended for use by applications that communicate
   *  over protocols that impose a maximum message size. It enables
   *  the application to fragment messages prior to applying protection.
   */
  gss_major_status = gss_wrap_size_limit(&gss_minor_status,
                                         m_gss_ctx,
                                         flag_encryption,
                                         qop,
                                         gss_msg_max_size,
                                         &gss_size_limit);

  if (gss_major_status != GSS_S_COMPLETE) {
    throw gss_utils::GSSExceptionHandler(
        gss_major_status,
        gss_minor_status,
        "ERROR: gss_wrap_size_limit() failed!");
  }
  return gss_size_limit;
}


/*  gss_client_auth::GSSDataBuffer Implementation.
*/
GSSDataBuffer::GSSDataBuffer(const GSSDataBuffer& gss_buff)
{
  m_gss_buffer.length	= gss_buff.m_gss_buffer.length;
  m_gss_buffer.value 	= std::malloc(m_gss_buffer.length);
  std::memcpy(m_gss_buffer.value,
              gss_buff.m_gss_buffer.value,
              m_gss_buffer.length);
}

GSSDataBuffer::GSSDataBuffer(const gss_buffer_desc& gss_buff)
{
  m_gss_buffer.length = gss_buff.length;
  m_gss_buffer.value  = std::malloc(m_gss_buffer.length);
  std::memcpy(m_gss_buffer.value,
              gss_buff.value,
              m_gss_buffer.length);
}

GSSDataBuffer::GSSDataBuffer(size_t gss_buff_size)
{
  m_gss_buffer.length = gss_buff_size;
  m_gss_buffer.value  = std::malloc(m_gss_buffer.length);
}

GSSDataBuffer::GSSDataBuffer(const char* gss_str)
{
  m_gss_buffer.length = std::strlen(gss_str);
  m_gss_buffer.value  = std::malloc( m_gss_buffer.length );
  std::memcpy((char*) m_gss_buffer.value,
              gss_str, m_gss_buffer.length);
}

GSSDataBuffer::GSSDataBuffer(const void* gss_str, size_t gss_buff_size)
{
  m_gss_buffer.length = gss_buff_size;
  m_gss_buffer.value  = std::malloc(m_gss_buffer.length);
  std::memcpy(m_gss_buffer.value,
              gss_str,
              m_gss_buffer.length);
}

GSSDataBuffer::GSSDataBuffer(const std::string& gss_str)
{
  m_gss_buffer.length = gss_str.size();
  m_gss_buffer.value  = std::malloc(m_gss_buffer.length);
  std::memcpy(m_gss_buffer.value,
              gss_str.c_str(),
              m_gss_buffer.length);
}

GSSDataBuffer::GSSDataBuffer(std::istream& in_stream, size_t in_size)
{
  m_gss_buffer.length = in_size;
  m_gss_buffer.value  = std::malloc( m_gss_buffer.length );
  in_stream.read((char*) m_gss_buffer.value, in_size);
}

void GSSDataBuffer::gss_buff_set(const GSSDataBuffer& gss_buff)
{
  GSSDataBuffer gss_buff_tmp(gss_buff);
  swap(gss_buff_tmp);
}

void GSSDataBuffer::gss_buff_set(const gss_buffer_desc& gss_buff)
{
  GSSDataBuffer gss_buff_tmp(gss_buff);
  swap(gss_buff_tmp);
}

void GSSDataBuffer::gss_buff_set(const char* gss_buff)
{
  GSSDataBuffer gss_buff_tmp(gss_buff);
  swap(gss_buff_tmp);
}

void GSSDataBuffer::gss_buff_set(void* gss_buff, size_t gss_buff_size)
{
  GSSDataBuffer gss_buff_tmp(gss_buff, gss_buff_size);
  swap(gss_buff_tmp);
}

void GSSDataBuffer::gss_buff_set(const std::string& gss_buff)
{
  GSSDataBuffer gss_buff_tmp(gss_buff);
  swap(gss_buff_tmp);
}

void GSSDataBuffer::gss_buff_set(std::istream& in_stream, size_t in_size)
{
  GSSDataBuffer gss_buff_tmp(in_stream, in_size);
  swap(gss_buff_tmp);
}

void GSSDataBuffer::swap(GSSDataBuffer& gss_buff)
{
  std::swap(m_gss_buffer,
            gss_buff.m_gss_buffer);
}

void GSSDataBuffer::swap(gss_buffer_desc& gss_buff)
{
  std::swap(m_gss_buffer,
            gss_buff);
}

void GSSDataBuffer::gss_buff_clear()
{
  OM_uint32 gss_minor_status(0);

  /*  OM_uint32 gss_release_buffer
   *    (OM_uint32 *minor_status,
   *    gss_buffer_t buffer);
   *
   *  Free storage associated with a buffer. The storage must have been
   *  allocated by a GSS-API routine
   */
  gss_release_buffer(&gss_minor_status, &m_gss_buffer);
}

void GSSDataBuffer::gss_buff_resize(size_t gss_buff_size)
{
  GSSDataBuffer gss_buff(gss_buff_size);

  std::memcpy(gss_buff.m_gss_buffer.value,
              m_gss_buffer.value,
              std::min(gss_buff_size, m_gss_buffer.length));
  swap(gss_buff);
}

}   //-- namespace gss_client_auth


/*  GSSMechanism Operator Implementation.
*/
std::ostream&
operator<< (std::ostream& out_stream,
            const gss_client_auth::GSSMechanismBase& gss_mechanism)
{
  out_stream << gss_mechanism.get_string();
  return out_stream;
}

const gss_client_auth::GSSMechanismBase
gss_client_auth::GSSMechanismList::operator[] (size_t idx) const
{
  return (m_gss_oid_set->elements + idx);
}

gss_client_auth::GSSMechanismList&
gss_client_auth::GSSMechanismList::
operator+= (const GSSMechanismBase& gss_mechanism)
{
  gss_mechanism_add(gss_mechanism);
  return (*this);
}


/*  GSSNameType Operator Implementation.
*/
bool gss_client_auth::GSSNameType::
operator== (const gss_client_auth::GSSNameType& lhs) const
{
  auto is_same_name(0);
  OM_uint32 gss_major_status(0);

  /*  OM_uint32 gss_compare_name
   *    (OM_uint32 *minor_status,
   *    const gss_name_t name1,
   *    const gss_name_t name2,
   *    int *name_equal);
   *
   *  Allows an application to compare two internal-form names to
   *  determine whether they refer to the same entity.
   *
   *  If either name presented to gss_compare_name denotes an anonymous
   *  principal, the routines should indicate that the two names do not
   *  refer to the same identity
   */
  gss_major_status = gss_compare_name(0,
                                      *(const_cast<GSSNameType*>(this)),
                                      const_cast<GSSNameType&>(lhs),
                                      &is_same_name);

  return ((gss_major_status == GSS_S_COMPLETE) && is_same_name);
}

bool gss_client_auth::GSSNameType::
operator== (const std::string& lhs) const
{
  try {
    GSSNameType other(lhs);
    return (*this == other);
  }
    /*  Unreachable code, but required for try block.
     *  We use it so GSS Names are not allowed to be imported by
     *  get_string(), so they are treated as 'different' names.
     */
  catch (gss_utils::GSSExceptionHandler& except) {
    return false;
  }
}

bool gss_client_auth::GSSNameType::
operator!= (const GSSNameType& lhs) const
{ return !(*this == lhs); }

bool gss_client_auth::GSSNameType::
operator!= (const std::string& lhs) const
{ return !(*this == lhs); }

bool operator== (const std::string& lhs,
                 const gss_client_auth::GSSNameType& rhs)
{ return (rhs == lhs); }

bool operator!= (const std::string& lhs,
                 const gss_client_auth::GSSNameType& rhs)
{ return !(rhs == lhs); }

std::ostream&
operator<< (std::ostream& out_stream,
            const gss_client_auth::GSSNameType& gss_type)
{
  out_stream << gss_type.get_string();
  return out_stream;
}


/*  GSSDataBuffer Operator Implementation.
*/
gss_client_auth::GSSDataBuffer& gss_client_auth::GSSDataBuffer::
operator+ (const gss_client_auth::GSSDataBuffer& gss_buff)
{
  gss_client_auth::GSSDataBuffer gss_buff_tmp(*this);

  gss_buff_tmp.gss_buff_resize(gss_buff_tmp.gss_buff_getsize() +
                               gss_buff.gss_buff_getsize());
  std::memcpy(reinterpret_cast<char*>(
                  gss_buff_tmp.m_gss_buffer.value +
                  gss_client_auth::GSSDataBuffer:: gss_buff_getsize()
              ), gss_buff.m_gss_buffer.value,
                 gss_buff.gss_buff_getsize()
  );
  gss_client_auth::GSSDataBuffer::swap(gss_buff_tmp);
  return (*this);
}

gss_client_auth::GSSDataBuffer& gss_client_auth::GSSDataBuffer::
operator+= (const gss_client_auth::GSSDataBuffer& gss_buff)
{
  GSSDataBuffer gss_buff_tmp(*this + gss_buff);
  swap(gss_buff_tmp);
  return (*this);
}

bool operator== (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{
  return (lhs.gss_buff_getsize() == rhs.gss_buff_getsize() &&
          !std::memcmp(lhs.get_bytes(),
                       rhs.get_bytes(),
                       lhs.gss_buff_getsize()));
}

bool operator== (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_buffer_desc& rhs)
{
  return (lhs.gss_buff_getsize() == rhs.length &&
          !std::memcmp(lhs.get_bytes(),
                       rhs.value,
                       lhs.gss_buff_getsize()));
}

bool operator== (const gss_buffer_desc& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{
  return (rhs == lhs);
}

bool operator< (const gss_client_auth::GSSDataBuffer& lhs,
                const gss_client_auth::GSSDataBuffer& rhs)
{
  return (lhs.gss_buff_getsize() < rhs.gss_buff_getsize() ||
          (lhs.gss_buff_getsize() == rhs.gss_buff_getsize() &&
           0 > std::memcmp(lhs.get_bytes(),
                           rhs.get_bytes(),
                           lhs.gss_buff_getsize())));
}

bool operator< (const gss_client_auth::GSSDataBuffer& lhs,
                const gss_buffer_desc& rhs)
{
  return (lhs.gss_buff_getsize() < rhs.length ||
          (lhs.gss_buff_getsize() == rhs.length &&
           0 > std::memcmp(lhs.get_bytes(),
                           rhs.value,
                           lhs.gss_buff_getsize())));
}

bool operator< (const gss_buffer_desc& lhs,
                const gss_client_auth::GSSDataBuffer& rhs)
{ return (rhs > lhs); }

bool operator!= (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{ return !(lhs == rhs); }

bool operator!= (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_buffer_desc& rhs)
{ return !(lhs == rhs); }

bool operator!= (const gss_buffer_desc& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{ return (rhs != lhs); }

bool operator> (const gss_client_auth::GSSDataBuffer& lhs,
                const gss_client_auth::GSSDataBuffer& rhs)
{ return !(lhs < rhs) && !(lhs == rhs); }

bool operator> (const gss_client_auth::GSSDataBuffer& lhs,
                const gss_buffer_desc& rhs)
{ return !(lhs < rhs) && !(lhs == rhs); }

bool operator> (const gss_buffer_desc& lhs,
                const gss_client_auth::GSSDataBuffer& rhs)
{ return (rhs < lhs); }

bool operator<= (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{ return (lhs < rhs) || (lhs == rhs); }

bool operator<= (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_buffer_desc& rhs)
{ return (lhs < rhs) || (lhs == rhs); }

bool operator<= (const gss_buffer_desc& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{ return (rhs >= lhs); }

bool operator>= (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{ return (lhs > rhs) || (lhs == rhs); }

bool operator>= (const gss_client_auth::GSSDataBuffer& lhs,
                 const gss_buffer_desc& rhs)
{ return (lhs > rhs) || (lhs == rhs); }

bool operator>= (const gss_buffer_desc& lhs,
                 const gss_client_auth::GSSDataBuffer& rhs)
{ return (rhs <= lhs); }

std::ostream& operator<< (std::ostream& out_stream,
                          const gss_client_auth::GSSDataBuffer& gss_buff)
{
  out_stream.write(gss_buff.get_bytes(),
                   gss_buff.gss_buff_getsize());
  return out_stream;
}

std::istream& operator>> (std::istream& in_stream,
                          gss_client_auth::GSSDataBuffer& gss_buff)
{
  std::string gss_str_buff;
  in_stream >> gss_str_buff;
  gss_buff.gss_buff_set(gss_str_buff);
  return in_stream;
}


// ----------------------------- END-OF-FILE --------------------------------//
