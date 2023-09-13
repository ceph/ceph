// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_ESCAPE_H
#define CEPH_RGW_ESCAPE_H

#include <ostream>
#include <string_view>

/* Returns the length of a buffer that would be needed to escape 'buf'
 * as an XML attribute
 */
size_t escape_xml_attr_len(const char *buf);

/* Escapes 'buf' as an XML attribute. Assumes that 'out' is at least long
 * enough to fit the output. You can find out the required length by calling
 * escape_xml_attr_len first.
 */
void escape_xml_attr(const char *buf, char *out);

/* Returns the length of a buffer that would be needed to escape 'buf'
 * as an JSON attribute
 */
size_t escape_json_attr_len(const char *buf, size_t src_len);

/* Escapes 'buf' as an JSON attribute. Assumes that 'out' is at least long
 * enough to fit the output. You can find out the required length by calling
 * escape_json_attr_len first.
 */
void escape_json_attr(const char *buf, size_t src_len, char *out);

/* Note: we escape control characters. Although the XML spec doesn't actually
 * require this, Amazon does it in their XML responses.
 */

// stream output operators that write escaped text without making a copy
// usage:
//   std::string xml_input = ...;
//   std::cout << xml_stream_escaper(xml_input) << std::endl;

struct xml_stream_escaper {
  std::string_view str;
  xml_stream_escaper(std::string_view str) : str(str.data(), str.size()) {}
};
std::ostream& operator<<(std::ostream& out, const xml_stream_escaper& e);

struct json_stream_escaper {
  std::string_view str;
  json_stream_escaper(std::string_view str) : str(str.data(), str.size()) {}
};
std::ostream& operator<<(std::ostream& out, const json_stream_escaper& e);

#endif
