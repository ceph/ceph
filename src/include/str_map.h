// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_STRMAP_H
#define CEPH_STRMAP_H

#include <map>
#include <string>
#include <sstream>

/**
 * Parse **str** and set **str_map** with the key/value pairs read
 * from it. The format of **str** is either a well formed JSON object
 * or a custom key[=value] plain text format.
 *
 * JSON is tried first. If successfully parsed into a JSON object, it
 * is copied into **str_map** verbatim. If it is not a JSON object ( a
 * string, integer etc. ), -EINVAL is returned and **ss** is set to
 * a human readable error message.
 *
 * If **str** is no valid JSON, it is assumed to be a string
 * containing white space separated key=value pairs. A white space is
 * either space, tab or newline. The value is optional, in which case
 * it defaults to an empty string. For example:
 * 
 *     insert your own=political    statement=here 
 *
 * will be parsed into:
 *
 *     { "insert": "", 
 *       "your": "", 
 *       "own": "policital",
 *       "statement": "here" }
 *
 * Returns 0 on success.
 *
 * @param [in] str JSON or plain text key/value pairs
 * @param [out] ss human readable message on error
 * @param [out] str_map key/value pairs read from str
 * @return **0** on success or a -EINVAL on error.
 */
extern int get_str_map(const std::string &str,
		       std::ostream &ss,
		       std::map<std::string,std::string> *str_map);

#endif
