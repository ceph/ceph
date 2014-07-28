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

/**
 * Returns the value of **key** in **str_map** if available.
 *
 * If **key** is not available in **str_map**, and if **def_val** is
 * not-NULL then returns **def_val**. Otherwise checks if the value of
 * **key** is an empty string and if so will return **key**.
 * If the map contains **key**, the function returns the value of **key**.
 *
 * @param[in] str_map Map to obtain **key** from
 * @param[in] key The key to search for in the map
 * @param[in] def_val The value to return in case **key** is not present
 */
extern std::string get_str_map_value(
    const std::map<std::string,std::string> &str_map,
    const std::string &key,
    const std::string *def_val = NULL);

/**
 * Returns the value of **key** in **str_map** if available.
 *
 * If **key** is available in **str_map** returns the value of **key**.
 *
 * If **key** is not available in **str_map**, and if **def_key**
 * is not-NULL and available in **str_map**, then returns the value
 * of **def_key**.
 *
 * Otherwise returns an empty string.
 *
 * @param[in] str_map Map to obtain **key** or **def_key** from
 * @param[in] key Key to obtain the value of from **str_map**
 * @param[in] def_key Key to fallback to if **key** is not present
 *                    in **str_map**
 */
extern std::string get_str_map_key(
    const std::map<std::string,std::string> &str_map,
    const std::string &key,
    const std::string *fallback_key = NULL);

#endif
