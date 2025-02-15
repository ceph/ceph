// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2009 Red Hat Inc.
 *
 * Forked from Red Hat's Ceph project.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

/**
 * Split **str** into a list of strings, using the ";,= \t" delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as list
 * @param [out] str_list List modified containing str after it has been split
**/
extern void get_str_list(const std::string& str,
			 std::list<std::string>& str_list);

/**
 * Split **str** into a list of strings, using the **delims** delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as list
 * @param [in] delims characters used to split **str**
 * @param [out] str_list List modified containing str after it has been split
**/
extern void get_str_list(const std::string& str,
                         const char *delims,
			 std::list<std::string>& str_list);

/**
 * Split **str** into a list of strings, using the ";,= \t" delimiters and output the result in **str_vec**.
 * 
 * @param [in] str String to split and save as Vector
 * @param [out] str_vec Vector modified containing str after it has been split
**/
extern void get_str_vec(const std::string& str,
			 std::vector<std::string>& str_vec);

/**
 * Split **str** into a list of strings, using the **delims** delimiters and output the result in **str_vec**.
 * 
 * @param [in] str String to split and save as Vector
 * @param [in] delims characters used to split **str**
 * @param [out] str_vec Vector modified containing str after it has been split
**/
extern void get_str_vec(const std::string& str,
                         const char *delims,
			 std::vector<std::string>& str_vec);

/**
 * Split **str** into a list of strings, using the ";,= \t" delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as Set
 * @param [out] str_list Set modified containing str after it has been split
**/
extern void get_str_set(const std::string& str,
			std::set<std::string>& str_list);

/**
 * Split **str** into a list of strings, using the **delims** delimiters and output the result in **str_list**.
 * 
 * @param [in] str String to split and save as Set
 * @param [in] delims characters used to split **str**
 * @param [out] str_list Set modified containing str after it has been split
**/
extern void get_str_set(const std::string& str,
                        const char *delims,
			std::set<std::string>& str_list);

/**
 * Return a String containing the vector **v** joined with **sep**
 * 
 * If **v** is empty, the function returns an empty string
 * For each element in **v**,
 * it will concatenate this element and **sep** with result
 * 
 * @param [in] v Vector to join as a String
 * @param [in] sep String used to join each element from **v**
 * @return empty string if **v** is empty or concatenated string
**/
inline std::string str_join(const std::vector<std::string>& v, const std::string& sep)
{
  if (v.empty())
    return std::string();
  std::vector<std::string>::const_iterator i = v.begin();
  std::string r = *i;
  for (++i; i != v.end(); ++i) {
    r += sep;
    r += *i;
  }
  return r;
}

#endif
