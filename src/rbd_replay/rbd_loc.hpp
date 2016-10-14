// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef _INCLUDED_RBD_REPLAY_RBD_LOC_HPP
#define _INCLUDED_RBD_REPLAY_RBD_LOC_HPP

#include <string>

namespace rbd_replay {

/**
   Stores a pool, image name, and snap name triple.
   rbd_locs can be converted to/from strings with the format pool/image\@snap.
   The slash and at signs can be omitted if the pool and snap are empty, respectively.
   Backslashes can be used to escape slashes and at signs in names.
   Examples:

   |Pool  | Image | Snap | String             |
   |------|-------|------|--------------------|
   |rbd   | vm    | 1    | rbd/vm\@1          |
   |rbd   | vm    |      | rbd/vm             |
   |      | vm    | 1    | vm\@1              |
   |      | vm    |      | vm                 |
   |rbd   |       | 1    | rbd/\@1            |
   |rbd\@x| vm/y  | 1    | rbd\\\@x/vm\\/y\@1 |

   (The empty string should obviously be avoided as the image name.)

   Note that the non-canonical forms /vm\@1 and rbd/vm\@ can also be parsed,
   although they will be formatted as vm\@1 and rbd/vm.
 */
struct rbd_loc {
  /**
     Constructs an rbd_loc with the empty string for the pool, image, and snap.
   */
  rbd_loc();

  /**
     Constructs an rbd_loc with the given pool, image, and snap.
   */
  rbd_loc(std::string pool, std::string image, std::string snap);

  /**
     Parses an rbd_loc from the given string.
     If parsing fails, the contents are unmodified.
     @retval true if parsing succeeded
   */
  bool parse(std::string name_string);

  /**
     Returns the string representation of the locator.
   */
  std::string str() const;

  /**
     Compares the locators lexicographically by pool, then image, then snap.
   */
  int compare(const rbd_loc& rhs) const;

  /**
     Returns true if the locators have identical pool, image, and snap.
   */
  bool operator==(const rbd_loc& rhs) const;

  /**
     Compares the locators lexicographically by pool, then image, then snap.
   */
  bool operator<(const rbd_loc& rhs) const;

  std::string pool;

  std::string image;

  std::string snap;
};

}

#endif
