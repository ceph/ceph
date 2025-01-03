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
#include "common/utf8.h"
#include "gtest/gtest.h"
#include <stdint.h>

TEST(IsValidUtf8, SimpleAscii) {
  ASSERT_EQ(0, check_utf8_cstr("Ascii ftw."));
  ASSERT_EQ(0, check_utf8_cstr(""));
  ASSERT_EQ(0, check_utf8_cstr("B"));
  ASSERT_EQ(0, check_utf8_cstr("Badgers badgers badgers badgers "
			       "mushroom mushroom"));
  ASSERT_EQ(0, check_utf8("foo", strlen("foo")));
}

TEST(IsValidUtf8, ControlChars) {
  // Sadly, control characters are valid utf8...
  uint8_t control_chars[] = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 
			      0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d };
  ASSERT_EQ(0, check_utf8((char*)control_chars, sizeof(control_chars)));
}

TEST(IsValidUtf8, SimpleUtf8) {
  uint8_t funkystr[] = { 0x66, 0xd1, 0x86, 0xd1, 0x9d, 0xd2, 0xa0, 0xd3,
		       0xad, 0xd3, 0xae, 0x0a };
  ASSERT_EQ(0, check_utf8((char*)funkystr, sizeof(funkystr)));

  uint8_t valid2[] = { 0xc3, 0xb1 };
  ASSERT_EQ(0, check_utf8((char*)valid2, sizeof(valid2)));
}

TEST(IsValidUtf8, InvalidUtf8) {
  uint8_t inval[] = { 0xe2, 0x28, 0xa1 };
  ASSERT_NE(0, check_utf8((char*)inval, sizeof(inval)));

  uint8_t invalid2[] = { 0xc3, 0x28 };
  ASSERT_NE(0, check_utf8((char*)invalid2, sizeof(invalid2)));
}

TEST(HasControlChars, HasControlChars1) {
  uint8_t has_control_chars[] = { 0x41, 0x01, 0x00 };
  ASSERT_NE(0, check_for_control_characters_cstr((const char*)has_control_chars));
  uint8_t has_control_chars2[] = { 0x7f, 0x41, 0x00 };
  ASSERT_NE(0, check_for_control_characters_cstr((const char*)has_control_chars2));

  char has_newline[] = "blah   blah\n";
  ASSERT_NE(0, check_for_control_characters_cstr(has_newline));

  char no_control_chars[] = "blah   blah";
  ASSERT_EQ(0, check_for_control_characters_cstr(no_control_chars));

  uint8_t validutf[] = { 0x66, 0xd1, 0x86, 0xd1, 0x9d, 0xd2, 0xa0, 0xd3,
		       0xad, 0xd3, 0xae, 0x0 };
  ASSERT_EQ(0, check_for_control_characters_cstr((const char*)validutf));
}
