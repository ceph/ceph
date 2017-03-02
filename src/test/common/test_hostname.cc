// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "common/hostname.h"
#include "common/SubProcess.h"
#include "stdio.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "unistd.h"

#include <array>
#include <iostream>
#include <stdexcept>
#include <stdio.h>
#include <string>
#include <memory>

std::string exec(const char* cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 128, pipe.get()) != NULL)
            result += buffer.data();
    }
    // remove \n
    return result.substr(0, result.size()-1);;
}

TEST(Hostname, full) {
  std::string hn = ceph_get_hostname();
  ASSERT_EQ(hn, exec("hostname")) ;

	
}

TEST(Hostname, short) {
  std::string shn = ceph_get_short_hostname();
  ASSERT_EQ(shn, exec("hostname -s")) ;
}
