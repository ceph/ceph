// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <fcntl.h>
#include <iostream>
#include <string>
#include <unistd.h>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "include/buffer.h"

inline int rgw_admin_read_input(const std::string& infile, ceph::bufferlist& bl)
{
  int fd = 0;
  if (!infile.empty()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      std::cerr << "error reading input file " << infile << std::endl;
      return err;
    }
  }
  constexpr auto READ_CHUNK = 8196;
  int r, err;
  do {
    char buf[READ_CHUNK];
    r = safe_read(fd, buf, READ_CHUNK);
    if (r < 0) {
      err = -errno;
      std::cerr << "error while reading input" << std::endl;
      goto out;
    }
    bl.append(buf, r);
  } while (r > 0);
  err = 0;
 out:
  if (!infile.empty()) {
    close(fd);
  }
  return err;
}

template <class T>
inline int rgw_admin_read_decode_json(const std::string& infile, T& t)
{
  ceph::bufferlist bl;
  int ret = rgw_admin_read_input(infile, bl);
  if (ret < 0) {
    std::cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    std::cout << "failed to parse JSON" << std::endl;
    return -EINVAL;
  }

  try {
    decode_json_obj(t, &p);
  } catch (const JSONDecoder::err& e) {
    std::cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0;
}

template <class T, class K>
inline int rgw_admin_read_decode_json(const std::string& infile, T& t, K *k)
{
  ceph::bufferlist bl;
  int ret = rgw_admin_read_input(infile, bl);
  if (ret < 0) {
    std::cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    std::cout << "failed to parse JSON" << std::endl;
    return -EINVAL;
  }

  try {
    t.decode_json(&p, k);
  } catch (const JSONDecoder::err& e) {
    std::cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0;
}
