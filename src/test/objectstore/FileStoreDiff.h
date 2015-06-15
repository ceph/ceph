// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 New Dream Network
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef FILESTORE_DIFF_H_
#define FILESTORE_DIFF_H_

#include <iostream>
#include <stdlib.h>
#include <map>
#include <boost/scoped_ptr.hpp>
#include "common/debug.h"
#include "os/filestore/FileStore.h"
#include "common/config.h"

class FileStoreDiff {

 private:
  FileStore *a_store;
  FileStore *b_store;

  bool diff_objects(FileStore *a_store, FileStore *b_store, coll_t coll);
  bool diff_objects_stat(struct stat& a, struct stat& b);
  bool diff_attrs(std::map<std::string,bufferptr>& b,
      std::map<std::string,bufferptr>& a);

public:
  FileStoreDiff(FileStore *a, FileStore *b);
  virtual ~FileStoreDiff();

  bool diff();
};

#endif /* FILESTOREDIFF_H_ */
