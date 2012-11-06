// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef FILESTOREBACKENDH
#define FILESTOREBACKENDH

#include "common/Finisher.h"
#include "backend.h"
#include "include/Context.h"
#include "os/ObjectStore.h"

class FileStoreBackend : public Backend {
  ObjectStore *os;
  Finisher finisher;
  map<string, ObjectStore::Sequencer> osrs;
  const bool write_infos;

public:
  FileStoreBackend(ObjectStore *os, bool write_infos);
  ~FileStoreBackend() {
    finisher.stop();
  }
  void write(
    const string &oid,
    uint64_t offset,
    const bufferlist &bl,
    Context *on_applied,
    Context *on_commit);

  void read(
    const string &oid,
    uint64_t offset,
    uint64_t length,
    bufferlist *bl,
    Context *on_complete);
};

#endif
