#pragma once

#include "mds/JournalPointer.h"

class MemoryJournalPointerStore: public JournalPointerStore {
  JournalPointer *backup;
public:
  MemoryJournalPointerStore(JournalPointer *backup) : backup(backup) { }
  virtual int load() { pointer = *backup; return 0; };
  virtual int save() const { save(nullptr); return 0; };
  virtual void save(Context *completion) const
  {
    *backup = pointer;;
    if (completion) {
      completion->complete(0);
    }
  };
};
