#pragma once

#include "mds/JournalPointer.h"

class MemoryJournalPointerStore: public JournalPointerStore {
  mutable JournalPointer store;
public:
  MemoryJournalPointerStore(JournalPointer const* backup = nullptr) { 
    if (backup) {
      store = *backup;
    }
  }
  virtual int load() { pointer = store; return 0; };
  virtual int save() const { save(nullptr); return 0; };
  virtual void save(Context *completion) const
  {
    store = pointer;
    if (completion) {
      completion->complete(0);
    }
  };
};
