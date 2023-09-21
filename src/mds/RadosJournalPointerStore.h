#pragma once

#include "JournalPointer.h"

class RadosJournalPointerStore : public JournalPointerStore {
  public:
  RadosJournalPointerStore(int node_id_, int64_t pool_id_, Objecter* objecter_)
      : node_id(node_id_)
      , pool_id(pool_id_)
      , objecter(objecter_)
  {
  }

  int load();
  int save() const;
  void save(Context *completion) const;

  private:
  // MDS rank
  int node_id;
  // Metadata pool ID
  int64_t pool_id;
  Objecter* objecter;

  std::string get_object_id() const;
};
