#pragma once

#include "include/fs_types.h"

#include <string_view>

using namespace std::literals::string_view_literals;

class QuarantineEvent : public LogEvent {
protected:
  inodeno_t subvol_ino;	// subvol inode number
  unsigned qtine_op;	// quarantine op: QUARANTINE_ADD | QUARANTINE_DEL

  std::string_view get_qtine_op_name() const {
    switch (qtine_op) {
      case QUARANTINE_NONE: return "QUARANTINE_NONE"sv; break;
      case QUARANTINE_ADD: return "QUARANTINE_ADD"sv; break;
      case QUARANTINE_DEL: return "QUARANTINE_DEL"sv; break;
      default: return "UNKNOWN"sv; break;
    }
  }

public:
  QuarantineEvent(int t) : LogEvent(t), subvol_ino(0), qtine_op(0) {}

  void set_subvol_ino(inodeno_t ino) {
    subvol_ino = ino;
  }

  void set_qtine_op(unsigned op) {
    qtine_op = op;
  }
};
