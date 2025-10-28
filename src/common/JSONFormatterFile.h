// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "JSONFormatter.h"

#include <fstream>

namespace ceph {

  class JSONFormatterFile : public JSONFormatter {
public:
    JSONFormatterFile(const std::string& path, bool pretty=false) :
      JSONFormatter(pretty),
      path(path),
      file(path, std::ios::out | std::ios::trunc)
    {
    }
    ~JSONFormatterFile() {
      flush();
    }

    void flush(std::ostream& os) override {
      flush();
    }
    void flush() {
      JSONFormatter::finish_pending_string();
      file.flush();
    }

    void reset() override {
      JSONFormatter::reset();
      file = std::ofstream(path, std::ios::out | std::ios::trunc);
    }
    int get_len() const override {
      return file.tellp();
    }
    std::ofstream const& get_ofstream() const {
      return file;
    }

protected:
    std::ostream& get_ss() override {
      return file;
    }

private:
    std::string path;
    mutable std::ofstream file; // mutable for get_len
  };

}

