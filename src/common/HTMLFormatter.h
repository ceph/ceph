// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_HTML_FORMATTER_H
#define CEPH_HTML_FORMATTER_H

#include "Formatter.h"

namespace ceph {
  class HTMLFormatter : public XMLFormatter {
  public:
    explicit HTMLFormatter(bool pretty = false);
    ~HTMLFormatter() override;
    void reset() override;

    void set_status(int status, const char* status_name) override;
    void output_header() override;

    void dump_unsigned(const char *name, uint64_t u) override;
    void dump_int(const char *name, int64_t u) override;
    void dump_float(const char *name, double d) override;
    void dump_string(const char *name, std::string_view s) override;
    std::ostream& dump_stream(const char *name) override;
    void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) override;

    /* with attrs */
    void dump_string_with_attrs(const char *name, std::string_view s, const FormatterAttrs& attrs) override;
  private:
    template <typename T> void dump_template(const char *name, T arg);

    int m_status;
    const char* m_status_name;
  };

}

#endif
