// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_FORMATTER_H
#define CEPH_FORMATTER_H

#include "include/buffer_fwd.h"

#include <functional>
#include <list>
#include <memory>
#include <string>

#include <stdarg.h>

namespace ceph {

  struct FormatterAttrs {
    std::list< std::pair<std::string, std::string> > attrs;

    FormatterAttrs(const char *attr, ...);
  };

  class Formatter {
  public:
    class ObjectSection {
      Formatter& formatter;

    public:
      ObjectSection(Formatter& f, std::string_view name) : formatter(f) {
        formatter.open_object_section(name);
      }
      ObjectSection(Formatter& f, std::string_view name, const char *ns) : formatter(f) {
        formatter.open_object_section_in_ns(name, ns);
      }
      ~ObjectSection() {
        formatter.close_section();
      }
    };
    class ArraySection {
      Formatter& formatter;

    public:
      ArraySection(Formatter& f, std::string_view name) : formatter(f) {
        formatter.open_array_section(name);
      }
      ArraySection(Formatter& f, std::string_view name, const char *ns) : formatter(f) {
        formatter.open_array_section_in_ns(name, ns);
      }
      ~ArraySection() {
        formatter.close_section();
      }
    };

    /// Helper to check if a type is a map for our purpose
    /// (based on fmt code)
    template <typename T> class is_map {
      template <typename U> static auto check(U*) -> typename U::mapped_type;
      template <typename> static void check(...);
    public:
      static constexpr const bool value =
        !std::is_void<decltype(check<T>(nullptr))>::value;
    };

    /**
     * with_array_section()
     * Opens an array section and calls 'fn' on each element in the container.
     * Two overloads are provided:
     * 1. for maps, where the function takes a key and a value, and
     * 2. for other types of containers, where the function takes just an
     *    element.
     */

    // for maps
    template <
	typename M,     //!< a map<K, V>
	typename FN,    //!< a callable to be applied to each element
	typename K = std::remove_cvref_t<M>::key_type,
	typename V = std::remove_cvref_t<M>::mapped_type>
      requires(
	  is_map<M>::value && (
          std::is_invocable_v<FN, Formatter&, const K&, const V&, std::string_view> ||
          std::is_invocable_v<FN, Formatter&, const K&, const V&>))
    void with_array_section(std::string_view txt, const M& m, FN&& fn) {
      Formatter::ArraySection as(*this, txt);
      for (const auto& [k, v] : m) {
        if constexpr (std::is_invocable_v<FN, Formatter&, const K&, const V&, std::string_view>) {
	  std::invoke(std::forward<FN>(fn), *this, k, v, txt);
        } else {
          std::invoke(std::forward<FN>(fn), *this, k, v);
        }
      }
    }

    // for other types of containers
    template <
	typename M,
	typename FN,
	typename V = std::remove_cvref_t<M>::value_type>
      requires(
	  !is_map<M>::value && (
          std::is_invocable_r_v<void, FN, Formatter&, const V&,
	       std::string_view> ||
	  std::is_invocable_r_v<void, FN, Formatter&, const V&>))
    void with_array_section(std::string_view txt, const M& m, FN&& fn) {
      Formatter::ArraySection as(*this, txt);
      for (const auto& v : m) {
	if constexpr (std::is_invocable_v<
			  FN, Formatter&, const V&, std::string_view>) {
	  std::invoke(std::forward<FN>(fn), *this, v, txt);
	} else {
	  std::invoke(std::forward<FN>(fn), *this, v);
	}
      }
    }

    /**
     * with_obj_array_section()
     * Opens an array section, then - iterates over the container
     * (which can be a map or a vector) and creates an object section
     * for each element in the container. The provided function 'fn' is
     * called on each element in the container.
     *
     * Two overloads are provided:
     * 1. for maps, where the function takes a key and a value, and
     * 2. for other types of containers, where the function is only
     *    handed the object (value) in the container.
     */

    template <
	typename M,     //!< a map<K, V>
	typename FN,    //!< a callable to be applied to each element
	typename K = std::remove_cvref_t<M>::key_type,
	typename V = std::remove_cvref_t<M>::mapped_type>
      requires(
	  is_map<M>::value && (
          std::is_invocable_v<FN, Formatter&, const K&, const V&, std::string_view> ||
          std::is_invocable_v<FN, Formatter&, const K&, const V&>))
    void with_obj_array_section(std::string_view txt, const M& m, FN&& fn) {
      Formatter::ArraySection as(*this, txt);
      for (const auto& [k, v] : m) {
	Formatter::ObjectSection os(*this, txt);
        if constexpr (std::is_invocable_v<FN, Formatter&, const K&, const V&, std::string_view>) {
            std::invoke(std::forward<FN>(fn), *this, k, v, txt);
        } else {
            std::invoke(std::forward<FN>(fn), *this, k, v);
        }
      }
    }

    template <
	typename M,  //!< a container (which is not a map) of 'V's
	typename FN,
	typename V = std::remove_cvref_t<M>::value_type>
      requires(
	  (!is_map<M>::value) && (
          std::is_invocable_v<FN, Formatter&, const V&, std::string_view> ||
          std::is_invocable_v<FN, Formatter&, const V&>))
    void with_obj_array_section(std::string_view txt, const M& m, FN&& fn) {
      Formatter::ArraySection as(*this, txt);
      for (const auto& v : m) {
	Formatter::ObjectSection os(*this, txt);
        if constexpr (std::is_invocable_v<FN, Formatter&, const V&, std::string_view>) {
            std::invoke(std::forward<FN>(fn), *this, v, txt);
        } else {
            std::invoke(std::forward<FN>(fn), *this, v);
        }
      }
    }

    static Formatter *create(std::string_view type,
			     std::string_view default_type,
			     std::string_view fallback);
    static Formatter *create(std::string_view type,
			     std::string_view default_type) {
      return create(type, default_type, "");
    }
    static Formatter *create(std::string_view type) {
      return create(type, "json-pretty", "");
    }
    template <typename... Params>
    static std::unique_ptr<Formatter> create_unique(Params &&...params)
    {
      return std::unique_ptr<Formatter>(
	  Formatter::create(std::forward<Params>(params)...));
    }

    Formatter() = default;
    virtual ~Formatter() = default;

    virtual void enable_line_break() = 0;
    virtual void flush(std::ostream& os) = 0;
    void flush(bufferlist &bl);
    virtual void reset() = 0;

    virtual void set_status(int status, const char* status_name) = 0;
    virtual void output_header() = 0;
    virtual void output_footer() = 0;

    virtual void open_array_section(std::string_view name) = 0;
    virtual void open_array_section_in_ns(std::string_view name, const char *ns) = 0;
    virtual void open_object_section(std::string_view name) = 0;
    virtual void open_object_section_in_ns(std::string_view name, const char *ns) = 0;
    virtual void close_section() = 0;
    virtual void dump_null(std::string_view name) = 0;
    virtual void dump_unsigned(std::string_view name, uint64_t u) = 0;
    virtual void dump_int(std::string_view name, int64_t s) = 0;
    virtual void dump_float(std::string_view name, double d) = 0;
    virtual void dump_string(std::string_view name, std::string_view s) = 0;
    virtual void dump_bool(std::string_view name, bool b)
    {
      dump_format_unquoted(name, "%s", (b ? "true" : "false"));
    }
    template<typename T>
    void dump_object(std::string_view name, const T& foo) {
      open_object_section(name);
      foo.dump(this);
      close_section();
    }
    virtual std::ostream& dump_stream(std::string_view name) = 0;
    virtual void dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap) = 0;
    virtual void dump_format(std::string_view name, const char *fmt, ...);
    virtual void dump_format_ns(std::string_view name, const char *ns, const char *fmt, ...);
    virtual void dump_format_unquoted(std::string_view name, const char *fmt, ...);
    virtual int get_len() const = 0;
    virtual void write_raw_data(const char *data) = 0;
    /* with attrs */
    virtual void open_array_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
    {
      open_array_section(name);
    }
    virtual void open_object_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
    {
      open_object_section(name);
    }
    virtual void dump_string_with_attrs(std::string_view name, std::string_view s, const FormatterAttrs& attrs)
    {
      dump_string(name, s);
    }

    virtual void *get_external_feature_handler(const std::string& feature) {
      return nullptr;
    }
    virtual void write_bin_data(const char* buff, int buf_len);
  };

  std::string fixed_to_string(int64_t num, int scale);
  std::string fixed_u_to_string(uint64_t num, int scale);
}
#endif

