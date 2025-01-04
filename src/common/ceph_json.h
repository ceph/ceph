#ifndef CEPH_JSON_H
#define CEPH_JSON_H

#include <string>
#include <iostream>
#include <stdexcept>

#include <ranges>
#include <concepts>
#include <typeindex>

#include <strings.h>

#include <boost/json.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include <fmt/format.h>

#include "common/ceph_time.h"

#include <include/types.h>
#include <include/utime.h>
#include <include/ceph_fs.h>

#include "Formatter.h"

class utime_t;

class JSONObj;
class JSONFormattable;

namespace ceph_json::detail {

template <typename T, typename ...Ts>
consteval bool is_any_of()
{
 return (std::is_same_v<T, Ts> || ...);
}

/* Note that std::is_integer<> will also pick up bool, which in our
case we need to count as a non-integer type as the JSON codecs treat
it differently ("https://en.cppreference.com/w/cpp/types/is_integral").

From what I can see in the extant code, it looks like the same rules
should be applied to char, etc., so I've also done so here:
*/
template <typename T>
concept json_integer = requires
{
 requires std::is_integral_v<T>;
 requires !std::is_same_v<T, bool>;

 requires !is_any_of<T, char, char8_t, char16_t, char32_t, wchar_t>();
};

template <typename T>
concept json_signed_integer = requires 
{
 requires json_integer<T> && std::signed_integral<T>; 
};

template <typename T>
concept json_unsigned_integer = requires
{
 requires json_integer<T> && std::unsigned_integral<T>;
};

/* Distinguish between containers with a value that's an associative kv-pair (a mapped type) and
those which are a "single" value. Note that this is not the same as the AssociativeContainer
named concept, as the rule there is that the container is key-indexed, and it does not necessarily
have to be a pair (e.g. std::set<> is an AssociativeContainer). Similarly, for sequence types
we don't want to capture standard strings and the like, even if we otherwise could consider them
a value sequence:
*/
template <typename ContainerT>
concept json_mapped_kv_seq = requires
{
 typename ContainerT::key_type;
 typename ContainerT::key_compare;

 typename ContainerT::value_type;
 typename ContainerT::mapped_type;
};

template <typename ContainerT>
concept json_val_seq = requires
{
 typename ContainerT::value_type;

 requires !json_mapped_kv_seq<ContainerT>;
 requires !std::convertible_to<ContainerT, std::string>;
};

} // namespace ceph_json

class JSONObjIter {

  using map_iter_t = std::map<std::string, std::unique_ptr<JSONObj>>::iterator;

  map_iter_t cur;
  map_iter_t last;

public:
  void set(const JSONObjIter::map_iter_t &_cur, const JSONObjIter::map_iter_t &_end);

  void operator++();

  // IMPORTANT: The returned pointer is intended as NON-OWNING (i.e. JSONObjIter 
  // is responsible for it):
  JSONObj *operator*();

  bool end() const {
    return (cur == last);
  }
};

class JSONObj 
{
  using children_multimap_t = std::multimap<std::string, std::unique_ptr<JSONObj>>;
  using children_multimap_value_type = typename children_multimap_t::value_type;

  JSONObj *parent = nullptr;

public:
  struct data_val {
    std::string str;
    bool quoted{false};

    void set(std::string_view s, bool q) {
      str = s;
      quoted = q;
    }
  };

protected:
  std::string name; // corresponds to obj_type in XMLObj

  boost::json::value data;

  data_val val;

  bool data_quoted{false};

  std::multimap<std::string, std::unique_ptr<JSONObj>> children;

  std::map<std::string, data_val> attr_map;

  void handle_value(boost::json::value v);

public:
  virtual ~JSONObj() = default;

public:
  void init(JSONObj *parent_node, boost::json::value data_in, std::string name_in);

  std::string& get_name() { return name; }
  data_val& get_data_val() { return val; }

  const std::string& get_data() { return val.str; }
  bool get_data(const std::string& key, data_val *dest);

  JSONObj *get_parent();

  // Note: takes ownership of child:
  void add_child(std::string el, JSONObj *child);

  bool get_attr(std::string name, data_val& attr);

  JSONObjIter find(const std::string& name);
  JSONObjIter find_first();
  JSONObjIter find_first(const std::string& name);
  JSONObj *find_obj(const std::string& name);

  friend std::ostream& operator<<(std::ostream &out,
                                  const JSONObj &obj); // does not work, FIXME

  bool is_array();
  bool is_object();

  std::vector<std::string> get_array_elements();
};

inline std::ostream& operator<<(std::ostream &out, const JSONObj::data_val& dv) {
  const char *q = (dv.quoted ? "\"" : "");
   out << q << dv.str << q;
   return out;
}

class JSONParser : public JSONObj
{
  int buf_len = 0;
  std::string json_buffer;

public:
  void handle_data(const char *s, int len);

  bool parse(const char *buf_, int len);
  bool parse(int len);
  bool parse();
  bool parse(const char *file_name);

  const char *get_json() { return json_buffer.c_str(); }
};

class JSONDecoder {
public:
  struct err : std::runtime_error {
    using runtime_error::runtime_error;
  };

  JSONParser parser;

  JSONDecoder(ceph::buffer::list& bl) {
    if (!parser.parse(bl.c_str(), bl.length())) {
      std::cout << "JSONDecoder::err()" << std::endl;		// JFW: should this still be here?
      throw JSONDecoder::err("failed to parse JSON input");
    }
  }

  template<class T>
  static bool decode_json(const char *name, T& val, JSONObj *obj, bool mandatory = false);

  template<class C>
  static bool decode_json(const char *name, C& container, void (*cb)(C&, JSONObj *obj), JSONObj *obj, bool mandatory = false);

  template<class T>
  static void decode_json(const char *name, T& val, const T& default_val, JSONObj *obj);

  template<class T>
  static bool decode_json(const char *name, boost::optional<T>& val, JSONObj *obj, bool mandatory = false);

  template<class T>
  static bool decode_json(const char *name, std::optional<T>& val, JSONObj *obj, bool mandatory = false);

};

/* Somewhat unfortunately, it does appear that JSONDecoder::err is checked
for in other parts of the code, so we can't get away with throwing standard
exceptions and remain compatible with the extant implementation. Somewhat 
annoyingly, the extant implication only in one case distinguishes between
out of range and invalid parsing. I've chosen to be a bit more specific.

Additionally, the existing implementation mysteriously parses trailing whitespace
after a number is successfully extracted, but appears to produce no interesting
side-effect based on that being successful; I see no real reason to do that,
unless I'm missing something, or we want to test the JSON parser: */
template <typename IntegerT>
requires ceph_json::detail::json_signed_integer<IntegerT> ||
         ceph_json::detail::json_unsigned_integer<IntegerT>
void decode_json_obj(IntegerT& val, JSONObj *obj)
try
{
 if(ceph_json::detail::json_signed_integer<IntegerT>)
  val = stol(obj->get_data());

 if(ceph_json::detail::json_unsigned_integer<IntegerT>)
  val = stoul(obj->get_data());
}
catch(const std::out_of_range& e)
{
 throw JSONDecoder::err(fmt::format("failed to parse number: {}", e.what()));
}
catch(const std::invalid_argument& e)
{
 throw JSONDecoder::err(fmt::format("failed to parse number: {}", e.what()));
}

template<class T>
void decode_json_obj(T& val, JSONObj *obj)
{
  val.decode_json(obj);
}

inline void decode_json_obj(std::string& val, JSONObj *obj)
{
  val = obj->get_data();
}

inline void decode_json_obj(JSONObj::data_val& val, JSONObj *obj)
{
  val = obj->get_data_val();
}

inline void decode_json_obj(bool& val, JSONObj *obj)
{
  std::string s = obj->get_data();
  if (strcasecmp(s.c_str(), "true") == 0) {
    val = true;
    return;
  }
  if (strcasecmp(s.c_str(), "false") == 0) {
    val = false;
    return;
  }
  int i;
  decode_json_obj(i, obj);
  val = (bool)i;
}

inline void decode_json_obj(bufferlist& val, JSONObj *obj)
{
  bufferlist bl;

  std::string_view sv = obj->get_data();

  bl.append(sv);

  try {
    val.decode_base64(bl);
  } catch (ceph::buffer::error& err) {
   throw JSONDecoder::err("failed to decode base64");
  }
}

inline void decode_json_obj(utime_t& val, JSONObj *obj)
{
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(obj->get_data(), &epoch, &nsec);
  if (r == 0) {
    val = utime_t(epoch, nsec);
  } else {
    throw JSONDecoder::err("failed to decode utime_t");
  }
}

inline void decode_json_obj(ceph::real_time& val, JSONObj *obj)
{
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(obj->get_data(), &epoch, &nsec);
  if (r == 0) {
    using namespace std::chrono;
    val = real_time{seconds(epoch) + nanoseconds(nsec)};
  } else {
    throw JSONDecoder::err("failed to decode real_time");
  }
}

inline void decode_json_obj(ceph::coarse_real_time& val, JSONObj *obj)
{
  uint64_t epoch;
  uint64_t nsec;
  int r = utime_t::parse_date(obj->get_data(), &epoch, &nsec);
  if (r == 0) {
    using namespace std::chrono;
    val = coarse_real_time{seconds(epoch) + nanoseconds(nsec)};
  } else {
    throw JSONDecoder::err("failed to decode coarse_real_time");
  }
}

inline void decode_json_obj(ceph_dir_layout& i, JSONObj *obj)
{
    unsigned tmp;
    JSONDecoder::decode_json("dir_hash", tmp, obj, true);
    i.dl_dir_hash = tmp;
    JSONDecoder::decode_json("unused1", tmp, obj, true);
    i.dl_unused1 = tmp;
    JSONDecoder::decode_json("unused2", tmp, obj, true);
    i.dl_unused2 = tmp;
    JSONDecoder::decode_json("unused3", tmp, obj, true);
    i.dl_unused3 = tmp;
}

template <ceph_json::detail::json_val_seq SeqT>
void decode_json_obj(SeqT& seq, JSONObj *obj)
{
 seq.clear();

   JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    typename SeqT::value_type val;
    JSONObj *o = *iter;
    decode_json_obj(val, o);

    if constexpr (requires { seq.emplace_back(val); })
     seq.emplace_back(val);
    else
     seq.emplace(val);
  }
}

template <ceph_json::detail::json_mapped_kv_seq KVSeqT>
void decode_json_obj(KVSeqT& kvs, JSONObj *obj)
{
  kvs.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    typename KVSeqT::key_type key;
    typename KVSeqT::mapped_type val;
    JSONObj *o = *iter;
    JSONDecoder::decode_json("key", key, o);
    JSONDecoder::decode_json("val", val, o);
  
    if constexpr(requires { kvs[key] = val; }) 
     kvs[key] = val; // i.e. insert_or_assign()
    else
     kvs.insert({key, val}); 
  }
}

template<class C>
void decode_json_obj(C& container, void (*cb)(C&, JSONObj *obj), JSONObj *obj)
{
  container.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    JSONObj *o = *iter;
    cb(container, o);
  }
}

template<class T>
bool JSONDecoder::decode_json(const char *name, T& val, JSONObj *obj, bool mandatory)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    if constexpr (std::is_default_constructible_v<T>) {
      val = T();
    }
    return false;
  }

  try {
    decode_json_obj(val, *iter);
  } catch (const err& e) {
    std::string s = std::string(name) + ": ";
    s.append(e.what());
    throw err(s);
  }

  return true;
}

template<class C>
bool JSONDecoder::decode_json(const char *name, C& container, void (*cb)(C&, JSONObj *), JSONObj *obj, bool mandatory)
{
  container.clear();

  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    return false;
  }

  try {
    decode_json_obj(container, cb, *iter);
  } catch (const err& e) {
    std::string s = std::string(name) + ": ";
    s.append(e.what());
    throw err(s);
  }

  return true;
}

template<class T>
void JSONDecoder::decode_json(const char *name, T& val, const T& default_val, JSONObj *obj)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    val = default_val;
    return;
  }

  try {
    decode_json_obj(val, *iter);
  } catch (const err& e) {
    val = default_val;
    std::string s = std::string(name) + ": ";
    s.append(e.what());
    throw err(s);
  }
}

template<class T>
bool JSONDecoder::decode_json(const char *name, boost::optional<T>& val, JSONObj *obj, bool mandatory)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    val = boost::none;
    return false;
  }

  try {
    val.reset(T());
    decode_json_obj(val.get(), *iter);
  } catch (const err& e) {
    val.reset();
    std::string s = std::string(name) + ": ";
    s.append(e.what());
    throw err(s);
  }

  return true;
}

template<class T>
bool JSONDecoder::decode_json(const char *name, std::optional<T>& val, JSONObj *obj, bool mandatory)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    val.reset();
    return false;
  }

  try {
    val.emplace();
    decode_json_obj(*val, *iter);
  } catch (const err& e) {
    val.reset();
    std::string s = std::string(name) + ": ";
    s.append(e.what());
    throw err(s);
  }

  return true;
}

class JSONEncodeFilter
{
public:
  class HandlerBase {
  public:
    virtual ~HandlerBase() {}

    virtual std::type_index get_type() = 0;
    virtual void encode_json(const char *name, const void *pval, ceph::Formatter *) const = 0;
  };

  template <class T>
  class Handler : public HandlerBase {
  public:
    virtual ~Handler() {}

    std::type_index get_type() override {
      return std::type_index(typeid(const T&));
    }
  };

private:
  std::map<std::type_index, HandlerBase *> handlers;

public:
  void register_type(HandlerBase *h) {
    handlers[h->get_type()] = h;
  }

  template <class T>
  bool encode_json(const char *name, const T& val, ceph::Formatter *f) {
    auto iter = handlers.find(std::type_index(typeid(val)));
    if (iter == handlers.end()) {
      return false;
    }

    iter->second->encode_json(name, (const void *)&val, f);
    return true;
  }
};

void encode_json(const char *name, ceph_json::detail::json_signed_integer auto val, Formatter *f)
{
 f->dump_int(name, val);
}

void encode_json(const char *name, ceph_json::detail::json_unsigned_integer auto val, Formatter *f)
{
 f->dump_unsigned(name, val);
}

template<class T>
requires requires(const T& val, ceph::Formatter *f) { val.dump(f); }
void encode_json_impl(const char *name, const T& val, ceph::Formatter *f)
{
  f->open_object_section(name);
  val.dump(f);
  f->close_section();
}

template<class T>
requires requires(const T& val, ceph::Formatter *f) { encode_json_impl("", val, f); }
void encode_json(const char *name, const T& val, ceph::Formatter *f)
{
  JSONEncodeFilter *filter = static_cast<JSONEncodeFilter *>(f->get_external_feature_handler("JSONEncodeFilter"));

  if (!filter ||
      !filter->encode_json(name, val, f)) {
    encode_json_impl(name, val, f);
  }
}

inline void encode_json(const char *name, std::string_view val, Formatter *f)
{
  f->dump_string(name, val);
}

inline void encode_json(const char *name, const std::string& val, Formatter *f)
{
  f->dump_string(name, val);
}

inline void encode_json(const char *name, const char *val, Formatter *f)
{
  f->dump_string(name, val);
}

inline void encode_json(const char *name, bool val, Formatter *f)
{
  f->dump_bool(name, val);
}

inline void encode_json(const char *name, const utime_t& val, Formatter *f)
{
  val.gmtime(f->dump_stream(name));
}

inline void encode_json(const char *name, const ceph::real_time& val, Formatter *f)
{
  encode_json(name, utime_t{val}, f);
}

inline void encode_json(const char *name, const ceph::coarse_real_time& val, Formatter *f)
{
  encode_json(name, utime_t{val}, f);
}

inline void encode_json(const char *name, const bufferlist& bl, Formatter *f)
{
  /* need to copy data from bl, as it is const bufferlist */
  bufferlist src = bl;

  bufferlist b64;
  src.encode_base64(b64);

  std::string_view sv(b64.c_str(), b64.length()); 

  encode_json(name, sv, f);
}

template <class T>
void encode_json(const char *name, const std::optional<T>& o, ceph::Formatter *f)
{
  if (!o) {
    return;
  }
  encode_json(name, *o, f);
}

inline void encode_json(const char *name, const JSONObj::data_val& v, Formatter *f)
{
  if (v.quoted) {
    encode_json(name, v.str, f);
  } else {
    f->dump_format_unquoted(name, "%s", v.str.c_str());
  }
}

inline void encode_json(const char *name, const JSONFormattable& v, Formatter *f);

template<class K, class V>
void encode_json_map(const char *name, const std::map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
    encode_json("obj", iter->second, f);
  }
  f->close_section();
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     void (*cb)(const char *, const V&, ceph::Formatter *, void *), void *parent,
                     const std::map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
    if (index_name) {
      f->open_object_section("key_value");
      f->dump_string(index_name, iter->first);
    }

    if (object_name) {
      f->open_object_section(object_name);
    }

    if (cb) {
      cb(value_name, iter->second, f, parent);
    } else {
      encode_json(value_name, iter->second, f);
    }

    if (object_name) {
      f->close_section();
    }
    if (index_name) {
      f->close_section();
    }
  }
  f->close_section(); 
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     const std::map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, object_name, value_name, NULL, NULL, m, f);
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name, const char *value_name,
                     const std::map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, NULL, value_name, NULL, NULL, m, f);
}

template <class K, class V>
void encode_json_map(const char *name, const boost::container::flat_map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
    encode_json("obj", iter->second, f);
  }
  f->close_section();
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     void (*cb)(const char *, const V&, ceph::Formatter *, void *), void *parent,
                     const boost::container::flat_map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
    if (index_name) {
      f->open_object_section("key_value");
      f->dump_string(index_name, iter->first);
    }

    if (object_name) {
      f->open_object_section(object_name);
    }

    if (cb) {
      cb(value_name, iter->second, f, parent);
    } else {
      encode_json(value_name, iter->second, f);
    }

    if (object_name) {
      f->close_section();
    }
    if (index_name) {
      f->close_section();
    }
  }
  f->close_section(); 
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     const boost::container::flat_map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, object_name, value_name, NULL, NULL, m, f);
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name, const char *value_name,
                     const boost::container::flat_map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, NULL, value_name, NULL, NULL, m, f);
}

void encode_json(const char *name, const ceph_json::detail::json_val_seq auto& val, Formatter *f)
{
  f->open_array_section(name);
   std::ranges::for_each(val, [&f](const auto &obj) {
		 ::encode_json("obj", obj, f);
  		});
  f->close_section();
}

void encode_json(const char *name, const ceph_json::detail::json_mapped_kv_seq auto& val, Formatter *f)
{
  f->open_array_section(name);
   std::ranges::for_each(val, [&f](const auto& kv) {
		    f->open_object_section("entry");
		     ::encode_json("key", kv.first, f);
		     ::encode_json("val", kv.second, f);
		    f->close_section();
		});
  f->close_section();
}

class JSONFormattable : public ceph::JSONFormatter {
  JSONObj::data_val value;
  std::vector<JSONFormattable> arr;
  std::map<std::string, JSONFormattable> obj;

  std::vector<JSONFormattable *> enc_stack;
  JSONFormattable *cur_enc;

protected:
  bool handle_value(std::string_view name, std::string_view s, bool quoted) override;
  bool handle_open_section(std::string_view name, const char *ns, bool section_is_array) override;
  bool handle_close_section() override;

public:
  JSONFormattable(bool p = false) : JSONFormatter(p) {
    cur_enc = this;
    enc_stack.push_back(cur_enc);
  }

  enum Type {
    FMT_NONE,
    FMT_VALUE,
    FMT_ARRAY,
    FMT_OBJ,
  } type{FMT_NONE};

  void set_type(Type t) {
    type = t;
  }

  void decode_json(JSONObj *jo) {
    if (jo->is_array()) {
      set_type(JSONFormattable::FMT_ARRAY);
      decode_json_obj(arr, jo);
    } else if (jo->is_object()) {
      set_type(JSONFormattable::FMT_OBJ);
      auto iter = jo->find_first();
      for (;!iter.end(); ++iter) {
        JSONObj *field = *iter;
        decode_json_obj(obj[field->get_name()], field);
      }
    } else {
      set_type(JSONFormattable::FMT_VALUE);
      decode_json_obj(value, jo);
    }
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode((uint8_t)type, bl);
    encode(value.str, bl);
    encode(arr, bl);
    encode(obj, bl);
    encode(value.quoted, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    uint8_t t;
    decode(t, bl);
    type = (Type)t;
    decode(value.str, bl);
    decode(arr, bl);
    decode(obj, bl);
    if (struct_v >= 2) {
      decode(value.quoted, bl);
    } else {
      value.quoted = true;
    }
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    switch (type) {
      case FMT_VALUE:
        if (value.quoted) {
          f->dump_string("value", value.str);
        } else {
          f->dump_format_unquoted("value", "%s", value.str.c_str());
        }
        break;
      case FMT_ARRAY:
        f->open_array_section("array");
        for (auto& i : arr) {
          i.dump(f);
        }
        f->close_section();
        break;
      case FMT_OBJ:
        f->open_object_section("object");
        for (auto& i : obj) {
          f->dump_object(i.first.c_str(), i.second);
        }
        f->close_section();
        break;
      default:
        break;
    }
  }

  static void generate_test_instances(std::list<JSONFormattable*>& o) {
    o.push_back(new JSONFormattable);
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_VALUE);
    o.back()->value.str = "foo";
    o.back()->value.quoted = true;
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_VALUE);
    o.back()->value.str = "foo";
    o.back()->value.quoted = false;
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_ARRAY);
    o.back()->arr.push_back(JSONFormattable());
    o.back()->arr.back().set_type(FMT_VALUE);
    o.back()->arr.back().value.str = "foo";
    o.back()->arr.back().value.quoted = true;
    o.back()->arr.push_back(JSONFormattable());
    o.back()->arr.back().set_type(FMT_VALUE);
    o.back()->arr.back().value.str = "bar";
    o.back()->arr.back().value.quoted = true;
    o.push_back(new JSONFormattable);
    o.back()->set_type(FMT_OBJ);
    o.back()->obj["foo"] = JSONFormattable();
    o.back()->obj["foo"].set_type(FMT_VALUE);
    o.back()->obj["foo"].value.str = "bar";
    o.back()->obj["foo"].value.quoted = true;
  }

  const std::string& val() const {
    return value.str;
  }

  int val_int() const;
  long val_long() const;
  long long val_long_long() const;
  bool val_bool() const;

  const std::map<std::string, JSONFormattable> object() const {
    return obj;
  }

  const std::vector<JSONFormattable>& array() const {
    return arr;
  }

  const JSONFormattable& operator[](const std::string& name) const;
  const JSONFormattable& operator[](size_t index) const;

  JSONFormattable& operator[](const std::string& name);
  JSONFormattable& operator[](size_t index);

  operator std::string() const {
    return value.str;
  }

  explicit operator int() const {
    return val_int();
  }

  explicit operator long() const {
    return val_long();
  }

  explicit operator long long() const {
    return val_long_long();
  }

  explicit operator bool() const {
    return val_bool();
  }

  template<class T>
  T operator[](const std::string& name) const {
    return this->operator[](name)(T());
  }

  template<class T>
  T operator[](const std::string& name) {
    return this->operator[](name)(T());
  }

  std::string operator ()(const char *def_val) const {
    return def(std::string(def_val));
  }

  int operator()(int def_val) const {
    return def(def_val);
  }

  bool operator()(bool def_val) const {
    return def(def_val);
  }

  bool exists(const std::string& name) const;
  bool exists(size_t index) const;

  std::string def(const std::string& def_val) const;
  int def(int def_val) const;
  bool def(bool def_val) const;

  bool find(const std::string& name, std::string *val) const;

  std::string get(const std::string& name, const std::string& def_val) const;

  int get_int(const std::string& name, int def_val) const;
  bool get_bool(const std::string& name, bool def_val) const;

  int set(const std::string& name, const std::string& val);
  int erase(const std::string& name);

  void derive_from(const JSONFormattable& jf);

  void encode_json(const char *name, ceph::Formatter *f) const;

  bool is_array() const {
    return (type == FMT_ARRAY);
  }
};
WRITE_CLASS_ENCODER(JSONFormattable)

inline void encode_json(const char *name, const JSONFormattable& v, Formatter *f)
{
  v.encode_json(name, f);
}

inline void JSONFormattable::encode_json(const char *name, Formatter *f) const
{
  switch (type) {
    case JSONFormattable::FMT_VALUE:
      ::encode_json(name, value, f);
      break;
    case JSONFormattable::FMT_ARRAY:
      ::encode_json(name, arr, f);
      break;
    case JSONFormattable::FMT_OBJ:
      f->open_object_section(name);
      for (auto iter : obj) {
        ::encode_json(iter.first.c_str(), iter.second, f);
      }
      f->close_section();
      break;
    case JSONFormattable::FMT_NONE:
      break;
  }
}

#endif
