// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

#include "include/denc.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/onode.h"
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include "crimson/common/errorator.h"
#include "crimson/common/coroutine.h"
#include "log_manager.h"

namespace crimson::os::seastore::log_manager{

struct LogKVNodeLayout;
struct delta_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    REMOVE,
    ADD_PREV,
    INIT,
  } op;
  std::string key;
  ceph::bufferlist val;
  laddr_t prev;

  DENC(delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.val, p);
    denc(v.prev, p);
    DENC_FINISH(p);
  }

  void replay(LogKVNodeLayout &l);
};

class delta_buffer_t {
  std::vector<delta_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }
  void insert(
    const std::string &key,
    const ceph::bufferlist &val) {
    buffer.push_back(
      delta_t{
        delta_t::op_t::INSERT,
        key,
        val
      });
  }
  void remove(const std::string &key) {
    buffer.push_back(
      delta_t{
	delta_t::op_t::REMOVE,
	key,
	bufferlist()
      });
  }
  void insert_prev_addr(
      const laddr_t l) {
    buffer.push_back(
      delta_t{
	delta_t::op_t::ADD_PREV,
	std::string(),
	bufferlist(),
	l
      });
  }

  void insert_init() {
    buffer.push_back(
      delta_t{
	delta_t::op_t::INIT,
	std::string(),
	bufferlist(),
	L_ADDR_NULL
      });
  }


  void replay(LogKVNodeLayout &node) {
    for (auto &i: buffer) {
      i.replay(node);
    }
  }

  void clear() {
    buffer.clear();
  }

  laddr_t get_latest_prev_leaf() {
    laddr_t l;
    for (auto &i: buffer) {
      if (i.op == delta_t::op_t::ADD_PREV) {
	l = i.prev;
      }
    }
    return l;
  }

  DENC(delta_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }

};
}
WRITE_CLASS_DENC(crimson::os::seastore::log_manager::delta_t)
WRITE_CLASS_DENC(crimson::os::seastore::log_manager::delta_buffer_t)

namespace crimson::os::seastore::log_manager{

constexpr uint32_t LOG_NODE_BLOCK_SIZE = 16384;

const std::string BEGIN_KEY = "";
const std::string END_KEY(64, (char)(-1));

inline constexpr uint32_t get_log_node_block_size() {
  return crimson::os::seastore::log_manager::LOG_NODE_BLOCK_SIZE;
}


struct log_context_t {
  TransactionManager &tm;
  Transaction &t;
  laddr_t hint;
};

struct LogNode;
using LogNodeRef = TCachedExtentRef<LogNode>;

struct log_key_t {
  uint16_t key_len = 0;
  uint16_t val_len = 0;

  log_key_t() = default;
  log_key_t(uint16_t k_len, uint16_t v_len)
  : key_len(k_len), val_len(v_len) {}

  DENC(log_key_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_len, p);
    denc(v.val_len, p);
    DENC_FINISH(p);
  }
};

struct log_key_le_t {
  ceph_le16 key_len{0};
  ceph_le16 val_len{0};

  log_key_le_t() = default;
  log_key_le_t(const log_key_le_t &) = default;
  explicit log_key_le_t(const log_key_t &key)
    : key_len(key.key_len),
      val_len(key.val_len) {}

  log_key_le_t& operator=(log_key_t key) {
    key_len = key.key_len;
    val_len = key.val_len;
    return *this;
  }


  operator log_key_t() const {
    return log_key_t{uint16_t(key_len),
                           uint16_t(val_len)};
  }
};


/**
 * LogKVNodeLayout
 *
 *  [ num_keys ][ prev pointer ][ last_pos ][ key entry #1 ][ value #1 ] ...
 *
 *  - num_keys:
 *      Total number of key-value pairs stored in this node.
 *
 *  - prev pointer (laddr):
 *      The location of the prev node (logical address).
 *      Set to NULL if this is the last node.
 *
 *  - last_pos:
 *      The offset position where the last key-value data ends in this node.
 *
 *  - key entry:
 *      Format: [ key_len ][ val_len ]
 *        - key_len:  Length of the key in bytes.
 *        - val_len:  Length of the value in bytes.
 *  - val entry:
 *  	Format: [ key_buf ][ val_buf ]
 *        - key_buf:  Raw key data.
 *        - val_buf:  Raw value data.
 *
 */

class LogKVNodeLayout {
  using LogKVNodeLayoutRef = boost::intrusive_ptr<LogKVNodeLayout>;
  char *buf;
  extent_len_t len = 0;

  uint32_t reserved_len = 0;
  using L = absl::container_internal::Layout<ceph_le32, laddr_le_t, ceph_le32, log_key_le_t>;
  static constexpr L layout{1, 1, 1, 1};
public:
  template <bool is_const>
  class iter_t {
    friend class LogKVNodeLayout;
    using parent_t = typename crimson::common::maybe_const_t<LogKVNodeLayout, is_const>::type;

    parent_t node;
    uint32_t pos;

    iter_t(
      parent_t parent,
      uint32_t pos) : node(parent), pos(pos) {}

  public:
    iter_t(const iter_t &) = default;
    iter_t(iter_t &&) = default;
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    operator iter_t<!is_const>() const {
      static_assert(!is_const);
      return iter_t<!is_const>(node, pos);
    }

    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    iter_t operator++(int) {
      auto ret = *this;
      auto last = get_node_key();
      auto new_pos = node->get_size() == 0 ? 0 :
	pos + node->get_entry_size(last.key_len, last.val_len);
      pos = new_pos;
      return ret;
    }

    iter_t &operator++() {
      auto last = get_node_key();
      auto new_pos = node->get_size() == 0 ? 0 :
	pos + node->get_entry_size(last.key_len, last.val_len);
      pos = new_pos;
      return *this;
    }

    bool operator==(const iter_t &rhs) const {
      assert(node == rhs.node);
      return rhs.pos == pos;
    }

    bool operator!=(const iter_t &rhs) const {
      assert(node == rhs.node);
      return pos != rhs.pos;
    }

  private:
    log_key_t get_node_key() const {
      log_key_le_t kint = *((log_key_le_t*)get_node_key_ptr());
      return log_key_t(kint);
    }
    auto get_node_key_ptr() const {
      return reinterpret_cast<
	typename crimson::common::maybe_const_t<char, is_const>::type>(
	  node->get_node_key_ptr()) + pos;
    }

    uint32_t get_node_val_offset() const {
      return get_node_key().key_off;
    }
    auto get_node_val_ptr() const {
      return get_node_key_ptr() + sizeof(log_key_t);
    }

    void set_node_key(log_key_t _lb) {
      static_assert(!is_const);
      log_key_le_t lb;
      lb = _lb;
      *((log_key_le_t*)get_node_key_ptr()) = lb;
    }

    void set_node_val(const std::string &key, const ceph::bufferlist &val) {
      static_assert(!is_const);
      auto node_key = get_node_key();
      assert(key.size() == node_key.key_len);
      assert(val.length() == node_key.val_len);
      ::memcpy(get_node_val_ptr(), key.data(), key.size());
      auto bliter = val.begin();
      bliter.copy(node_key.val_len, get_node_val_ptr() + node_key.key_len);
    }

  public:
    uint32_t get_pos() const {
      return pos;
    }

    std::string get_key() const {
      return std::string(
	get_node_val_ptr(),
	get_node_key().key_len);
    }

    ceph::bufferlist get_val() const {
      auto node_key = get_node_key();
      ceph::bufferlist bl;
      ceph::bufferptr bptr(
	get_node_val_ptr() + node_key.key_len,
	get_node_key().val_len);
      bl.append(bptr);
      return bl;
    }
  };
  
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

  uint32_t get_size() const {
    ceph_le32 &size = *layout.template Pointer<0>(buf);
    return uint32_t(size);
  }

  laddr_t get_prev() const {
    laddr_le_t &prev = *layout.template Pointer<1>(buf);
    return laddr_t(prev);
  }

  ceph_le32 *get_size_ptr() {
    return L::Partial(1, 1, 1, 1).template Pointer<0>(buf);
  }
  laddr_le_t *get_node_addr_ptr() {
    return L::Partial(1, 1, 1, 1).template Pointer<1>(buf);
  }
  ceph_le32 *get_last_pos_ptr() {
    return L::Partial(1, 1, 1, 1).template Pointer<2>(buf);
  }
  log_key_le_t *get_node_key_ptr() {
    return L::Partial(1, 1, 1, 1).template Pointer<3>(buf);
  }
  const log_key_le_t *get_node_key_ptr() const {
    return L::Partial(1, 1, 1, 1).template Pointer<3>(buf);
  }

  uint32_t get_start_off() const {
    return layout.Offset<3>();
  }

  const_iterator iter_cend() const {
    const_iterator prev_iter(this, get_last_pos());
    auto last = prev_iter->get_node_key();
    return const_iterator(this, get_size() == 0 ? get_last_pos() :
      get_last_pos() + get_entry_size(last.key_len, last.val_len));
  }
  const_iterator iter_end() const {
    return iter_cend();
  }

  iterator iter_begin() {
    return iterator(
	this,
	0);
  }

  const_iterator iter_begin() const {
    return iter_cbegin();
  }

  const_iterator iter_cbegin() const {
    return const_iterator(
	this,
	0);
  }

  iterator iter_end() {
    iterator prev_iter(this, get_last_pos());
    auto last = prev_iter->get_node_key();
    return iterator(this, get_size() == 0 ? get_last_pos() :
      get_last_pos() + get_entry_size(last.key_len, last.val_len));
  }

public:
  LogKVNodeLayout() : buf(nullptr) {}

  void set_layout_buf(char *_buf, extent_len_t _len) {
    assert(_len > 0);
    assert(buf == nullptr);
    assert(_buf != nullptr);
    buf = _buf;
    len = _len;
  }

  void set_prev_node(laddr_t laddr) {
    laddr_le_t l;
    l = laddr;
    *get_node_addr_ptr() = l;
  }

  void set_size(uint32_t size) {
    ceph_le32 v(size);
    *get_size_ptr() = v;
  }

  void set_last_pos(uint32_t pos) {
    ceph_assert(pos <= LOG_NODE_BLOCK_SIZE);
    ceph_le32 p;
    p = pos;
    *layout.template Pointer<2>(buf) = p;
  }

  uint32_t get_last_pos() const {
    ceph_le32 &pos = *layout.template Pointer<2>(buf);
    return uint32_t(pos);
  }

  void set_reserved_len(const uint32_t len) {
    reserved_len = len;
  }

  uint32_t get_reserved_len() const {
    return reserved_len;
  }

  uint16_t get_entry_size(size_t ksize, size_t vsize) const {
    return (sizeof(log_key_le_t) + ksize + vsize);
  }

  static uint16_t test_get_entry_size(size_t ksize, size_t vsize) {
    return (sizeof(log_key_le_t) + ksize + vsize);
  }

  uint32_t free_space() const {
    assert(capacity() >= used_space());
    return capacity() - used_space();
  }

  uint32_t capacity() const {
    return len
      - (reinterpret_cast<char*>(layout.template Pointer<3>(buf))
      - reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  uint32_t used_space() const {
    if (get_size() == 0) {
      return 0;
    }
    const_iterator iter(this, get_last_pos());
    auto k = iter->get_node_key();
    return get_last_pos() + get_entry_size(k.key_len, k.val_len);
  }

  void _append(const std::string &key, const ceph::bufferlist &val) {
    iterator prev_iter(this, get_last_pos());
    auto last = prev_iter->get_node_key();
    iterator next_iter(this, get_size() == 0 ? get_last_pos() :
      get_last_pos() + get_entry_size(last.key_len, last.val_len));
    next_iter.set_node_key(log_key_t(key.size(), val.length()));
    next_iter.set_node_val(key, val);
    if (get_size() >= 1) {
      set_last_pos(get_last_pos() + get_entry_size(last.key_len, last.val_len));
    }
    set_size(get_size() + 1);
  }


  void journal_append(
    const std::string &key,
    const ceph::bufferlist &val,
    delta_buffer_t *recorder) {
    recorder->insert(key, val);
    reserved_len += this->get_entry_size(key.size(), val.length());
  }

  void journal_append_prev_addr(
    const laddr_t l,
    delta_buffer_t *recorder) {
    recorder->insert_prev_addr(l);
  }

  void journal_append_init(
    delta_buffer_t *recorder) {
    recorder->insert_init();
  }

  void append(
    const std::string &key,
    const ceph::bufferlist &val) {
    _append(key, val);
  }

  void init_vars() {
    set_last_pos(0); 
    set_size(0);
    set_prev_node(L_ADDR_NULL);
    set_reserved_len(0);
  }

  bool expect_overflow(size_t ksize, size_t vsize) const {
    return free_space() < get_entry_size(ksize, vsize) + reserved_len;
  }

  bool is_overflow(size_t ksize, size_t vsize) const {
    return free_space() < get_entry_size(ksize, vsize);
  }

  bool is_overflow(const LogKVNodeLayout &rhs) const {
    return free_space() < rhs.used_space();
  }

  std::string get_last_key() const {
    const_iterator iter(this, get_last_pos());
    return iter->get_key();
  }
};

struct LogNode 
  : LogicalChildNode,
    LogKVNodeLayout {
  static constexpr extent_types_t TYPE = extent_types_t::LOG_NODE;
  explicit LogNode(ceph::bufferptr &&ptr) : LogicalChildNode(std::move(ptr)) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_prev_node(L_ADDR_NULL);
  }
  explicit LogNode(extent_len_t length) : LogicalChildNode(length) {}

  LogNode(const LogNode &rhs)
    : LogicalChildNode(rhs, share_buffer_t()) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_last_pos(*get_last_pos_ptr()); // shared buf
    set_size(get_size());
    set_reserved_len(rhs.get_reserved_len());
  }
  ~LogNode() {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LogNode(*this));
  }

  crimson::os::seastore::extent_types_t get_type() const {
    return extent_types_t::LOG_NODE;
  }

  ceph::bufferlist get_delta() {
    ceph::bufferlist bl;
    if (!delta_buffer.empty()) {
      encode(delta_buffer, bl);
    }
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) {
    assert(bl.length());
    delta_buffer_t buffer;
    auto bptr = bl.cbegin();
    decode(buffer, bptr);
    buffer.replay(*this);
  }

  mutable delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  void append_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val);

  /*
   *
   * set laddr directly if LogNode is not mutating
   * add laddr to delta_buffer if LogNode is mutating
   *
   */
  void set_prev_addr(laddr_t l);

  void set_init_vars();

  using get_value_ret = OMapManager::omap_get_value_ret;
  get_value_ret get_value(const std::string &key)
  {
    auto iter = iter_begin();
    while(iter != iter_end()) {
      if (iter->get_key() == key) {
	auto bl = iter->get_val();
	return get_value_ret(
	  interruptible::ready_future_marker{},
	  std::move(bl));
      }
      iter++;
    };
    return get_value_ret(
      interruptible::ready_future_marker{},
      std::nullopt);
  }

  const_iterator string_lower_bound(std::string_view str) const {
    auto iter = iter_begin();
    auto target = iter;
    if (get_last_key() < str) {
      return iter_end();
    }
    while(iter != iter_end()) {
      if (iter->get_key() < str) {
	target = iter;
      }
      iter++;
    };
    return target;
  }

  const_iterator string_upper_bound(std::string_view str) const {
    auto iter = iter_begin();
    while(iter != iter_end()) {
      if (iter->get_key() > str) {
	break;
      }
      iter++;
    };
    return iter; 
  }

  void list(const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    std::map<std::string, bufferlist> &kvs)
  {
    auto s = first ? string_upper_bound(*first) : iter_cbegin();
    auto e = last ? string_lower_bound(*last) : iter_cend();
    while (s != e) { 
      auto bl = s->get_val();
      kvs[s->get_key()] = bl;
      s++;
    };
  }

  bool last_is_less_than(const std::optional<std::string> &str) {
    if (get_size() == 0) {
      return false;
    }
    ceph_assert(str);
    std::string_view s(*str);
    if (s > get_last_key()) {
      return true;
    }
    return false;
  }

  bool first_is_larger_than(const std::optional<std::string> &str) {
    if (get_size() == 0) {
      return false;
    }
    assert(str);
    std::string_view s = *str;
    if (s < iter_begin()->get_key()) {
      return true;
    }
    return false;
  }

  std::ostream &print_detail_l(std::ostream &out) const final;

  laddr_t get_prev_addr() const {
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      if (!delta_buffer.empty()) {
	return delta_buffer.get_latest_prev_leaf();
      }
    }
    return this->get_prev();
  }

  uint32_t use_space() const {
    return this->used_space();
  }

  uint32_t get_capacity() const {
    return this->capacity();
  }

  void update_delta() {
    if (!delta_buffer.empty()) {
      delta_buffer.replay(*this);
      delta_buffer.clear();
    }
  }

  void logical_on_delta_write() final {
    update_delta();
    set_reserved_len(0);
  }

  // TODO: consistent view in a transaction
  void prepare_commit() final {
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      ceph_assert(!delta_buffer.empty());
      update_delta();
    } else {
      assert(delta_buffer.empty());
    }
  }

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
  }

  void init_range(std::string _begin, std::string _end) {
    assert(begin.empty());
    assert(end.empty());
    begin = std::move(_begin);
    end = std::move(_end);
  }

  std::string begin;
  std::string end;
};

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::log_manager::log_key_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::log_manager::LogNode> : fmt::ostream_formatter {};
#endif

