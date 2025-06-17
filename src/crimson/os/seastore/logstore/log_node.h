// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

//#include "crimson/common/log.h"
#include "include/denc.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/omap_manager.h"


namespace crimson::os::seastore::logstore_manager{

struct LogKVNodeLayout;
struct delta_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    UPDATE,
    REMOVE,
    ADD_HEAD,
  } op;
  std::string key;
  ceph::bufferlist val;
  laddr_t next;

  DENC(delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.val, p);
    denc(v.next, p);
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
  void insert_head_addr(
      const laddr_t l) {
    buffer.push_back(
      delta_t{
	delta_t::op_t::ADD_HEAD,
	std::string(),
	bufferlist(),
	l
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

  laddr_t get_latest_head_leaf() {
    laddr_t l;
    for (auto &i: buffer) {
      if (i.op == delta_t::op_t::ADD_HEAD) {
	l = i.next;
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

struct LogKVLeafNodeLayout;
struct delta_leaf_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    REMOVE,
    ADD_NEXT,
  } op;
  std::string key;
  ceph::bufferlist val;
  laddr_t next;

  DENC(delta_leaf_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.val, p);
    denc(v.next, p);
    DENC_FINISH(p);
  }

  void replay(LogKVLeafNodeLayout &l);
};

class delta_leaf_buffer_t {
  std::vector<delta_leaf_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }
  void insert(
    const std::string &key,
    const ceph::bufferlist &val) {
    buffer.push_back(
      delta_leaf_t{
        delta_leaf_t::op_t::INSERT,
        key,
        val
      });
  }
  void remove(const std::string &key) {
    buffer.push_back(
      delta_leaf_t{
	delta_leaf_t::op_t::REMOVE,
	key,
	bufferlist()
      });
  }
  void insert_next_addr(
      const laddr_t l) {
    buffer.push_back(
      delta_leaf_t{
	delta_leaf_t::op_t::ADD_NEXT,
	std::string(),
	bufferlist(),
	l
      });
  }

  void replay(LogKVLeafNodeLayout &node) {
    for (auto &i: buffer) {
      i.replay(node);
    }
  }

  void clear() {
    buffer.clear();
  }

  laddr_t get_latest_next_leaf() {
    laddr_t l;
    for (auto &i: buffer) {
      if (i.op == delta_leaf_t::op_t::ADD_NEXT) {
	l = i.next;
      }
    }
    return l;
  }

  DENC(delta_leaf_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }

};
}
WRITE_CLASS_DENC(crimson::os::seastore::logstore_manager::delta_leaf_t)
WRITE_CLASS_DENC(crimson::os::seastore::logstore_manager::delta_leaf_buffer_t)
WRITE_CLASS_DENC(crimson::os::seastore::logstore_manager::delta_t)
WRITE_CLASS_DENC(crimson::os::seastore::logstore_manager::delta_buffer_t)

namespace crimson::os::seastore::logstore_manager{

#define MAX_NODE_ENTRY 10
constexpr uint32_t LOG_NODE_BLOCK_SIZE = 8192;
constexpr uint32_t LOG_LEAF_NODE_BLOCK_SIZE = 16384;

const std::string BEGIN_KEY = "";
const std::string END_KEY(64, (char)(-1));

inline constexpr uint32_t get_log_leaf_node_block_size() {
  return crimson::os::seastore::logstore_manager::LOG_LEAF_NODE_BLOCK_SIZE;
}


struct log_context_t {
  TransactionManager &tm;
  Transaction &t;
  laddr_t hint;
};

struct LogNode;
using LogNodeRef = TCachedExtentRef<LogNode>;
struct LogLeafNode;
using LogLeafNodeRef = TCachedExtentRef<LogLeafNode>;

class LogStoreManager {
public:
  using base_iertr = TransactionManager::base_iertr;
  using initialize_lsm_iertr = base_iertr;
  using initialize_lsm_ret = initialize_lsm_iertr::future<omap_root_t>;
  LogStoreManager(TransactionManager &tm);
  initialize_lsm_ret initialize_lsm(Transaction &t, laddr_t hint);

  using log_set_keys_iertr = base_iertr;
  using log_set_keys_ret = log_set_keys_iertr::future<>;
  log_set_keys_ret log_set_keys(omap_root_t &omap_root,
    Transaction &t, std::map<std::string, ceph::bufferlist>&& kv);

  using log_set_key_iertr = base_iertr;
  using log_set_key_ret = log_set_key_iertr::future<>;
  log_set_key_ret log_set_key(omap_root_t &omap_root,
    Transaction &t, LogNodeRef e, const std::string &key,
    const ceph::bufferlist &value);

  using log_get_value_iertr = base_iertr;
  using log_get_value_ret = log_get_value_iertr::future<
    std::optional<bufferlist>>;
  log_get_value_ret
  log_get_value(const omap_root_t &omap_root, Transaction &t,
    const std::string &key);

  using log_list_iertr = base_iertr;
  using log_list_bare_ret = std::tuple<
    bool,
    std::map<std::string, bufferlist, std::less<>>>;
  using log_list_ret = log_list_iertr::future<log_list_bare_ret>;
  log_list_ret omap_list(
    const omap_root_t &log_root,
    Transaction &t,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    OMapManager::omap_list_config_t config =
    OMapManager::omap_list_config_t());


  using log_rm_key_range_iertr = base_iertr;
  using log_rm_key_range_ret = log_rm_key_range_iertr::future<>;
  log_rm_key_range_ret log_rm_key_range(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &first,
    const std::string &last,
    OMapManager::omap_list_config_t config);


  using log_rm_key_iertr = base_iertr;
  using log_rm_key_ret = log_rm_key_iertr::future<>;
  log_rm_key_ret log_rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key);

  log_rm_key_ret remove_kvs(Transaction &t, laddr_t dst,
    const std::string &first, const std::string &last,
    LogNodeRef root);

  log_list_iertr::future<>
  find_kvs(Transaction &t, laddr_t dst, const std::optional<std::string> &first,
    const std::optional<std::string> &last, std::map<std::string, bufferlist> &kvs);

  log_context_t get_log_context(
    Transaction &t, const omap_root_t &omap_root) {
    ceph_assert(omap_root.type < omap_type_t::NONE);
    return log_context_t{tm, t, omap_root.hint};
  }

  using log_load_extent_iertr = base_iertr;
  template <typename T>
  requires std::is_same_v<LogNode, T> || std::is_same_v<LogLeafNode, T>
  log_load_extent_iertr::future<TCachedExtentRef<T>> log_load_extent(
    Transaction &t, laddr_t laddr, std::string begin, std::string end);

  template <typename T>
  requires std::is_same_v<LogNode, T> || std::is_same_v<LogLeafNode, T>
  log_load_extent_iertr::future<TCachedExtentRef<T>> find_tail(
    Transaction &t, laddr_t dst);

  bool can_handle_by_lognode(std::string s) {
    // TODO: handle may_
    return (s[0] == '_' || s.substr(0, 4) == std::string("may_"));
  }

  bool can_handle_by_logleaf(std::string s) {
    pg_log_entry_t e;
    return (s.size() == e.get_key_name().size() &&
	(s[0] >= (0 + '0') && s[0] <= (9 + '0')));
  }
  log_get_value_ret find_kv(Transaction &t, laddr_t dst, const std::string &key);


  TransactionManager &tm;
};

struct log_key_t {
  uint16_t key_off = 0;
  uint16_t key_len = 0;
  uint16_t val_len = 0;

  log_key_t() = default;
  log_key_t(uint16_t k_off, uint16_t k_len, uint16_t v_len)
  : key_off(k_off), key_len(k_len), val_len(v_len) {}

  DENC(log_key_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_off, p);
    denc(v.key_len, p);
    denc(v.val_len, p);
    DENC_FINISH(p);
  }
};

struct log_key_le_t {
  ceph_le16 key_off{0};
  ceph_le16 key_len{0};
  ceph_le16 val_len{0};

  log_key_le_t() = default;
  log_key_le_t(const log_key_le_t &) = default;
  explicit log_key_le_t(const log_key_t &key)
    : key_off(key.key_off),
      key_len(key.key_len),
      val_len(key.val_len) {}

  log_key_le_t& operator=(log_key_t key) {
    key_off = key.key_off;
    key_len = key.key_len;
    val_len = key.val_len;
    return *this;
  }


  operator log_key_t() const {
    return log_key_t{uint16_t(key_off), uint16_t(key_len),
                      uint16_t(val_len)};
  }
};

/**
 *
 *  LogKVNodeLayout
 *
 *
 *  num_keys | head pointer |  the remaing part is the same as StringKVLeafNodeLayout
 *             laddr/paddr	
 *
 *
 */
template <typename iterator>
static void copy_from_local(
  unsigned len,
  iterator tgt,
  iterator from_src,
  iterator to_src) {
  assert(tgt->node == from_src->node);
  assert(to_src->node == from_src->node);

  auto to_copy = from_src->get_right_ptr_end() - to_src->get_right_ptr_end();
  assert(to_copy > 0);
  int adjust_offset = tgt > from_src? -len : len;
  memmove(to_src->get_right_ptr_end() + adjust_offset,
          to_src->get_right_ptr_end(),
          to_copy);

  for ( auto ite = from_src; ite < to_src; ite++) {
      ite->update_offset(-adjust_offset);
  }
  memmove(tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
          to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
}

class LogKVNodeLayout {
  using LogKVNodeLayoutRef = boost::intrusive_ptr<LogKVNodeLayout>;
  char *buf;
  extent_len_t len = 0;

  uint32_t reserved_len = 0;
  using L = absl::container_internal::Layout<ceph_le16, laddr_le_t, log_key_le_t>;
  static constexpr L layout{1, 1, 1};
public:
  template <bool is_const>
  class iter_t {
    friend class LogKVNodeLayout;
    using parent_t = typename crimson::common::maybe_const_t<LogKVNodeLayout, is_const>::type;

    template <typename iterator>
    friend void copy_from_local(unsigned, iterator, iterator, iterator);

    parent_t node;
    uint16_t index;

    iter_t(
      parent_t parent,
      uint16_t index) : node(parent), index(index) {}

  public:
    iter_t(const iter_t &) = default;
    iter_t(iter_t &&) = default;
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    operator iter_t<!is_const>() const {
      static_assert(!is_const);
      return iter_t<!is_const>(node, index);
    }

    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    iter_t operator++(int) {
      auto ret = *this;
      ++index;
      return ret;
    }

    iter_t &operator++() {
      ++index;
      return *this;
    }

    iter_t operator+(uint16_t off) const {
      return iter_t(node, index + off);
    }

    iter_t operator-(uint16_t off) const {
      ceph_assert(index > 0);
      ceph_assert(index >= off);
      return iter_t(node, index - off);
    }

    uint16_t operator<(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index < rhs.index;
    }

    uint16_t operator>(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index > rhs.index;
    }

    friend bool operator==(const iter_t &lhs, const iter_t &rhs) {
      assert(lhs.node == rhs.node);
      return lhs.index == rhs.index;
    }

  private:
    log_key_t get_node_key() const {
      log_key_le_t kint = node->get_node_key_ptr()[index];
      return log_key_t(kint);
    }
    auto get_node_key_ptr() const {
      return reinterpret_cast<
	typename crimson::common::maybe_const_t<char, is_const>::type>(
	  node->get_node_key_ptr() + index);
    }

    uint32_t get_node_val_offset() const {
      return get_node_key().key_off;
    }
    auto get_node_val_ptr() const {
      auto tail = node->buf + node->len;
      if (*this == node->iter_end())
        return tail;
      else {
        return tail - get_node_val_offset();
      }
    }

    int get_right_offset_end() const {
      if (index == 0)
        return 0;
      else
        return (*this - 1)->get_node_val_offset();
    }

    auto get_right_ptr_end() const {
      return node->buf + node->len - get_right_offset_end();
    }

    void update_offset(int offset) {
      auto key = get_node_key();
      assert(offset + key.key_off >= 0);
      key.key_off += offset;
      set_node_key(key);
    }

    void set_node_key(log_key_t _lb) const {
      static_assert(!is_const);
      log_key_le_t lb;
      lb = _lb;
      node->get_node_key_ptr()[index] = lb;
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
    uint16_t get_index() const {
      return index;
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

  uint16_t get_size() const {
    ceph_le16 &size = *layout.template Pointer<0>(buf);
    return uint16_t(size);
  }

  laddr_t get_head_addr() const {
    laddr_le_t &next = *layout.template Pointer<1>(buf);
    return laddr_t(next);
  }

  laddr_le_t *get_head_leaf_ptr() {
    return L::Partial(1, 1, 1).template Pointer<1>(buf);
  }
  log_key_le_t *get_node_key_ptr() {
    return L::Partial(1, 1, get_size()).template Pointer<2>(buf);
  }
  const log_key_le_t *get_node_key_ptr() const {
    return L::Partial(1, 1, get_size()).template Pointer<2>(buf);
  }

  const_iterator iter_cend() const {
    return const_iterator(
      this,
      get_size());
  }
  const_iterator iter_end() const {
    return iter_cend();
  }

  iterator iter_begin() {
    return iterator(
	this,
	0);
  }

  const_iterator iter_cbegin() const {
    return const_iterator(
	this,
	0);
  }

  const_iterator iter_begin() const {
    return iter_cbegin();
  }

  iterator iter_end() {
    return iterator(
	this,
	get_size());
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

  void set_head_leaf(laddr_t laddr) {
    laddr_le_t l;
    l = laddr;
    *get_head_leaf_ptr() = l;
  }

  void set_size(uint16_t size) {
    ceph_le16 s;
    s = size;
    *layout.template Pointer<0>(buf) = s;
  }

  laddr_t get_head_leaf() const {
    laddr_le_t &l = *layout.template Pointer<1>(buf);
    return laddr_t(l);
  }

  uint32_t used_space() const {
    uint32_t count = get_size();
    if (count) {
      log_key_t last_key = log_key_t(get_node_key_ptr()[count-1]);
      return last_key.key_off + count * sizeof(log_key_le_t);
    } else {
      return 0;
    }
  }

  bool is_overflow(size_t ksize, size_t vsize) const {
    return free_space() < (sizeof(log_key_le_t) + ksize + vsize);
  }

  uint32_t free_space() const {
    return capacity() - used_space();
  }

  uint32_t capacity() const {
    return len
      - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))
      - reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  const_iterator find_string_key(std::string str) const {
    const_iterator iter(this, 0);
    for (;iter != iter_end(); iter++) {
      if (iter->get_key() == str) {
	return iter;
      }
    }
    return iter_cend();
  }

  iterator find_string_key(std::string str) {
    uint16_t index = 0;
    iterator iter = iter_begin();
    for (;iter != iter_end(); iter++) {
      if (iter->get_key() == str) {
	index = iter.index;
	return iterator(this, index);
      }
    }
    return iter_end();
  }

  void journal_insert(
    const_iterator _iter,
    const std::string &key,
    const ceph::bufferlist &val,
    delta_buffer_t *recorder) {
    LOG_PREFIX(LogKVNodeLayout:journal_insert);
    SUBDEBUG(seastore_t, "index={}, key={}", _iter.index, key);
    if (recorder) {
      recorder->insert(
	key,
	val);
      reserved_len += this->get_entry_size(key.size(), val.length());
      return;
    }
    auto iter = iterator(this, _iter.index);
    _insert(iter, key, val);
  }

  void journal_update(
    const_iterator _iter,
    const std::string &key,
    const ceph::bufferlist &val,
    delta_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    LOG_PREFIX(LogKVNodeLayout::journal_update);
    SUBDEBUG(seastore_t, "index={}, key={}", _iter.index, iter->get_key());
    if (recorder) {
      recorder->remove(iter->get_key());
      recorder->insert(key, val);
    }
    _update(iter, key, val);
    return;
  }

  void journal_remove(
    const_iterator _iter,
    delta_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_key());
      return;
    }
    _remove(iter);
  }

  void _insert(
    iterator iter,
    const std::string &key,
    const bufferlist &val) {
    LOG_PREFIX(LogKVNodeLayout::_insert);
    SUBDEBUG(seastore_t, "index {}", iter->get_index());
    if (iter != iter_begin()) {
      assert((iter - 1)->get_key() < key);
    }
    if (iter != iter_end()) {
      assert(iter->get_key() > key);
    }
    assert(!is_overflow(key.size(), val.length()));
    log_key_t node_key;
    if (iter == iter_begin()) {
      node_key.key_off = key.size() + val.length();
      node_key.key_len = key.size();
      node_key.val_len = val.length();
    } else {
      node_key.key_off = (iter - 1)->get_node_key().key_off +
	(key.size() + val.length());
      node_key.key_len = key.size();
      node_key.val_len = val.length();
    }
    if (get_size() != 0 && iter != iter_end())
      copy_from_local(node_key.key_len + node_key.val_len, iter + 1, iter, iter_end());

    iter->set_node_key(node_key);
    set_size(get_size() + 1);
    iter->set_node_val(key, val);
  }

  void _update(
    iterator iter,
    const std::string &key,
    const ceph::bufferlist &val) {
    assert(iter != iter_end());
    _remove(iter);
    assert(!is_overflow(key.size(), val.length()));
    _insert(iter, key, val);
  }

  void _remove(iterator iter) {
    assert(iter != iter_end());
    if ((iter + 1) != iter_end()) {
      log_key_t key = iter->get_node_key();
      copy_from_local(key.key_len + key.val_len, iter, iter + 1, iter_end());
    }
    set_size(get_size() - 1);
  }

  uint16_t get_entry_size(size_t ksize, size_t vsize) const {
    return (sizeof(log_key_le_t) + ksize + vsize);
  }

  bool expect_overflow(size_t ksize, size_t vsize) const {
    return free_space() < get_entry_size(ksize, vsize) + reserved_len;
  }

  void set_reserved_len(const uint32_t len) {
    reserved_len = len;
  }

  uint32_t get_reserved_len() const {
    return reserved_len;
  }
};


class LogStoreManager;
struct LogNode : LogicalChildNode, LogKVNodeLayout {
  static constexpr extent_types_t TYPE = extent_types_t::LOG_NODE;
  explicit LogNode(ceph::bufferptr &&ptr) : LogicalChildNode(std::move(ptr)) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    // TODO: recheck 
    set_head_leaf(L_ADDR_NULL);
  }
  explicit LogNode(extent_len_t length) : LogicalChildNode(length) {}

  ~LogNode() {}
  
  laddr_t tail_laddr = L_ADDR_NULL;
  paddr_t tail_paddr = P_ADDR_NULL;

  LogNode(const LogNode &rhs)
    : LogicalChildNode(rhs, share_buffer_t()) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_head_leaf(rhs.get_head_leaf_laddr());
    tail_laddr = rhs.tail_laddr;
    tail_paddr = rhs.tail_paddr;
    set_reserved_len(rhs.get_reserved_len());
  }

  CachedExtentRef duplicate_for_write(Transaction &t) {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LogNode(*this));
  }

  crimson::os::seastore::extent_types_t get_type() const
  {
    return extent_types_t::LOG_NODE;
  }

  ceph::bufferlist get_delta() {
    ceph::bufferlist bl;
    if (!delta_buffer.empty()) {
      encode(delta_buffer, bl);
    }
    return bl;
  }

  void update_delta() {
    if (!delta_buffer.empty()) {
      delta_buffer.replay(*this);
      delta_buffer.clear();
    }
  }

  void apply_delta(const ceph::bufferlist &bl) {
    assert(bl.length());
    delta_buffer_t buffer;
    auto bptr = bl.cbegin();
    decode(buffer, bptr);
    buffer.replay(*this);
  }

  void logical_on_delta_write() final {
    update_delta();
    set_reserved_len(0);

  }

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
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

  std::ostream &print_detail_l(std::ostream &out) const final;

  void init_range(std::string _begin, std::string _end) {
    assert(begin.empty());
    assert(end.empty());
    begin = std::move(_begin);
    end = std::move(_end);
  }

  std::string begin;
  std::string end;

  mutable delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  void insert_kv(const std::string &key, const bufferlist val) {
    const_iterator iter = find_string_key(key);
    ceph_assert(iter->get_index() < MAX_NODE_ENTRY);
    if (iter != iter_cend()) {
      journal_update(iter, key, val, maybe_get_delta_buffer());
      return;
    }
    journal_insert(iter, key, val, maybe_get_delta_buffer());
  }

  void remove_kv(const std::string &key) {
    const_iterator iter = find_string_key(key);
    if (iter != iter_cend()) {
      journal_remove(iter, maybe_get_delta_buffer());
      return;
    }
  }

  using get_value_ret = LogStoreManager::log_get_value_ret;
  get_value_ret get_value(const std::string &key)
  {
    auto iter = find_string_key(key);
    if (iter != iter_end()) {
      auto value = iter->get_val();
      return get_value_ret(
        interruptible::ready_future_marker{},
        std::move(value));
    } else {
      return get_value_ret(
        interruptible::ready_future_marker{},
        std::nullopt);
    }
  }

  void list(const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    std::map<std::string, bufferlist> &kvs)
  {
    bool s = first ? false : true;
    bool e = last ? false : true;

    auto iter = iter_begin();
    while(iter != iter_end()) {
      if (s && e) {
	auto bl = iter->get_val();
	kvs[iter->get_key()] = bl;
      } else if (e) {
	std::string_view str(*first);
	if (str < iter->get_key()) {
	  kvs[iter->get_key()] = iter->get_val();
	}
      } else if (s) {
	std::string_view str(*last);
	if (str > iter->get_key()) {
	  kvs[iter->get_key()] = iter->get_val();
	}
      } else {
	std::string_view f(*first);
	std::string_view l(*last);
	if (iter->get_key() > f && iter->get_key() < l) {
	  kvs[iter->get_key()] = iter->get_val();
	}
      }
      iter++;
    }
  }

  laddr_t get_head_leaf_laddr() const {
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      if (!delta_buffer.empty()) {
	return delta_buffer.get_latest_head_leaf();
      }
    }
    return get_head_addr();
  }

  void append_head_laddr(laddr_t l) {
    auto p = maybe_get_delta_buffer();
    if (p) {
      p->insert_head_addr(l);
      return;
    }
    set_head_leaf(l);
  }

  // TODO: This must be called under mutation
  void set_tail_addrs(paddr_t _p, laddr_t _l) {
    tail_paddr = _p;
    tail_laddr = _l;
  }

};

struct log_leaf_key_t {
  uint16_t key_len = 0;
  uint16_t val_len = 0;

  log_leaf_key_t() = default;
  log_leaf_key_t(uint16_t k_len, uint16_t v_len)
  : key_len(k_len), val_len(v_len) {}

  DENC(log_leaf_key_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.key_len, p);
    denc(v.val_len, p);
    DENC_FINISH(p);
  }
};

struct log_leaf_key_le_t {
  ceph_le16 key_len{0};
  ceph_le16 val_len{0};

  log_leaf_key_le_t() = default;
  log_leaf_key_le_t(const log_leaf_key_le_t &) = default;
  explicit log_leaf_key_le_t(const log_leaf_key_t &key)
    : key_len(key.key_len),
      val_len(key.val_len) {}

  log_leaf_key_le_t& operator=(log_leaf_key_t key) {
    key_len = key.key_len;
    val_len = key.val_len;
    return *this;
  }


  operator log_leaf_key_t() const {
    return log_leaf_key_t{uint16_t(key_len),
                           uint16_t(val_len)};
  }
};

/**
 *
 *  LogKVLeafNodeLayout
 *
 *
 *  num_keys | next pointer | last_pos |   key entry #1    |      val #1       |
 *             laddr/paddr 
 *                          	       | key len | val len | key buf | val buf |
 *
 *
 */

class LogKVLeafNodeLayout {
  using LogKVLeafNodeLayoutRef = boost::intrusive_ptr<LogKVLeafNodeLayout>;
  char *buf;
  extent_len_t len = 0;

  uint32_t reserved_len = 0;
  using L = absl::container_internal::Layout<ceph_le32, laddr_le_t, ceph_le32, log_leaf_key_le_t>;
  static constexpr L layout{1, 1, 1, 1};
public:
  template <bool is_const>
  class iter_t {
    friend class LogKVLeafNodeLayout;
    using parent_t = typename crimson::common::maybe_const_t<LogKVLeafNodeLayout, is_const>::type;

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
    log_leaf_key_t get_node_key() const {
      log_leaf_key_le_t kint = *((log_leaf_key_le_t*)get_node_key_ptr());
      return log_leaf_key_t(kint);
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
      return get_node_key_ptr() + sizeof(log_leaf_key_t);
    }

    void set_node_key(log_leaf_key_t _lb) {
      static_assert(!is_const);
      log_leaf_key_le_t lb;
      lb = _lb;
      *((log_leaf_key_le_t*)get_node_key_ptr()) = lb;
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

  laddr_t get_next() const {
    laddr_le_t &next = *layout.template Pointer<1>(buf);
    return laddr_t(next);
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
  log_leaf_key_le_t *get_node_key_ptr() {
    return L::Partial(1, 1, 1, 1).template Pointer<3>(buf);
  }
  const log_leaf_key_le_t *get_node_key_ptr() const {
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
  LogKVLeafNodeLayout() : buf(nullptr) {}

  void set_layout_buf(char *_buf, extent_len_t _len) {
    assert(_len > 0);
    assert(buf == nullptr);
    assert(_buf != nullptr);
    buf = _buf;
    len = _len;
  }

  void set_next_node(laddr_t laddr) {
    laddr_le_t l;
    l = laddr;
    *get_node_addr_ptr() = l;
  }

  void set_size(uint32_t size) {
    ceph_le32 v(size);
    *get_size_ptr() = v;
  }

  void set_last_pos(uint32_t pos) {
    ceph_assert(pos <= LOG_LEAF_NODE_BLOCK_SIZE);
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
    return (sizeof(log_leaf_key_le_t) + ksize + vsize);
  }

  static uint16_t test_get_entry_size(size_t ksize, size_t vsize) {
    return (sizeof(log_leaf_key_le_t) + ksize + vsize);
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
    next_iter.set_node_key(log_leaf_key_t(key.size(), val.length()));
    next_iter.set_node_val(key, val);
    if (get_size() >= 1) {
      set_last_pos(get_last_pos() + get_entry_size(last.key_len, last.val_len));
    }
    set_size(get_size() + 1);
  }


  void journal_append(
    const std::string &key,
    const ceph::bufferlist &val,
    delta_leaf_buffer_t *recorder) {
    recorder->insert(key, val);
    reserved_len += this->get_entry_size(key.size(), val.length());
  }

  void journal_append(
    const laddr_t l,
    delta_leaf_buffer_t *recorder) {
    recorder->insert_next_addr(l);
  }

  void append(
    const std::string &key,
    const ceph::bufferlist &val) {
    _append(key, val);
  }

  void append(const laddr_t l) {
    set_next_node(l);
  }

  bool expect_overflow(size_t ksize, size_t vsize) const {
    return free_space() < get_entry_size(ksize, vsize) + reserved_len;
  }

  bool is_overflow(size_t ksize, size_t vsize) const {
    return free_space() < get_entry_size(ksize, vsize);
  }

  bool is_overflow(const LogKVLeafNodeLayout &rhs) const {
    return free_space() < rhs.used_space();
  }

  std::string get_last_key() const {
    const_iterator iter(this, get_last_pos());
    return iter->get_key();
  }
};

struct LogLeafNode 
  : LogicalChildNode,
    LogKVLeafNodeLayout {
  static constexpr extent_types_t TYPE = extent_types_t::LOG_LEAF;
  explicit LogLeafNode(ceph::bufferptr &&ptr) : LogicalChildNode(std::move(ptr)) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_next_node(L_ADDR_NULL);
  }
  explicit LogLeafNode(extent_len_t length) : LogicalChildNode(length) {}

  LogLeafNode(const LogLeafNode &rhs)
    : LogicalChildNode(rhs, share_buffer_t()) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_last_pos(*get_last_pos_ptr()); // shared buf
    set_size(get_size());
    set_reserved_len(rhs.get_reserved_len());
  }
  ~LogLeafNode() {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new LogLeafNode(*this));
  }

  crimson::os::seastore::extent_types_t get_type() const {
    return extent_types_t::LOG_LEAF;
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
    delta_leaf_buffer_t buffer;
    auto bptr = bl.cbegin();
    decode(buffer, bptr);
    buffer.replay(*this);
  }

  mutable delta_leaf_buffer_t delta_buffer;
  delta_leaf_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  void append_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val);

  void append_next_addr(laddr_t l);

  void set_next_leaf(laddr_t addr) {
    set_next_node(addr);
  }

  using get_value_ret = LogStoreManager::log_get_value_ret;
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

  laddr_t get_next_leaf_addr() const {
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      if (!delta_buffer.empty()) {
	return delta_buffer.get_latest_next_leaf();
      }
    }
    return this->get_next();
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
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::logstore_manager::log_leaf_key_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::logstore_manager::LogNode> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::logstore_manager::LogLeafNode> : fmt::ostream_formatter {};
#endif

