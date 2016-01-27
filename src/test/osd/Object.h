// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/interval_set.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <list>
#include <map>
#include <set>
#include <random>

#ifndef OBJECT_H
#define OBJECT_H

class ContDesc {
public:
  int objnum;
  int cursnap;
  unsigned seqnum;
  std::string prefix;
  std::string oid;

  ContDesc() :
    objnum(0), cursnap(0),
    seqnum(0), prefix("") {}

  ContDesc(int objnum,
	   int cursnap,
	   unsigned seqnum,
	   const std::string &prefix) :
    objnum(objnum), cursnap(cursnap),
    seqnum(seqnum), prefix(prefix) {}

  bool operator==(const ContDesc &rhs) {
    return (rhs.objnum == objnum &&
	    rhs.cursnap == cursnap &&
	    rhs.seqnum == seqnum &&
	    rhs.prefix == prefix &&
	    rhs.oid == oid);
  }

  bool operator<(const ContDesc &rhs) const {
    return seqnum < rhs.seqnum;
  }

  bool operator!=(const ContDesc &rhs) {
    return !((*this) == rhs);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bp);
};
WRITE_CLASS_ENCODER(ContDesc)

std::ostream &operator<<(std::ostream &out, const ContDesc &rhs);

class ContentsGenerator {
public:

  class iterator_impl {
  public:
    virtual char operator*() = 0;
    virtual iterator_impl &operator++() = 0;
    virtual void seek(uint64_t pos) = 0;
    virtual bool end() = 0;
    virtual ContDesc get_cont() const = 0;
    virtual uint64_t get_pos() const = 0;
    virtual bufferlist gen_bl_advance(uint64_t s) {
      bufferptr ret = buffer::create(s);
      for (uint64_t i = 0; i < s; ++i, ++(*this)) {
	ret[i] = **this;
      }
      bufferlist _ret;
      _ret.push_back(ret);
      return _ret;
    }
    virtual bool check_bl_advance(bufferlist &bl, uint64_t *off = nullptr) {
      uint64_t _off = 0;
      for (bufferlist::iterator i = bl.begin();
	   !i.end();
	   ++i, ++_off, ++(*this)) {
	if (*i != **this) {
	  if (off)
	    *off = _off;
	  return false;
	}
      }
      return true;
    }
    virtual ~iterator_impl() {};
  };

  class iterator {
  public:
    ContentsGenerator *parent;
    iterator_impl *impl;
    char operator *() { return **impl; }
    iterator &operator++() { ++(*impl); return *this; };
    void seek(uint64_t pos) { impl->seek(pos); }
    bool end() { return impl->end(); }
    ~iterator() { parent->put_iterator_impl(impl); }
    iterator(const iterator &rhs) : parent(rhs.parent) {
      impl = parent->dup_iterator_impl(rhs.impl);
    }
    iterator &operator=(const iterator &rhs) {
      iterator new_iter(rhs);
      swap(new_iter);
      return *this;
    }
    void swap(iterator &other) {
      ContentsGenerator *otherparent = other.parent;
      other.parent = parent;
      parent = otherparent;

      iterator_impl *otherimpl = other.impl;
      other.impl = impl;
      impl = otherimpl;
    }
    bufferlist gen_bl_advance(uint64_t s) {
      return impl->gen_bl_advance(s);
    }
    bool check_bl_advance(bufferlist &bl, uint64_t *off = nullptr) {
      return impl->check_bl_advance(bl, off);
    }
    iterator(ContentsGenerator *parent, iterator_impl *impl) :
      parent(parent), impl(impl) {}
  };

  virtual uint64_t get_length(const ContDesc &in) = 0;

  virtual void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) = 0;
  void get_ranges(const ContDesc &cont, interval_set<uint64_t> &out) {
    std::map<uint64_t, uint64_t> ranges;
    get_ranges_map(cont, ranges);
    for (std::map<uint64_t, uint64_t>::iterator i = ranges.begin();
	 i != ranges.end();
	 ++i) {
      out.insert(i->first, i->second);
    }
  }


  virtual iterator_impl *get_iterator_impl(const ContDesc &in) = 0;

  virtual iterator_impl *dup_iterator_impl(const iterator_impl *in) = 0;

  virtual void put_iterator_impl(iterator_impl *in) = 0;

  virtual ~ContentsGenerator() {};

  iterator get_iterator(const ContDesc &in) {
    return iterator(this, get_iterator_impl(in));
  }
};

class RandGenerator : public ContentsGenerator {
public:
  typedef std::minstd_rand0 RandWrap;

  class iterator_impl : public ContentsGenerator::iterator_impl {
  public:
    uint64_t pos;
    ContDesc cont;
    RandWrap rand;
    RandGenerator *cont_gen;
    char current;
    iterator_impl(const ContDesc &cont, RandGenerator *cont_gen) : 
      pos(0), cont(cont), rand(cont.seqnum), cont_gen(cont_gen) {
      current = rand();
    }

    ContDesc get_cont() const { return cont; }
    uint64_t get_pos() const { return pos; }

    iterator_impl &operator++() {
      pos++;
      current = rand();
      return *this;
    }

    char operator*() {
      return current;
    }

    void seek(uint64_t _pos) {
      if (_pos < pos) {
	iterator_impl begin = iterator_impl(cont, cont_gen);
	begin.seek(_pos);
	*this = begin;
      }
      while (pos < _pos) {
	++(*this);
      }
    }

    bool end() {
      return pos >= cont_gen->get_length(cont);
    }
  };

  ContentsGenerator::iterator_impl *get_iterator_impl(const ContDesc &in) {
    RandGenerator::iterator_impl *i = new iterator_impl(in, this);
    return i;
  }

  void put_iterator_impl(ContentsGenerator::iterator_impl *in) {
    delete in;
  }

  ContentsGenerator::iterator_impl *dup_iterator_impl(
    const ContentsGenerator::iterator_impl *in) {
    ContentsGenerator::iterator_impl *retval = get_iterator_impl(in->get_cont());
    retval->seek(in->get_pos());
    return retval;
  }
};

class VarLenGenerator : public RandGenerator {
  uint64_t max_length;
  uint64_t min_stride_size;
  uint64_t max_stride_size;
public:
  VarLenGenerator(
    uint64_t length, uint64_t min_stride_size, uint64_t max_stride_size) :
    max_length(length),
    min_stride_size(min_stride_size),
    max_stride_size(max_stride_size) {}
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out);
  uint64_t get_length(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    if (max_length == 0)
      return 0;
    return (rand() % (max_length/2)) + ((max_length - 1)/2) + 1;
  }
};

class AttrGenerator : public RandGenerator {
  uint64_t max_len;
  uint64_t big_max_len;
public:
  AttrGenerator(uint64_t max_len, uint64_t big_max_len)
    : max_len(max_len), big_max_len(big_max_len) {}
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) {
    out.insert(std::pair<uint64_t, uint64_t>(0, get_length(cont)));
  }
  uint64_t get_length(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    // make some attrs big
    if (in.seqnum & 3)
      return (rand() % max_len);
    else
      return (rand() % big_max_len);
  }
  bufferlist gen_bl(const ContDesc &in) {
    bufferlist bl;
    for (iterator i = get_iterator(in); !i.end(); ++i) {
      bl.append(*i);
    }
    assert(bl.length() < big_max_len);
    return bl;
  }
};

class AppendGenerator : public RandGenerator {
  uint64_t off;
  uint64_t alignment;
  uint64_t min_append_size;
  uint64_t max_append_size;
  uint64_t max_append_total;

  uint64_t round_up(uint64_t in, uint64_t by) {
    if (by)
      in += (by - (in % by));
    return in;
  }

public:
  AppendGenerator(
    uint64_t off,
    uint64_t alignment,
    uint64_t min_append_size,
    uint64_t _max_append_size,
    uint64_t max_append_multiple) :
    off(off), alignment(alignment),
    min_append_size(round_up(min_append_size, alignment)),
    max_append_size(round_up(_max_append_size, alignment)) {
    if (_max_append_size == min_append_size)
      max_append_size += alignment;
    max_append_total = max_append_multiple * max_append_size;
  }
  uint64_t get_append_size(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    return round_up(rand() % max_append_total, alignment);
  }
  uint64_t get_length(const ContDesc &in) {
    return off + get_append_size(in);
  }
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out);
};

class ObjectDesc {
public:
  ObjectDesc()
    : exists(false), dirty(false),
      version(0) {}
  ObjectDesc(const ContDesc &init, ContentsGenerator *cont_gen)
    : exists(false), dirty(false),
      version(0) {
    layers.push_front(std::pair<ceph::shared_ptr<ContentsGenerator>, ContDesc>(ceph::shared_ptr<ContentsGenerator>(cont_gen), init));
  }

  class iterator {
  public:
    uint64_t pos;
    uint64_t size;
    uint64_t cur_valid_till;

    class ContState {
      interval_set<uint64_t> ranges;
      const uint64_t size;

    public:
      ContDesc cont;
      ceph::shared_ptr<ContentsGenerator> gen;
      ContentsGenerator::iterator iter;

      ContState(
	ContDesc _cont,
	ceph::shared_ptr<ContentsGenerator> _gen,
	ContentsGenerator::iterator _iter)
	: size(_gen->get_length(_cont)), cont(_cont), gen(_gen), iter(_iter) {
	gen->get_ranges(cont, ranges);
      }

      const interval_set<uint64_t> &get_ranges() {
	return ranges;
      }

      uint64_t get_size() {
	return gen->get_length(cont);
      }

      bool covers(uint64_t pos) {
	return ranges.contains(pos) || (!ranges.starts_after(pos) && pos >= size);
      }

      uint64_t next(uint64_t pos) {
	assert(!covers(pos));
	return ranges.starts_after(pos) ? ranges.start_after(pos) : size;
      }

      uint64_t valid_till(uint64_t pos) {
	assert(covers(pos));
	return ranges.contains(pos) ?
	  ranges.end_after(pos) :
	  std::numeric_limits<uint64_t>::max();
      }
    };
    std::list<ContState> layers;

    struct StackState {
      const uint64_t next;
      const uint64_t size;
    };
    std::list<std::pair<std::list<ContState>::iterator, StackState> > stack;
    std::list<ContState>::iterator current;

    explicit iterator(ObjectDesc &obj) :
      pos(0),
      size(obj.layers.begin()->first->get_length(obj.layers.begin()->second)),
      cur_valid_till(0) {
      for (auto &&i : obj.layers) {
	layers.push_back({i.second, i.first, i.first->get_iterator(i.second)});
      }
      current = layers.begin();

      adjust_stack();
    }

    void adjust_stack();
    iterator &operator++() {
      assert(cur_valid_till >= pos);
      ++pos;
      if (pos >= cur_valid_till) {
	adjust_stack();
      }
      return *this;
    }

    char operator*() {
      if (current == layers.end()) {
	return '\0';
      } else {
	return pos >= size ? '\0' : *(current->iter);
      }
    }

    bool end() {
      return pos >= size;
    }

    void seek(uint64_t _pos) {
      if (_pos < pos) {
	assert(0);
      }
      while (pos < _pos) {
	assert(cur_valid_till >= pos);
	uint64_t next = std::min(_pos - pos, cur_valid_till - pos);
	pos += next;

	if (pos >= cur_valid_till) {
	  assert(pos == cur_valid_till);
	  adjust_stack();
	}
      }
      assert(pos == _pos);
    }

    bufferlist gen_bl_advance(uint64_t s) {
      bufferlist ret;
      while (s > 0) {
	assert(cur_valid_till >= pos);
	uint64_t next = std::min(s, cur_valid_till - pos);
	if (current != layers.end() && pos < size) {
	  ret.append(current->iter.gen_bl_advance(next));
	} else {
	  ret.append_zero(next);
	}

	pos += next;
	assert(next <= s);
	s -= next;

	if (pos >= cur_valid_till) {
	  assert(cur_valid_till == pos);
	  adjust_stack();
	}
      }
      return ret;
    }

    bool check_bl_advance(bufferlist &bl, uint64_t *error_at = nullptr) {
      uint64_t off = 0;
      while (off < bl.length()) {
	assert(cur_valid_till >= pos);
	uint64_t next = std::min(bl.length() - off, cur_valid_till - pos);

	bufferlist to_check;
	to_check.substr_of(bl, off, next);
	if (current != layers.end() && pos < size) {
	  if (!current->iter.check_bl_advance(to_check, error_at)) {
	    if (error_at)
	      *error_at += off;
	    return false;
	  }
	} else {
	  uint64_t at = pos;
	  for (auto i = to_check.begin(); !i.end(); ++i, ++at) {
	    if (*i) {
	      if (error_at)
		*error_at = at;
	      return false;
	    }
	  }
	}

	pos += next;
	off += next;
	assert(off <= bl.length());

	if (pos >= cur_valid_till) {
	  assert(cur_valid_till == pos);
	  adjust_stack();
	}
      }
      assert(off == bl.length());
      return true;
    }
  };
    
  iterator begin() {
    return iterator(*this);
  }

  bool deleted() {
    return !exists;
  }

  bool has_contents() {
    return layers.size();
  }

  // takes ownership of gen
  void update(ContentsGenerator *gen, const ContDesc &next);
  bool check(bufferlist &to_check);
  bool check_sparse(const std::map<uint64_t, uint64_t>& extends,
		    bufferlist &to_check);
  const ContDesc &most_recent();
  ContentsGenerator *most_recent_gen() {
    return layers.begin()->first.get();
  }
  std::map<std::string, ContDesc> attrs; // Both omap and xattrs
  bufferlist header;
  bool exists;
  bool dirty;

  uint64_t version;
private:
  std::list<std::pair<ceph::shared_ptr<ContentsGenerator>, ContDesc> > layers;
};

#endif
