// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
#include "include/interval_set.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <list>
#include <map>
#include <set>
#include <stack>
#include <random>

#ifndef OBJECT_H
#define OBJECT_H

/// describes an object
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
  void decode(bufferlist::const_iterator &bp);
};
WRITE_CLASS_ENCODER(ContDesc)

std::ostream &operator<<(std::ostream &out, const ContDesc &rhs);

class ChunkDesc {
public:
  uint32_t offset;
  uint32_t length;
  std::string oid;
};

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
    /// walk through given @c bl
    ///
    /// @param[out] off the offset of the first byte which does not match
    /// @returns true if @c bl matches with the content, false otherwise
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

    ContDesc get_cont() const override { return cont; }
    uint64_t get_pos() const override { return pos; }

    iterator_impl &operator++() override {
      pos++;
      current = rand();
      return *this;
    }

    char operator*() override {
      return current;
    }

    void seek(uint64_t _pos) override {
      if (_pos < pos) {
	iterator_impl begin = iterator_impl(cont, cont_gen);
	begin.seek(_pos);
	*this = begin;
      }
      while (pos < _pos) {
	++(*this);
      }
    }

    bool end() override {
      return pos >= cont_gen->get_length(cont);
    }
  };

  ContentsGenerator::iterator_impl *get_iterator_impl(const ContDesc &in) override {
    RandGenerator::iterator_impl *i = new iterator_impl(in, this);
    return i;
  }

  void put_iterator_impl(ContentsGenerator::iterator_impl *in) override {
    delete in;
  }

  ContentsGenerator::iterator_impl *dup_iterator_impl(
    const ContentsGenerator::iterator_impl *in) override {
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
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) override;
  uint64_t get_length(const ContDesc &in) override {
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
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) override {
    out.insert(std::pair<uint64_t, uint64_t>(0, get_length(cont)));
  }
  uint64_t get_length(const ContDesc &in) override {
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
    ceph_assert(bl.length() < big_max_len);
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
  uint64_t get_length(const ContDesc &in) override {
    return off + get_append_size(in);
  }
  void get_ranges_map(
    const ContDesc &cont, std::map<uint64_t, uint64_t> &out) override;
};

class ObjectDesc {
public:
  ObjectDesc()
    : exists(false), dirty(false),
      version(0), flushed(false) {}
  ObjectDesc(const ContDesc &init, ContentsGenerator *cont_gen)
    : exists(false), dirty(false),
      version(0), flushed(false) {
    layers.push_front(std::pair<std::shared_ptr<ContentsGenerator>, ContDesc>(std::shared_ptr<ContentsGenerator>(cont_gen), init));
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
      std::shared_ptr<ContentsGenerator> gen;
      ContentsGenerator::iterator iter;

      ContState(
	const ContDesc &_cont,
	std::shared_ptr<ContentsGenerator> _gen,
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
	ceph_assert(!covers(pos));
	return ranges.starts_after(pos) ? ranges.start_after(pos) : size;
      }

      uint64_t valid_till(uint64_t pos) {
	ceph_assert(covers(pos));
	return ranges.contains(pos) ?
	  ranges.end_after(pos) :
	  std::numeric_limits<uint64_t>::max();
      }
    };
    // from latest to earliest
    using layers_t = std::vector<ContState>;
    layers_t layers;

    struct StackState {
      const uint64_t next;
      const uint64_t size;
    };
    std::stack<std::pair<layers_t::iterator, StackState> > stack;
    layers_t::iterator current;

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
      ceph_assert(cur_valid_till >= pos);
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

    // advance @c pos to given position
    void seek(uint64_t _pos) {
      if (_pos < pos) {
	ceph_abort();
      }
      while (pos < _pos) {
	ceph_assert(cur_valid_till >= pos);
	uint64_t next = std::min(_pos - pos, cur_valid_till - pos);
	pos += next;

	if (pos >= cur_valid_till) {
	  ceph_assert(pos == cur_valid_till);
	  adjust_stack();
	}
      }
      ceph_assert(pos == _pos);
      if (current != layers.end()) {
        current->iter.seek(pos);
      }
    }

    // grab the bytes in the range of [pos, pos+s), and advance @c pos
    //
    // @returns the bytes in the specified range
    bufferlist gen_bl_advance(uint64_t s) {
      bufferlist ret;
      while (s > 0) {
	ceph_assert(cur_valid_till >= pos);
	uint64_t next = std::min(s, cur_valid_till - pos);
	if (current != layers.end() && pos < size) {
	  ret.append(current->iter.gen_bl_advance(next));
	} else {
	  ret.append_zero(next);
	}

	pos += next;
	ceph_assert(next <= s);
	s -= next;

	if (pos >= cur_valid_till) {
	  ceph_assert(cur_valid_till == pos);
	  adjust_stack();
	}
      }
      return ret;
    }

    // compare the range of [pos, pos+bl.length()) with given @c bl, and
    // advance @pos if all bytes in the range match
    //
    // @param error_at the offset of the first byte which does not match
    // @returns true if all bytes match, false otherwise
    bool check_bl_advance(bufferlist &bl, uint64_t *error_at = nullptr) {
      uint64_t off = 0;
      while (off < bl.length()) {
	ceph_assert(cur_valid_till >= pos);
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
	ceph_assert(off <= bl.length());

	if (pos >= cur_valid_till) {
	  ceph_assert(cur_valid_till == pos);
	  adjust_stack();
	}
      }
      ceph_assert(off == bl.length());
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
  bool check(bufferlist &to_check,
	     const std::pair<uint64_t, uint64_t>& offlen);
  bool check_sparse(const std::map<uint64_t, uint64_t>& extends,
		    bufferlist &to_check,
		    const std::pair<uint64_t, uint64_t>& offlen);
  const ContDesc &most_recent();

  // Returns the minimum set of byte ranges that MUST appear in the OSD's
  // mapext result.  This is get_written_extents(alignment) minus any
  // 4k-aligned, 4k-length blocks whose content is entirely zeros.
  //
  // Because writes and appends never produce zero bytes, a block is all-zeros
  // if and only if no write/append layer covers it (i.e. it was zeroed by a
  // ZeroGenerator or was never written).  After shard recovery/reconstruction
  // the OSD is free to drop such blocks from its allocation map, so the model
  // must not require them in the actual mapext result.
  //
  // Partial blocks at the edges of extents (smaller than a full alignment
  // unit) are included in the returned set — they will be narrowed further
  // by inward rounding in the caller (MapextOp::_finish) so that sub-4k
  // edge fragments are not required in the actual result either.
  interval_set<uint64_t> get_min_written_extents(uint64_t alignment) const;

  // Returns the set of byte ranges that are allocated (have data) on the
  // OSD, accounting for write-layer truncation and zero hole-punching.
  //
  // Layers are stored newest-first.  We scan newest→oldest maintaining a
  // 'masked' set: the union of all byte ranges whose fate has already been
  // decided by a newer layer (written as data OR punched as a hole).  An
  // older layer may only contribute bytes that are not yet masked.
  //
  // This correctly handles all orderings:
  //  - Zero newer than write:  hole is applied immediately; older write
  //    bytes under the hole are masked out and never added to written.
  //  - Write newer than zero:  write claims its bytes first (marks them
  //    masked as data); the older zero's hole is clipped to the unmasked
  //    remainder, so it cannot remove bytes the newer write restored.
  //
  // ZeroGenerator layers do not truncate.  Only write layers (VarLenGenerator,
  // AppendGenerator, …) impose a truncation via get_length(); older data
  // beyond any newer write-layer truncation is clamped away.
  //
  // EC pools (alignment > 1) have an additional consideration: the zero
  // operation in PrimaryLogPG emits a literal-zero write for any partial
  // alignment block at either edge of the zero range (head and tail).
  // This write allocates the whole alignment-sized block on the OSD, even
  // if no prior write ever touched it.  The tail partial block specifically
  // — [hole_end, round_up(z_end, alignment)) — may not be covered by any
  // older write layer, so it must be explicitly added to written here.
  // (The head partial block [z_off, hole_start) is always within the
  // previously-written object region, so older layers cover it naturally,
  // but we add both for symmetry and correctness.)
  interval_set<uint64_t> get_written_extents(uint64_t alignment = 1) const {
    interval_set<uint64_t> written;
    interval_set<uint64_t> masked;

    // UINT64_MAX means "no write-layer truncation seen yet"
    uint64_t min_write_trunc = UINT64_MAX;
    for (auto &layer : layers) {
      ContentsGenerator *gen = layer.first.get();
      auto *zero_gen = dynamic_cast<ZeroGenerator *>(gen);
      if (zero_gen != nullptr) {
        // Compute the aligned hole range:
        //   alignment == 1 (replicated): full zeroed range is deallocated.
        //   alignment >  1 (EC):         only the aligned interior is
        //                                deallocated; the partial head and
        //                                tail blocks are zeroed in-place.
        const uint64_t z_off = zero_gen->get_zero_offset();
        const uint64_t z_end = z_off + zero_gen->get_zero_length();
        uint64_t hole_start, hole_end;
        if (alignment > 1) {
          hole_start = (z_off + alignment - 1) / alignment * alignment; // round up
          hole_end   = z_end / alignment * alignment;                   // round down
        } else {
          hole_start = z_off;
          hole_end   = z_end;
        }
        // Clamp hole to min_write_trunc: bytes beyond the newest write-layer
        // truncation point don't exist, so there is nothing to punch there.
        if (min_write_trunc != UINT64_MAX && hole_end > min_write_trunc)
          hole_end = min_write_trunc;
        if (hole_start < hole_end) {
          // Only act on the portion of the hole not already decided by a
          // newer layer (a newer write may have re-allocated part of it).
          interval_set<uint64_t> hole;
          hole.insert(hole_start, hole_end - hole_start);
          interval_set<uint64_t> unmasked;
          unmasked.intersection_of(hole, masked);   // part already decided
          hole.subtract(unmasked);                  // keep only undecided part
          // hole now contains only bytes not yet masked: remove from written
          // and mark as masked (decided as a hole by this layer).
          interval_set<uint64_t> to_remove;
          to_remove.intersection_of(written, hole);
          written.subtract(to_remove);
          masked.union_of(hole);
        }
        // For EC pools: the zero op emits a literal-zero write for the
        // partial alignment block at each edge.  These writes allocate the
        // full alignment-sized block on the OSD regardless of prior history.
        // Add both edge blocks to written (unconditionally, bypassing the
        // mask) so the model reflects those allocations.
        if (alignment > 1) {
          // Head partial block: [z_off, hole_start).
          // Only present if the zero range starts mid-block.
          if (z_off < hole_start) {
            uint64_t head_block_end = hole_start;
            if (min_write_trunc == UINT64_MAX || head_block_end <= min_write_trunc) {
              interval_set<uint64_t> head_edge;
              head_edge.insert(z_off, head_block_end - z_off);
              interval_set<uint64_t> head_unmasked;
              head_unmasked.intersection_of(head_edge, masked);
              head_edge.subtract(head_unmasked);
              written.union_of(head_edge);
              masked.union_of(head_edge);
            }
          }
          // Tail partial block: [hole_end, round_up(z_end, alignment)).
          // Only present if the zero range ends mid-block.
          // Note: hole_end may have been clamped by min_write_trunc above,
          // but the unclamped z_end determines whether there is a real tail.
          const uint64_t z_end_raw = z_off + zero_gen->get_zero_length();
          const uint64_t tail_block_start = z_end_raw / alignment * alignment; // round down
          const uint64_t tail_block_end   = (z_end_raw + alignment - 1) / alignment * alignment; // round up
          if (tail_block_start < z_end_raw &&  // z_end is not already aligned
              tail_block_start < tail_block_end) {
            // The tail partial block [tail_block_start, tail_block_end) is
            // allocated by the tail literal-zero write.  Clamp to object size.
            uint64_t tbe = tail_block_end;
            if (min_write_trunc != UINT64_MAX && tbe > min_write_trunc)
              tbe = min_write_trunc;
            if (tail_block_start < tbe) {
              interval_set<uint64_t> tail_edge;
              tail_edge.insert(tail_block_start, tbe - tail_block_start);
              interval_set<uint64_t> tail_unmasked;
              tail_unmasked.intersection_of(tail_edge, masked);
              tail_edge.subtract(tail_unmasked);
              written.union_of(tail_edge);
              masked.union_of(tail_edge);
            }
          }
        }
        // ZeroGenerator does not truncate; no min_write_trunc update.
      } else {
        interval_set<uint64_t> layer_ranges;
        gen->get_ranges(layer.second, layer_ranges);
        // Clamp this layer's ranges to the truncation imposed by all
        // newer write layers.
        if (min_write_trunc != UINT64_MAX) {
          interval_set<uint64_t> valid;
          valid.insert(0, min_write_trunc);
          layer_ranges.intersection_of(valid);
        }
        // Only add ranges not already decided by a newer layer.
        interval_set<uint64_t> unmasked;
        unmasked.intersection_of(layer_ranges, masked); // already decided
        layer_ranges.subtract(unmasked);                // keep undecided only
        written.union_of(layer_ranges);
        masked.union_of(layer_ranges);
        uint64_t trunc = gen->get_length(layer.second);
        if (trunc < min_write_trunc)
          min_write_trunc = trunc;
      }
    }

    return written;
  }
  ContentsGenerator *most_recent_gen() {
    return layers.begin()->first.get();
  }
  std::map<std::string, ContDesc> attrs; // Both omap and xattrs
  bufferlist header;
  bool exists;
  bool dirty;

  uint64_t version;
  std::string redirect_target;
  std::map<uint64_t, ChunkDesc> chunk_info;
  bool flushed;
private:
  std::list<std::pair<std::shared_ptr<ContentsGenerator>, ContDesc> > layers;
};

#endif
