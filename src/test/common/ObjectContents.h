// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/interval_set.h"
#include "include/buffer_fwd.h"
#include <map>

#ifndef COMMON_OBJECT_H
#define COMMON_OBJECT_H

enum {
  RANDOMWRITEFULL,
  DELETED,
  CLONERANGE
};

bool test_object_contents();

class ObjectContents {
  uint64_t _size;
  std::map<uint64_t, unsigned int> seeds;
  interval_set<uint64_t> written;
  bool _exists;
public:
  class Iterator {
    ObjectContents *parent;
    std::map<uint64_t, unsigned int>::iterator iter;
    unsigned int current_state;
    int current_val;
    uint64_t pos;
  private:
    unsigned int get_state(uint64_t pos);
  public:
    explicit Iterator(ObjectContents *parent) :
      parent(parent), iter(parent->seeds.end()),
      current_state(0), current_val(0), pos(-1) {
      seek_to_first();
    }
    char operator*() {
      return parent->written.contains(pos) ?
	static_cast<char>(current_val % 256) : '\0';
    }
    uint64_t get_pos() {
      return pos;
    }
    void seek_to(uint64_t _pos) {
      if (pos > _pos ||
	  (iter != parent->seeds.end() && _pos >= iter->first)) {
	iter = parent->seeds.upper_bound(_pos);
	--iter;
	current_state = iter->second;
	current_val = rand_r(&current_state);
	pos = iter->first;
	++iter;
      }
      while (pos < _pos) ++(*this);
    }

    void seek_to_first() {
      seek_to(0);
    }
    Iterator &operator++() {
      ++pos;
      if (iter != parent->seeds.end() && pos >= iter->first) {
	assert(pos == iter->first);
	current_state = iter->second;
	++iter;
      }
      current_val = rand_r(&current_state);
      return *this;
    }
    bool valid() {
      return pos < parent->size();
    }
    friend class ObjectContents;
  };

  ObjectContents() : _size(0), _exists(false) {
    seeds[0] = 0;
  }

  explicit ObjectContents(bufferlist::iterator &bp) {
    ::decode(_size, bp);
    ::decode(seeds, bp);
    ::decode(written, bp);
    ::decode(_exists, bp);
  }

  void clone_range(ObjectContents &other,
		   interval_set<uint64_t> &intervals);
  void write(unsigned int seed,
	     uint64_t from,
	     uint64_t len);
  Iterator get_iterator() {
    return Iterator(this);
  }

  uint64_t size() const { return _size; }

  bool exists() { return _exists; }

  void debug(std::ostream &out) {
    out << "_size is " << _size << std::endl;
    out << "seeds is: (";
    for (std::map<uint64_t, unsigned int>::iterator i = seeds.begin();
	 i != seeds.end();
	 ++i) {
      out << "[" << i->first << "," << i->second << "], ";
    }
    out << ")" << std::endl;
    out << "written is " << written << std::endl;
    out << "_exists is " << _exists << std::endl;
  }

  void encode(bufferlist &bl) const {
    ::encode(_size, bl);
    ::encode(seeds, bl);
    ::encode(written, bl);
    ::encode(_exists, bl);
  }
};

#endif
