// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>

#ifndef OBJECT_H
#define OBJECT_H

class ContDesc {
public:
  int objnum;
  int cursnap;
  unsigned seqnum;
  string prefix;
  string oid;

  ContDesc() :
    objnum(0), cursnap(0),
    seqnum(0), prefix("") {}

  ContDesc(int objnum,
	   int cursnap,
	   unsigned seqnum,
	   const string &prefix) :
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
};

ostream &operator<<(ostream &out, const ContDesc &rhs);

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
    iterator(ContentsGenerator *parent, iterator_impl *impl) :
      parent(parent), impl(impl) {}
  };

  virtual bool read_header(bufferlist::iterator& input,
			   ContDesc &output) = 0;

  virtual uint64_t get_length(const ContDesc &in) = 0;

  virtual uint64_t get_attr_length(const ContDesc &in) = 0;

  virtual bufferlist gen_attribute(const ContDesc &in) = 0;

  virtual void get_ranges(const ContDesc &in, interval_set<uint64_t> &ranges) = 0;

  virtual iterator_impl *get_iterator_impl(const ContDesc &in) = 0;

  virtual iterator_impl *dup_iterator_impl(const iterator_impl *in) = 0;

  virtual void put_iterator_impl(iterator_impl *in) = 0;

  virtual ~ContentsGenerator() {};

  iterator get_iterator(const ContDesc &in) {
    return iterator(this, get_iterator_impl(in));
  }
};

class VarLenGenerator : public ContentsGenerator {
public:
  class RandWrap {
  public:
    unsigned int state;
    RandWrap(unsigned int seed)
    {
      state = seed;
    }

    int operator()()
    {
      return rand_r(&state);
    }
  };

  class iterator_impl : public ContentsGenerator::iterator_impl {
  public:
    uint64_t pos;
    ContDesc cont;
    RandWrap rand;
    bufferlist header;
    bufferlist::iterator header_pos;
    VarLenGenerator *cont_gen;
    char current;
    iterator_impl(const ContDesc &cont, VarLenGenerator *cont_gen) : 
      pos(0), cont(cont), rand(cont.seqnum), cont_gen(cont_gen) {
      cont_gen->write_header(cont, header);
      header_pos = header.begin();
      current = *header_pos;
      ++header_pos;
    }

    virtual ContDesc get_cont() const { return cont; }
    virtual uint64_t get_pos() const { return pos; }

    iterator_impl &operator++() {
      pos++;
      if (header_pos.end()) {
	current = rand();
      } else {
	current = *header_pos;
	++header_pos;
      }
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

  void get_ranges(const ContDesc &cont, interval_set<uint64_t> &out);

  ContentsGenerator::iterator_impl *get_iterator_impl(const ContDesc &in) {
    VarLenGenerator::iterator_impl *i = new iterator_impl(in, this);
    return i;
  }

  void put_iterator_impl(ContentsGenerator::iterator_impl *in) {
    delete in;
  }

  ContentsGenerator::iterator_impl *dup_iterator_impl(const ContentsGenerator::iterator_impl *in) {
    ContentsGenerator::iterator_impl *retval = get_iterator_impl(in->get_cont());
    retval->seek(in->get_pos());
    return retval;
  }

  int get_header_length(const ContDesc &in) {
    return 7*sizeof(int) + in.prefix.size();
  }

  uint64_t get_length(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    return (rand() % length) + get_header_length(in);
  }

  bufferlist gen_attribute(const ContDesc &in) {
    bufferlist header;
    write_header(in, header);
    ContentsGenerator::iterator iter = get_iterator(in);
    for (uint64_t to_write = get_attr_length(in); to_write > 0;
	 --to_write) {
      header.append(*iter);
      ++iter;
    }
    return header;
  }


  uint64_t get_attr_length(const ContDesc &in) {
    RandWrap rand(in.seqnum);
    return (rand() % attr_length) + get_header_length(in);
  }

  void write_header(const ContDesc &in, bufferlist &output);

  bool read_header(bufferlist::iterator &p, ContDesc &out);
  uint64_t length;
  uint64_t attr_length;
  uint64_t min_stride_size;
  uint64_t max_stride_size;
  VarLenGenerator(uint64_t length, uint64_t min_stride_size, uint64_t max_stride_size, uint64_t attr_length = 2000) :
    length(length), attr_length(attr_length),
    min_stride_size(min_stride_size), max_stride_size(max_stride_size) {}
};

class ObjectDesc {
public:
  ObjectDesc(ContentsGenerator *cont_gen)
    : exists(false), tmap(false), dirty(false),
      version(0), layers(), cont_gen(cont_gen) {}
  ObjectDesc(const ContDesc &init, ContentsGenerator *cont_gen)
    : exists(false), tmap(false), dirty(false),
      version(0), layers(), cont_gen(cont_gen) {
    layers.push_front(init);
  }

  class iterator {
  public:
    uint64_t pos;
    ObjectDesc &obj;
    ContentsGenerator *cont_gen;
    list<uint64_t> stack;
    map<ContDesc,ContentsGenerator::iterator> cont_iters;
    uint64_t limit;
    list<ContDesc>::iterator cur_cont;
    
    iterator(ObjectDesc &obj, ContentsGenerator *cont_gen) : 
      pos(0), obj(obj), cont_gen(cont_gen) {
      limit = cont_gen->get_length(*obj.layers.begin());
      cur_cont = obj.layers.begin();
      advance(true);
    }

    iterator &advance(bool init);
    iterator &operator++() {
      return advance(false);
    }

    char operator*() {
      if (cur_cont == obj.layers.end() && pos < obj.tmap_contents.length()) {
	return obj.tmap_contents[pos];
      } else if (cur_cont == obj.layers.end()) {
	return '\0';
      } else {
	map<ContDesc,ContentsGenerator::iterator>::iterator j = cont_iters.find(*cur_cont);
	assert(j != cont_iters.end());
	return *(j->second);
      }
    }

    bool end() {
      return pos >= cont_gen->get_length(*obj.layers.begin());
    }

    void seek(uint64_t _pos) {
      if (_pos < pos) {
	assert(0);
      }
      while (pos < _pos) {
	++(*this);
      }
    }
  };

  iterator begin() {
    return iterator(*this, this->cont_gen);
  }

  bool deleted() {
    return !exists;
  }

  bool has_contents() {
    return layers.size();
  }

  void update(const ContDesc &next);
  bool check(bufferlist &to_check);
  const ContDesc &most_recent();
  map<string, ContDesc> attrs; // Both omap and xattrs
  bufferlist header;
  bool exists;
  bool tmap;
  bool dirty;
  bufferlist tmap_contents;
  uint64_t version;
private:
  list<ContDesc> layers;
  ContentsGenerator *cont_gen;
  ObjectDesc();
};

#endif
