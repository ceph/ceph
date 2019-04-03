// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis Inc
 *
 * Author: Mykola Golub <mgolub@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CRUSH_TREE_DUMPER_H
#define CRUSH_TREE_DUMPER_H

#include "CrushWrapper.h"
#include "include/stringify.h"

/**
 * CrushTreeDumper:
 * A helper class and functions to dump a crush tree.
 *
 * Example:
 *
 *  class SimpleDumper : public CrushTreeDumper::Dumper<ostream> {
 *  public:
 *    SimpleDumper(const CrushWrapper *crush) :
 *      CrushTreeDumper::Dumper<ostream>(crush) {}
 *  protected:
 *    virtual void dump_item(const CrushTreeDumper::Item &qi, ostream *out) {
 *      *out << qi.id;
 *      for (int k = 0; k < qi.depth; k++)
 *        *out << "-";
 *      if (qi.is_bucket())
 *        *out << crush->get_item_name(qi.id)
 *      else
 *        *out << "osd." << qi.id;
 *      *out << "\n";
 *    }
 *  };
 *
 *  SimpleDumper(crush).dump(out);
 *
 */

namespace CrushTreeDumper {

  struct Item {
    int id;
    int parent;
    int depth;
    float weight;
    std::list<int> children;

    Item() : id(0), parent(0), depth(0), weight(0) {}
    Item(int i, int p, int d, float w) : id(i), parent(p), depth(d), weight(w) {}

    bool is_bucket() const { return id < 0; }
  };

  template <typename F>
  class Dumper : public std::list<Item> {
  public:
    explicit Dumper(const CrushWrapper *crush_,
		    const name_map_t& weight_set_names_)
      : crush(crush_), weight_set_names(weight_set_names_) {
      crush->find_nonshadow_roots(&roots);
      root = roots.begin();
    }
    explicit Dumper(const CrushWrapper *crush_,
                    const name_map_t& weight_set_names_,
                    bool show_shadow)
      : crush(crush_), weight_set_names(weight_set_names_) {
      if (show_shadow) {
        crush->find_roots(&roots);
      } else {
        crush->find_nonshadow_roots(&roots);
      }
      root = roots.begin();
    }

    virtual ~Dumper() {}

    virtual void reset() {
      root = roots.begin();
      touched.clear();
      clear();
    }

    virtual bool should_dump_leaf(int i) const {
      return true;
    }
    virtual bool should_dump_empty_bucket() const {
      return true;
    }

    bool should_dump(int id) {
      if (id >= 0)
	return should_dump_leaf(id);
      if (should_dump_empty_bucket())
	return true;
      int s = crush->get_bucket_size(id);
      for (int k = s - 1; k >= 0; k--) {
	int c = crush->get_bucket_item(id, k);
	if (should_dump(c))
	  return true;
      }
      return false;
    }

    bool next(Item &qi) {
      if (empty()) {
	while (root != roots.end() && !should_dump(*root))
	  ++root;
	if (root == roots.end())
	  return false;
	push_back(Item(*root, 0, 0, crush->get_bucket_weightf(*root)));
	++root;
      }

      qi = front();
      pop_front();
      touched.insert(qi.id);

      if (qi.is_bucket()) {
	// queue bucket contents, sorted by (class, name)
	int s = crush->get_bucket_size(qi.id);
	std::map<std::string, std::pair<int,float>> sorted;
	for (int k = s - 1; k >= 0; k--) {
	  int id = crush->get_bucket_item(qi.id, k);
	  if (should_dump(id)) {
	    std::string sort_by;
	    if (id >= 0) {
	      const char *c = crush->get_item_class(id);
	      sort_by = c ? c : "";
	      sort_by += "_";
	      char nn[80];
	      snprintf(nn, sizeof(nn), "osd.%08d", id);
	      sort_by += nn;
	    } else {
	      sort_by = "_";
	      sort_by += crush->get_item_name(id);
	    }
	    sorted[sort_by] = std::make_pair(
	      id, crush->get_bucket_item_weightf(qi.id, k));
	  }
	}
	for (auto p = sorted.rbegin(); p != sorted.rend(); ++p) {
	  qi.children.push_back(p->second.first);
	  push_front(Item(p->second.first, qi.id, qi.depth + 1,
			  p->second.second));
	}
      }
      return true;
    }

    void dump(F *f) {
      reset();
      Item qi;
      while (next(qi))
	dump_item(qi, f);
    }

    bool is_touched(int id) const { return touched.count(id) > 0; }

    void set_root(const std::string& bucket) {
      roots.clear();
      if (crush->name_exists(bucket)) {
	int i = crush->get_item_id(bucket);
	roots.insert(i);
      }
    }

  protected:
    virtual void dump_item(const Item &qi, F *f) = 0;

  protected:
    const CrushWrapper *crush;
    const name_map_t &weight_set_names;

  private:
    std::set<int> touched;
    std::set<int> roots;
    std::set<int>::iterator root;
  };

  inline void dump_item_fields(const CrushWrapper *crush,
			       const name_map_t& weight_set_names,
			       const Item &qi, ceph::Formatter *f) {
    f->dump_int("id", qi.id);
    const char *c = crush->get_item_class(qi.id);
    if (c)
      f->dump_string("device_class", c);
    if (qi.is_bucket()) {
      int type = crush->get_bucket_type(qi.id);
      f->dump_string("name", crush->get_item_name(qi.id));
      f->dump_string("type", crush->get_type_name(type));
      f->dump_int("type_id", type);
    } else {
      f->dump_stream("name") << "osd." << qi.id;
      f->dump_string("type", crush->get_type_name(0));
      f->dump_int("type_id", 0);
      f->dump_float("crush_weight", qi.weight);
      f->dump_unsigned("depth", qi.depth);
    }
    if (qi.parent < 0) {
      f->open_object_section("pool_weights");
      for (auto& p : crush->choose_args) {
	const crush_choose_arg_map& cmap = p.second;
	int bidx = -1 - qi.parent;
	const crush_bucket *b = crush->get_bucket(qi.parent);
	if (b &&
	    bidx < (int)cmap.size &&
	    cmap.args[bidx].weight_set &&
	    cmap.args[bidx].weight_set_positions >= 1) {
	  int bpos;
	  for (bpos = 0;
	       bpos < (int)cmap.args[bidx].weight_set[0].size &&
		 b->items[bpos] != qi.id;
	       ++bpos) ;
	  std::string name;
	  if (p.first == CrushWrapper::DEFAULT_CHOOSE_ARGS) {
	    name = "(compat)";
	  } else {
	    auto q = weight_set_names.find(p.first);
	    name = q != weight_set_names.end() ? q->second :
	      stringify(p.first);
	  }
	  f->open_array_section(name.c_str());
	  for (unsigned opos = 0;
	       opos < cmap.args[bidx].weight_set_positions;
	       ++opos) {
	    float w = (float)cmap.args[bidx].weight_set[opos].weights[bpos] /
	      (float)0x10000;
	    f->dump_float("weight", w);
	  }
	  f->close_section();
	}
      }
      f->close_section();
    }
  }

  inline void dump_bucket_children(const CrushWrapper *crush,
				   const Item &qi, ceph::Formatter *f) {
    if (!qi.is_bucket())
      return;

    f->open_array_section("children");
    for (std::list<int>::const_iterator i = qi.children.begin();
	 i != qi.children.end();
	 ++i) {
      f->dump_int("child", *i);
    }
    f->close_section();
  }

  class FormattingDumper : public Dumper<ceph::Formatter> {
  public:
    explicit FormattingDumper(const CrushWrapper *crush,
			      const name_map_t& weight_set_names)
      : Dumper<ceph::Formatter>(crush, weight_set_names) {}
    explicit FormattingDumper(const CrushWrapper *crush,
                              const name_map_t& weight_set_names,
                              bool show_shadow)
      : Dumper<ceph::Formatter>(crush, weight_set_names, show_shadow) {}

  protected:
    void dump_item(const Item &qi, ceph::Formatter *f) override {
      f->open_object_section("item");
      dump_item_fields(qi, f);
      dump_bucket_children(qi, f);
      f->close_section();
    }

    virtual void dump_item_fields(const Item &qi, ceph::Formatter *f) {
      CrushTreeDumper::dump_item_fields(crush, weight_set_names, qi, f);
    }

    virtual void dump_bucket_children(const Item &qi, ceph::Formatter *f) {
      CrushTreeDumper::dump_bucket_children(crush, qi, f);
    }
  };

}

#endif
