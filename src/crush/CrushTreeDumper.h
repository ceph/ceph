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
    int depth;
    float weight;
    list<int> children;

    Item() : id(0), depth(0), weight(0) {}
    Item(int i, int d, float w) : id(i), depth(d), weight(w) {}

    bool is_bucket() const { return id < 0; }
  };

  template <typename F>
  class Dumper : public list<Item> {
  public:
    explicit Dumper(const CrushWrapper *crush_) : crush(crush_) {
      crush->find_roots(roots);
      root = roots.begin();
    }

    virtual ~Dumper() {}

    virtual void reset() {
      root = roots.begin();
      touched.clear();
      clear();
    }

    bool next(Item &qi) {
      if (empty()) {
	if (root == roots.end())
	  return false;
	push_back(Item(*root, 0, crush->get_bucket_weightf(*root)));
	++root;
      }

      qi = front();
      pop_front();
      touched.insert(qi.id);

      if (qi.is_bucket()) {
	// queue bucket contents...
	int s = crush->get_bucket_size(qi.id);
	for (int k = s - 1; k >= 0; k--) {
	  int id = crush->get_bucket_item(qi.id, k);
	  qi.children.push_back(id);
	  push_front(Item(id, qi.depth + 1,
			  crush->get_bucket_item_weightf(qi.id, k)));
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

  protected:
    virtual void dump_item(const Item &qi, F *f) = 0;

  protected:
    const CrushWrapper *crush;

  private:
    set<int> touched;
    set<int> roots;
    set<int>::iterator root;
  };

  inline void dump_item_fields(const CrushWrapper *crush,
			       const Item &qi, Formatter *f) {
    f->dump_int("id", qi.id);
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
  }

  inline void dump_bucket_children(const CrushWrapper *crush,
				   const Item &qi, Formatter *f) {
    if (!qi.is_bucket())
      return;

    f->open_array_section("children");
    for (list<int>::const_iterator i = qi.children.begin();
	 i != qi.children.end();
	 ++i) {
      f->dump_int("child", *i);
    }
    f->close_section();
  }

  class FormattingDumper : public Dumper<Formatter> {
  public:
    explicit FormattingDumper(const CrushWrapper *crush) : Dumper<Formatter>(crush) {}

  protected:
    virtual void dump_item(const Item &qi, Formatter *f) {
      f->open_object_section("item");
      dump_item_fields(qi, f);
      dump_bucket_children(qi, f);
      f->close_section();
    }

    virtual void dump_item_fields(const Item &qi, Formatter *f) {
      CrushTreeDumper::dump_item_fields(crush, qi, f);
    }

    virtual void dump_bucket_children(const Item &qi, Formatter *f) {
      CrushTreeDumper::dump_bucket_children(crush, qi, f);
    }
  };

}

#endif
