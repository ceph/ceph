// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_BLINKER_H
#define CEPH_BLINKER_H

class Blinker {

 public:

  class Op {
    int op;
    static const int LOOKUP = 1;
    static const int INSERT = 2;
    static const int REMOVE = 3;
    static const int CLEAR = 4;
    Op(int o) : op(o) {}
  };
  
  class OpLookup : public Op {
  public:
    bufferptr key;
    OpLookup(bufferptr& k) : Op(Op::LOOKUP), key(k) {}
  };

  class OpInsert : public Op {
    bufferptr key;
    bufferlist val;
    OpInsert(bufferptr& k, bufferlist& v) : Op(Op::INSERT), key(k), val(v) {}
  };

  class OpRemove : public Op {
  public:
    bufferptr key;
    OpRemove(bufferptr& k) : Op(Op::REMOVE), key(k) {}
  };

  class OpClear : public Op {
  public:
    OpClear() : Op(Op::CLEAR) {}
  };



private:
  Objecter *objecter;

  // in-flight operations.


  // cache information about tree structure.
  


public:
  // public interface

  // simple accessors
  void lookup(inode_t& inode, bufferptr& key, bufferlist *pval, Context *onfinish);

  // simple modifiers
  void insert(inode_t& inode, bufferptr& key, bufferlist& val, Context *onack, Context *onsafe);
  void remove(inode_t& inode, bufferptr& key, Context *onack, Context *onsafe);
  void clear(inode_t& inode, Context *onack, Context *onsafe);

  // these are dangerous: the table may be large.
  void listkeys(inode_t& inode, list<bufferptr>* pkeys, Context *onfinish);
  void listvals(inode_t& inode, list<bufferptr>* pkeys, list<bufferlist>* pvals, Context *onfinish);

  // fetch *at least* key, but also anything else that is convenient. 
  // include lexical bounds for which this is a complete result.
  //  (if *start and *end are empty, it's the entire table)
  void prefetch(inode_t& inode, bufferptr& key, 
		list<bufferptr>* pkeys, list<bufferlist>* pvals, 
		bufferptr *start, bufferptr *end,
		Context *onfinish);

  
};

#endif
