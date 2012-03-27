// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_LOG_ENTRYQUEUE_H
#define __CEPH_LOG_ENTRYQUEUE_H

#include "Entry.h"

namespace ceph {
namespace log {

struct EntryQueue {
  int m_len;
  struct Entry *m_head, *m_tail;

  bool empty() const {
    return m_len == 0;
  }

  void swap(EntryQueue& other) {
    int len = m_len;
    struct Entry *h = m_head, *t = m_tail;
    m_len = other.m_len;
    m_head = other.m_head;
    m_tail = other.m_tail;
    other.m_len = len;
    other.m_head = h;
    other.m_tail = t;
  }

  void enqueue(Entry *e) {
    if (m_tail) {
      m_tail->m_next = e;
      m_tail = e;
    } else {
      m_head = m_tail = e;
    }
    m_len++;
  }

  Entry *dequeue() {
    if (!m_head)
      return NULL;
    Entry *e = m_head;
    m_head = m_head->m_next;
    if (!m_head)
      m_tail = NULL;
    m_len--;
    e->m_next = NULL;
    return e;
  }

  EntryQueue()
    : m_len(0),
      m_head(NULL),
      m_tail(NULL)
  {}
  ~EntryQueue() {
    Entry *t;
    while (m_head) {
      t = m_head->m_next;
      delete m_head;
      m_head = t;
    }      
  }
};

}
}

#endif
