#include <iostream>
#include <vector>
#include <map>
#include <utility>
#include <functional>

#include "include/ceph_assert.h"

#ifndef __INDEXEDPRIORITYQUEUE_H__
#define __INDEXEDPRIORITYQUEUE_H__ 1


template <class T, class Compare>
class IndexedPriorityQueue
{
  public:
    IndexedPriorityQueue();
    ~IndexedPriorityQueue();

    void set_name(const char *name);

    const T top() const;
    void push(const T t);
    const T pop();
    unsigned size() const;
    bool empty() const;
    void erase(const T t);
    bool find(const T t) const;
    void dump() const;

  private:
    Compare comp;
    std::vector<T> m_pq;
    // mapping of object to index in priority queue
    std::map<T, unsigned> m_index; 
    std::string m_name;


    void exch(unsigned i, unsigned j);
    void swim(unsigned k);
    void sink(unsigned k);
};


template <class T, class Compare>
IndexedPriorityQueue<T,Compare>::IndexedPriorityQueue()
{
}

template <class T, class Compare>
IndexedPriorityQueue<T,Compare>::~IndexedPriorityQueue()
{
  /* this class does not own the objects added to the priority queue
   * so there's nothing to do here
   */
}

template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::set_name(const char *name)
{
  m_name = name;
}

template <class T, class Compare>
const T IndexedPriorityQueue<T,Compare>::top() const
{
  return m_pq[0];
}

template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::push(const T t)
{
  // std::cerr << std::hex << "0x" << pthread_self() << ":" << __FUNCTION__ << ":: " << m_name << ": t:" << t << std::dec << std::endl;
  ceph_assert(m_index.find(t) == m_index.end()); // t should not be found in the index

  m_index[t] = m_pq.size();
  m_pq.push_back(t); // NOTE: size of vector changes after assginment

  swim(m_pq.size() - 1);
  ceph_assert(m_index.size() == m_pq.size());
}

template <class T, class Compare>
const T IndexedPriorityQueue<T,Compare>::pop()
{
  // std::cerr << __PRETTY_FUNCTION__ << std::endl;
  T t = m_pq[0];
  m_index.erase(m_pq[0]);
  if (m_pq.size() > 1) {
    m_pq[0] = m_pq[m_pq.size() - 1];
    m_index[m_pq[0]] = 0;
  }
  m_pq.erase(--m_pq.cend());

  if (m_pq.size() > 0) {
    sink(0);
  }

  ceph_assert(m_index.size() == m_pq.size());
  return t;
}

template <class T, class Compare>
unsigned IndexedPriorityQueue<T,Compare>::size() const
{
  // std::cerr << __PRETTY_FUNCTION__ << std::endl;
  return m_pq.size();
}

template <class T, class Compare>
bool IndexedPriorityQueue<T,Compare>::empty() const
{
  // std::cerr << __PRETTY_FUNCTION__ << std::endl;
  ceph_assert(m_index.size() == m_pq.size());
  return m_pq.empty();
}

template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::erase(const T t)
{
  // std::cerr << std::hex << "0x" << pthread_self() << ":" << __FUNCTION__ << ":: " << m_name << " t:" << t << std::endl;
  auto idx_it = m_index.find(t);

  ceph_assert(idx_it != m_index.end());

  unsigned idx = idx_it->second;

  ceph_assert(idx < m_pq.size());

  // std::cerr << std::hex << "0x" << pthread_self() << ":" << __FUNCTION__ << ":: " << m_name << " t:" << t << ", at:" << std::dec << idx << std::endl;
  m_index.erase(t);
  if ((m_pq.size() > 1) && (idx != (m_pq.size() -1))) {
    m_pq[idx] = m_pq[m_pq.size() - 1];
    m_index[m_pq[idx]] = idx;
  }
  m_pq.erase(--m_pq.cend());

  if ((m_pq.size() > 0) && (idx < (m_pq.size() - 1)))
    sink(idx);
  ceph_assert(m_index.size() == m_pq.size());
}

template <class T, class Compare>
bool IndexedPriorityQueue<T,Compare>::find(const T t) const
{
  if (m_index.find(t) != m_index.end())
    return true;
  return false;
}


/* P R I V A T E   M E T H O D S */
template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::exch(unsigned i, unsigned j)
{
  // std::cerr << __PRETTY_FUNCTION__ << std::endl;
  std::swap(m_pq[i], m_pq[j]);
  m_index[m_pq[i]] = i;
  m_index[m_pq[j]] = j;
}

template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::swim(unsigned k)
{
  // std::cerr << __PRETTY_FUNCTION__ << std::endl;
  if (m_pq.size() <= 1)
    return;

  while (k > 0 && comp(m_pq[k/2], m_pq[k])) {
    exch(k, k/2);
    k = k/2;
  }
}

template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::sink(unsigned k)
{
  // std::cerr << __PRETTY_FUNCTION__ << std::endl;
  unsigned N = m_pq.size();

  if (N <= 1)
    return;

  // priority queue is zero index based
  while ((2*k) <= (N-1)) {
    unsigned j = (2*k);
    if (j < (N-1) && comp(m_pq[j], m_pq[j+1])) j++;
    if (!comp(m_pq[k], m_pq[j])) break;
    exch(k, j);
    k = j;
  }
}

template <class T, class Compare>
void IndexedPriorityQueue<T,Compare>::dump() const
{
  std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "Priority Queue:" << std::endl;
  int i = 0;
  for (auto it = m_pq.begin(); it != m_pq.end(); ++it) {
    std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "m_pq[" << i << "]:" << (*it) << std::endl;
    i++;
  }
  std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "Priority Queue Index:" << std::endl;
  i = 0;
  for (auto it = m_index.begin(); it != m_index.end(); ++it) {
    std::cout << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << i << "  m_index[" << it->first << "]:" << it->second << std::endl;
    i++;
  }
}
#endif
