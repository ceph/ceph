// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef DISTIRITBIONHPP
#define DISTIRITBIONHPP

#include <map>
#include <set>
#include <utility>
#include <vector>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/tuple/tuple.hpp>

typedef boost::mt11213b rngen_t;

template <typename T>
class Distribution {
public:
  virtual T operator()() = 0;
  virtual ~Distribution() {}
};

template <typename T, typename U, typename V, typename W>
class FourTupleDist : public Distribution<boost::tuple<T, U, V, W> > {
  boost::scoped_ptr<Distribution<T> > t;
  boost::scoped_ptr<Distribution<U> > u;
  boost::scoped_ptr<Distribution<V> > v;
  boost::scoped_ptr<Distribution<W> > w;
public:
  FourTupleDist(
    Distribution<T> *t,
    Distribution<U> *u,
    Distribution<V> *v,
    Distribution<W> *w)
    : t(t), u(u), v(v), w(w) {}
  boost::tuple<T, U, V, W> operator()() {
    return boost::make_tuple((*t)(), (*u)(), (*v)(), (*w)());
  }
};

template <typename T>
class RandomDist : public Distribution<T> {
  rngen_t rng;
  std::map<uint64_t, T> contents;
public:
  RandomDist(const rngen_t &rng, std::set<T> &initial) : rng(rng) {
    uint64_t count = 0;
    for (typename std::set<T>::iterator i = initial.begin();
	 i != initial.end();
	 ++i, ++count) {
      contents.insert(std::make_pair(count, *i));
    }
  }
  virtual T operator()() {
    assert(contents.size());
    boost::uniform_int<> value(0, contents.size() - 1);
    return contents.find(value(rng))->second;
  }
};

template <typename T>
class WeightedDist : public Distribution<T> {
  rngen_t rng;
  double total;
  std::map<double, T> contents;
public:
  WeightedDist(const rngen_t &rng, const std::set<std::pair<double, T> > &initial)
    : rng(rng), total(0) {
    for (typename std::set<std::pair<double, T> >::const_iterator i =
	   initial.begin();
	 i != initial.end();
	 ++i) {
      total += i->first;
      contents.insert(std::make_pair(total, i->second));
    }
  }
  virtual T operator()() {
    return contents.lower_bound(
      boost::uniform_real<>(0, total)(rng))->second;
  }
};

template <typename T, typename U>
class SequentialDist : public Distribution<T> {
  rngen_t rng;
  std::vector<T> contents;
  typename std::vector<T>::iterator cur;
public:
  SequentialDist(rngen_t rng, U &initial) : rng(rng) {
    contents.insert(initial.begin(), initial.end());
    cur = contents.begin();
  }
  virtual T operator()() {
    assert(contents.size());
    if (cur == contents.end())
      cur = contents.begin();
    return *(cur++);
  }
};

class UniformRandom : public Distribution<uint64_t> {
  rngen_t rng;
  uint64_t min;
  uint64_t max;
public:
  UniformRandom(const rngen_t &rng, uint64_t min, uint64_t max) :
    rng(rng), min(min), max(max) {}
  virtual uint64_t operator()() {
    return boost::uniform_int<uint64_t>(min, max)(rng);
  }
};

class Align : public Distribution<uint64_t> {
  boost::scoped_ptr<Distribution<uint64_t> > dist;
  uint64_t align;
public:
  Align(Distribution<uint64_t> *dist, uint64_t align) :
    dist(dist), align(align) {}
  virtual uint64_t operator()() {
    uint64_t ret = (*dist)();
    return ret - (ret % align);
  }
};

class Uniform : public Distribution<uint64_t> {
  uint64_t val;
public:
  explicit Uniform(uint64_t val) : val(val) {}
  virtual uint64_t operator()() {
    return val;
  }
};

#endif
