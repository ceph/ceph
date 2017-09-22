#ifndef CEPH_ABS_INTERVAL_SET_H
#define CEPH_ABS_INTERVAL_SET_H

#include <iterator>
#include <ostream>
#include <utility>

#include "include/assert.h"

namespace abs_interval_set {

  namespace iter_wrappers {

    template<class Iter>
    inline void* copy(const void* iter) {
      return reinterpret_cast<void*>(new Iter(
          *reinterpret_cast<const Iter*>(iter)));
    }

    template<class Iter>
    inline void assign(void* lhs, const void* rhs) {
      *reinterpret_cast<Iter*>(lhs) = *reinterpret_cast<const Iter*>(rhs);
    }

    template<class Iter>
    inline void del (void* iter) {
      delete reinterpret_cast<Iter*>(iter);
    }

    template<class Iter>
    inline bool eq(const void* iter, const void* rhs) {
      return *reinterpret_cast<const Iter*>(iter) ==
          *reinterpret_cast<const Iter*>(rhs);
    }

    template<class Iter>
    inline void inc(void* iter) {
      ++(*reinterpret_cast<Iter*>(iter));
    }

    template<class Iter>
    inline void dec(void* iter) {
      --(*reinterpret_cast<Iter*>(iter));
    }

    template<class Key, class T, class Iter>
    inline std::pair<const Key, T> deref(const void* iter) {
      return *(*reinterpret_cast<const Iter*>(iter));
    }

    template<class T, class Iter>
    inline T get_len(const void* iter) {
      return (*(*reinterpret_cast<const Iter*>(iter))).second;
    }

    template<class Key, class Iter>
    inline Key get_start(const void* iter) {
      return (*(*reinterpret_cast<const Iter*>(iter))).first;
    }

    template<class Key, class Iter>
    inline Key get_end(const void* iter) {
      return (*(*reinterpret_cast<const Iter*>(iter))).first +
          (*(*reinterpret_cast<const Iter*>(iter))).second;
    }

    template<class T, class Iter>
    inline void set_len(void* iter, const T& value) {
      (*reinterpret_cast<Iter*>(iter))->second = value;
    }
  }

  template<class Key, class T>
  struct iter_funcs {
    void* (*copy)(const void*);
    void (*assign)(void*, const void*);
    void (*del)(void*);
    bool (*eq)(const void*, const void*);
    void (*inc)(void*);
    void (*dec)(void*);
    /*
     * Dereference is return by value, as a means to hide differences
     * between pair<const Key,T> used by map and pair<Key,T> used by
     * vector (vector elements are not allowed to contain const
     * members). Copy elision (RVO) will optimize away unecessary
     * copies when possible.
     */
    std::pair<const Key, T>(*deref)(const void*);
    T(*get_len)(const void*);
    Key(*get_start)(const void*);
    Key(*get_end)(const void*);
    void (*set_len)(void*, const T&);
  };

  template<class Key, class T, class Iter>
  iter_funcs<Key,T>* mut_iter_funcs() {
    static iter_funcs<Key,T> funcs{
      iter_wrappers::copy<Iter>,
      iter_wrappers::assign<Iter>,
      iter_wrappers::del<Iter>,
      iter_wrappers::eq<Iter>,
      iter_wrappers::inc<Iter>,
      iter_wrappers::dec<Iter>,
      iter_wrappers::deref<Key,T,Iter>,
      iter_wrappers::get_len<T,Iter>,
      iter_wrappers::get_start<Key,Iter>,
      iter_wrappers::get_end<Key,Iter>,
      iter_wrappers::set_len<T,Iter>
    };
    return &funcs;
  }

  template<class Key, class T, class Iter>
  iter_funcs<Key,T>* const_iter_funcs() {
    static iter_funcs<Key,T> funcs{
      iter_wrappers::copy<Iter>,
      iter_wrappers::assign<Iter>,
      iter_wrappers::del<Iter>,
      iter_wrappers::eq<Iter>,
      iter_wrappers::inc<Iter>,
      iter_wrappers::dec<Iter>,
      iter_wrappers::deref<Key,T,Iter>,
      iter_wrappers::get_len<T,Iter>,
      iter_wrappers::get_start<Key,Iter>,
      iter_wrappers::get_end<Key,Iter>,
      NULL
    };
    return &funcs;
  }

  template<class Key, class T>
  class iterator: public std::iterator <
      std::forward_iterator_tag, std::pair<const Key, T>> {

    public:
      iterator(): _iter(NULL), _funcs(NULL) {}
      template<class Iter>
      explicit iterator(const Iter& impl):
          _iter(reinterpret_cast<void*>(new Iter(impl))),
          _funcs(mut_iter_funcs<Key,T,Iter>()) {}

      iterator(const iterator& iter) {
        if (iter._funcs != NULL)
          _iter = iter._funcs->copy(iter._iter);
        else
          _iter = NULL;
        _funcs = iter._funcs;
      }

      iterator &operator=(const iterator &iter) {
        if (_funcs != NULL) {
          if (_funcs == iter._funcs) {
            _funcs->assign(_iter, &iter);
            return *this;
          }
          else
            _funcs->del(_iter);
        }
        _funcs = iter._funcs;
        if (_funcs != NULL)
          _iter = _funcs->copy(iter._iter);
        else
          _iter = NULL;
        return *this;
      }

      template<class Iter>
      iterator &operator=(const Iter &iter) {
        assert(_funcs != NULL);
        _funcs->assign(_iter, &iter);
        return *this;
      }

      ~iterator() {
        if (_funcs != NULL)
          _funcs->del(_iter);
      }

      bool operator==(const iterator<Key,T>& rhs) const {
        return _funcs->eq(_iter, rhs._iter);
      }

      bool operator!=(const iterator<Key,T>& rhs) const {
        return !_funcs->eq(_iter, rhs._iter);
      }

      std::pair<const Key, T> operator*() const {
        return _funcs->deref(_iter);
      }

     /*
      * -> is not supported because deref is
      * return by value
      */
     /*
      std::pair<const Key, T> *operator->() {
        return &(_funcs->deref(_iter));
      }
      */

      Key get_start() const {
        return _funcs->get_start(_iter);
      }

      Key get_end() const {
        return _funcs->get_end(_iter);
      }

      T get_len() const {
        return _funcs->get_len(_iter);
      }

      void set_len(T value) {
        _funcs->set_len(_iter, value);
      }

      iterator<Key,T> &operator++() {
        _funcs->inc(_iter);
        return *this;
      }

      iterator<Key,T> operator++(int) {
        iterator<Key,T> prev(*this);
        _funcs->inc(_iter);
        return prev;
      }

      iterator<Key,T> &operator--() {
        _funcs->dec(_iter);
        return *this;
      }

      iterator<Key,T> operator--(int) {
        iterator<Key,T> prev(*this);
        _funcs->dec(_iter);
        return prev;
      }

      const void* get_impl() const {
        return _iter;
      }

    private:
      void *_iter;                    // dynamically allocated
      iter_funcs<Key,T>* _funcs;      // statically allocated
  };

  template<class Key, class T>
  class const_iterator:
      public std::iterator <
          std::forward_iterator_tag, std::pair<const Key, T>> {

    public:
      const_iterator(): _iter(NULL), _funcs(NULL) {}

     /*
      * Support implicit conversion from mutable iterator, since
      * the interval_set lower_bound method can return a mutable
      * iterator when we attempt to assign to a const_iterator
      * variable.
      */
      template<class Iter>
      const_iterator(const Iter& impl):
          _iter(reinterpret_cast<void*>(new Iter(impl))),
          _funcs(const_iter_funcs<Key,T,Iter>()) {}

      const_iterator(const const_iterator& iter) {
        if (iter._funcs != NULL)
          _iter = iter._funcs->copy(iter._iter);
        else
          _iter = NULL;
        _funcs = iter._funcs;
      }

      const_iterator &operator=(const const_iterator &iter) {
        if (_funcs != NULL)
          _funcs->del(_iter);
        _funcs = iter._funcs;
        if (_funcs != NULL)
          _iter = _funcs->copy(iter._iter);
        else
          _iter = NULL;
        return *this;
      }

      ~const_iterator() {
        if (_funcs != NULL)
          _funcs->del(_iter);
      }

      bool operator==(const const_iterator<Key,T>& rhs) const {
        return _funcs->eq(_iter, rhs._iter);
      }

      bool operator!=(const const_iterator<Key,T>& rhs) const {
        return !_funcs->eq(_iter, rhs._iter);
      }

      const std::pair<const Key, T> operator*() const {
        return _funcs->deref(_iter);
      }

     /*
      * -> is not supported because deref is
      * return by value
      */
     /*
      std::pair<const Key, T> *operator->() {
        return &(_funcs->deref(_iter));
      }
      */

      Key get_start() const {
        return _funcs->get_start(_iter);
      }

      Key get_end() const {
        return _funcs->get_end(_iter);
      }

      T get_len() const {
        return _funcs->get_len(_iter);
      }

      const_iterator<Key,T> &operator++() {
        _funcs->inc(_iter);
        return *this;
      }

      const_iterator<Key,T> operator++(int) {
        const_iterator<Key,T> prev(*this);
        _funcs->inc(_iter);
        return prev;
      }

      const_iterator<Key,T> &operator--() {
        _funcs->dec(_iter);
        return *this;
      }

      const_iterator<Key,T> operator--(int) {
        const_iterator<Key,T> prev(*this);
        _funcs->dec(_iter);
        return prev;
      }

      const void* get_impl() const {
        return _iter;
      }

    private:
      void *_iter;                  // dynamically allocated
      iter_funcs<Key,T>* _funcs;    // statically allocated
  };


  template<typename T>
  class interval_set {
    public:
      virtual ~interval_set() {}
      typedef std::pair<const T, T> value_type;
      typedef abs_interval_set::iterator<T,T> iterator;
      typedef abs_interval_set::const_iterator<T,T> const_iterator;
      virtual iterator begin() = 0;
      virtual iterator end() = 0;
      virtual const_iterator begin() const = 0;
      virtual const_iterator end() const = 0;
      virtual int64_t size() const = 0;
      virtual int num_intervals() const = 0;
      virtual void clear() = 0;
      virtual bool empty() const = 0;
      virtual T range_start() const = 0;
      virtual T range_end() const = 0;

      /*
       * Allow comparison of pair<const T, T> with pair<T, T>, since
       * vector does not allow elements to contain const members.
       */
      template <class ValueTypeA, class ValueTypeB>
      static bool item_equiv(const ValueTypeA& lhs, const ValueTypeB& rhs) {
        return lhs.first == rhs.first && lhs.second == rhs.second;
      }

      template <class ISet>
      bool operator==(const ISet &rhs) const {
        if (size() != rhs.size() || num_intervals() != rhs.num_intervals())
          return false;
        auto rhsi = rhs.get_raw_data().begin();
        auto rhs_end = rhs.get_raw_data().end();
        for (auto i = begin(); rhsi != rhs_end; ++i, ++rhsi)
          if (!item_equiv(*i, *rhsi))
            return false;
        return true;
      }

      template <class ISet>
      bool operator!=(const ISet &rhs) const {
        return !(*this == rhs);
      }

     /*
      * NOTE: Callers, like swap and union_of methods, need to use templates
      * in order to give this method access to the get_raw_data() method of
      * the derived type. Due to covariance, the get_raw_data() method cannot
      * be virtual.
      */
      template <class ISet>
      interval_set& operator=(const ISet &rhs) {
        clear();
        iterator i = begin();
        const auto rhs_end = rhs.get_raw_data().end();
        for (auto rhsi = rhs.get_raw_data().begin(); rhsi != rhs_end; ++rhsi)
          insert(i, *rhsi);
        return *this;
      }

      template <class ISetA, class ISetB>
      void intersection_of(const ISetA &a, const ISetB &b) {
        assert(&a != this);
        assert(&b != this);
        clear();

        const interval_set *s, *l;

        if (a.size() < b.size()) {
          s = reinterpret_cast<const interval_set*>(&a);
          l = reinterpret_cast<const interval_set*>(&b);
        } else {
          s = reinterpret_cast<const interval_set*>(&b);
          l = reinterpret_cast<const interval_set*>(&a);
        }

        if (!s->size())
          return;

        /*
         * Use the lower_bound algorithm for larger size ratios
         * where it performs better, but not for smaller size
         * ratios where sequential search performs better.
         */
        if (l->size() / s->size() >= 10) {
          /*
           * Maximize performance by passing through derived types so
           * that get_raw_data() can be used to access the underlying
           * iterator types.
           */
          if (s == &a)
            intersection_size_asym(*reinterpret_cast<const ISetA*>(s),
                *reinterpret_cast<const ISetB*>(l));
          else
            intersection_size_asym(*reinterpret_cast<const ISetB*>(s),
                *reinterpret_cast<const ISetA*>(l));
          return;
        }

        auto pa = a.get_raw_data().begin();
        auto pb = b.get_raw_data().begin();
        const auto a_end = a.get_raw_data().end();
        const auto b_end = b.get_raw_data().end();
        iterator mi = begin();

        while (pa != a_end && pb != b_end) {
          // passing?
          if (pa->first + pa->second <= pb->first)
            { ++pa;  continue; }
          if (pb->first + pb->second <= pa->first)
            { ++pb;  continue; }

          if (item_equiv(*pa, *pb)) {
            do {
              insert(mi, *pa);
              ++pa;
              ++pb;
            } while (pa != a_end && pb != b_end && item_equiv(*pa, *pb));
            continue;
          }

          T start = std::max<T>(pa->first, pb->first);
          T en = std::min<T>(pa->first+pa->second, pb->first+pb->second);
          assert(en > start);
          insert(mi, {start, en - start});
          if (pa->first+pa->second > pb->first+pb->second)
            ++pb;
          else
            ++pa;
        }
      }

    protected:
      virtual void insert(iterator&, const value_type&) = 0;

    private:

      template <class ISetA, class ISetB>
      void intersection_size_asym(const ISetA &s, const ISetB &l) {
        auto ps = s.get_raw_data().begin();
        const auto s_end = s.get_raw_data().end();
        const auto l_end = l.get_raw_data().end();
        auto pl = l.get_raw_data().begin();
        assert(ps != s_end);
        T offset = ps->first, prev_offset;
        bool first = true;
        iterator mi = begin();

        while (1) {
          if (first)
            first = false;
          else
            assert(offset > prev_offset);
          pl = l.find_inc(offset);
          prev_offset = offset;
          if (pl == l_end)
            break;
          while (ps != s_end && ps->first + ps->second <= pl->first)
            ++ps;
          if (ps == s_end)
            break;
          offset = pl->first + pl->second;
          if (offset <= ps->first) {
            offset = ps->first;
            continue;
          }

          if (item_equiv(*ps, *pl)) {
            do {
              insert(mi, *ps);
              ++ps;
              ++pl;
            } while (ps != s_end && pl != l_end && item_equiv(*ps, *pl));
            if (ps == s_end)
              break;
            offset = ps->first;
            continue;
          }

          T start = std::max<T>(ps->first, pl->first);
          T en = std::min<T>(ps->first + ps->second, offset);
          assert(en > start);
          insert(mi, {start, en - start});
          if (ps->first + ps->second <= offset) {
            ++ps;
            if (ps == s_end)
              break;
            offset = ps->first;
          }
        }
      }
  };


  template<class T>
  inline std::ostream& operator<<(std::ostream& out, const interval_set<T> &s) {
    const auto s_end = s.end();
    out << "[";
    const char *prequel = "";
    for (typename interval_set<T>::const_iterator i = s.begin();
         i != s_end;
         ++i)
    {
      out << prequel << i.get_start() << "~" << i.get_len();
      prequel = ",";
    }
    out << "]";
    return out;
  }

}

#endif
