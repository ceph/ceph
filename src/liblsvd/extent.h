// file:        extent.h
// description: Extent map for S3 Block Device
// author:      Peter Desnoyers, Northeastern University
//              Copyright 2021, 2022 Peter Desnoyers
// license:     GNU LGPL v2.1 or newer
//              LGPL-2.1-or-later
//

// There may be an elegant way to do this in C++; this isn't it.

// Defines four types of map:
//  objmap (int64_t => obj_offset)   - maps LBA to obj+offset
//  cachemap (obj_offset => int64_t) - maps obj+offset to LBA
//  cachemap2 (int64_t => int64_t)   - maps vLBA to pLBA
//  bufmap (int64_t => sector_ptr)   - maps LBA to char*
//
// Update/trim takes a pointer to a vector for discarded extents, so
// we don't have to search the map twice, and we can use read-only
// iterators for lookup
//
// extents have A and D bits:
//   set - access() / dirty() - set
//   get - a() / d()
// note that D bit is set internally, while A bit is set by the user
#ifndef EXTENT_H
#define EXTENT_H

#include <cstddef>
#include <stdint.h>
#include <stdlib.h>
#include <vector>
#include <set>
#include <tuple>
#include <cassert>

namespace extmap {

    // we support three map outputs:
    // LBA        - just an int64_t
    // obj_offset - sequence number + LBA offset
    // sector_ptr - points into a memory buffer. ptr increments by 512
    //
    struct obj_offset {
	int64_t obj    : 36;
	int64_t offset : 28;	// 128GB
    public:
	obj_offset operator+=(int val) {
	    offset += val;
	    return *this;
	}
	bool operator==(const obj_offset other) const {
	    return (obj == other.obj) && (offset == other.offset);
	}
	bool operator<(const obj_offset other) const {
	    return (obj == other.obj) ? (offset < other.offset) : (obj < other.obj);
	}
	bool operator>(const obj_offset other) const {
	    return (obj == other.obj) ? (offset > other.offset) : (obj > other.obj);
	}
	bool operator>=(const obj_offset other) const {
	    return (obj == other.obj) ? (offset >= other.offset) : (obj >= other.obj);
	}
	bool operator<=(const obj_offset other) const {
	    return (obj == other.obj) ? (offset <= other.offset) : (obj <= other.obj);
	}
	obj_offset operator+(int val) {
	    return (obj_offset){.obj = obj, .offset = offset + val};
	}
	int operator-(const obj_offset other) {
	    return offset - other.offset; // meaningless if obj != other.obj
	}
    };

    struct sector_ptr {
	char *buf;
    public:
	sector_ptr(char *ptr) {
	    buf = ptr;
	}
	sector_ptr() {}
	sector_ptr operator+=(int val) {
	    buf += val*512;
	    return *this;
	}
	bool operator<(const sector_ptr other) const {
	    return buf < other.buf;
	}
	bool operator>(const sector_ptr other) const {
	    return buf > other.buf;
	}
	bool operator==(const sector_ptr other) const {
	    return buf == other.buf;
	}
	sector_ptr operator+(int val) {
	    sector_ptr p(buf + val*512);
	    return p;
	}
	int operator-(const sector_ptr val) {
	    return (buf - val.buf) / 512;
	}
    };
    
    // These are the three map types we support. There's probably a way to 
    // do this with a template, but I don't think it's worth the effort
    // these are the actual structures stored in the map
    //
    struct _lba2buf {
	uint64_t    a    : 1;
	uint64_t    d    : 1;
	uint64_t    base : 38;
	uint64_t    len  : 24;
	sector_ptr ptr;
    };
	
    struct _obj2lba {
	obj_offset base;
	uint64_t   a   : 1;
	uint64_t   d   : 1;
	uint64_t   len : 24;
	int64_t    ptr : 38;	// LBA
    };

    struct _lba2lba {		// TODO: any way to do this in 12 bytes?
	int64_t  a    : 1;
	int64_t  d    : 1;
	int64_t  base : 38;	// LBA
	int64_t  len  : 24;
	int64_t  ptr  : 40;	// LBA
	int64_t  pad  : 24;
    };
	
    struct _lba2obj {
	int64_t    a    : 1;
	int64_t    d    : 1;
	int64_t    base : 38;	// 128TB max
	int64_t    len  : 24;
	obj_offset ptr;
    };

    // Here's the wrapper around those types, so that we can use the same
    // map logic for all three of them
    //
    // Note that new extents are created with D=1, and rebase/relimit set D=1, so
    // the update logic never has to worry about dirty bits.
    //
    template <class T, class T_in, class T_out> 
    struct _extent {
	T s;
    public:
	_extent(T_in _base, int64_t _len, T_out _ptr) {
	    s.base = _base;
	    s.len = _len;
	    s.ptr = _ptr;
	    s.a = 0;
	    s.d = 1;		// new extents are dirty
	}
	_extent(T_in _base) {
	    s.base = _base;
	}
	_extent() {}

	// extents all get implemented as [base .. base+len) but the logic 
	// using them a lot easier if we work with [base .. limit)
	// 
	T_in base(void) { return s.base; }
	T_in limit(void) { return s.base + s.len; }
	T_out ptr(void) { return s.ptr; }
	void relimit(T_in _limit) {
	    s.len = _limit - s.base;
	    s.d = 1;		// dirty
	}

	// if we change the base, we need to update the pointer by the same amount
	//
	void rebase(T_in _base) {
	    auto delta = _base - s.base;
	    s.ptr += delta;
	    s.len -= delta;
	    s.base = _base;
	    s.d = 1;		// dirty
	}
	
	_extent operator+=(int val) {
	    s.ptr += val;
	    return *this;
	}
	bool operator<(const _extent &other) const {
	    return s.base < other.s.base;
	}

	// can't use a bitfield directly in make_tuple - need +0
	std::tuple<T_in, T_in, T_out> vals(void) {
            auto _ptr = s.ptr;
	    return std::make_tuple(s.base+0, s.base+s.len, _ptr);
	}

	std::tuple<T_in, T_in, T_out> vals(T_in _base, T_in _limit) {
	    auto _ptr = s.ptr;
	    if (s.base < _base)
		_ptr += (_base - s.base);
	    else
		_base = s.base;
	    _limit = std::min(limit(), _limit);
	    return std::make_tuple(_base, _limit, _ptr);
	}

	void access(bool val) {
	    s.a = (val ? 1 : 0);
	}
	void dirty(bool val) {
	    s.d = (val ? 1 : 0);
	}
	bool a(void) {
	    return s.a != 0;
	}
	bool d(void) {
	    return s.d != 0;
	}
    };

    // template <class T, class T_in, class T_out> 
    typedef _extent<_lba2buf,int64_t,sector_ptr> lba2buf;
    typedef _extent<_obj2lba,obj_offset,int64_t> obj2lba;
    typedef _extent<_lba2obj,int64_t,obj_offset> lba2obj;
    typedef _extent<_lba2lba,int64_t,int64_t>    lba2lba;

    // an extent map with entries of type T, which map from T_in to T_out
    //
    template <class T, class T_in, class T_out, int load = 256>
    struct extmap {
	static const int _load = load;

    public:
	typedef std::vector<T>      extent_vector;
	std::vector<extent_vector*> lists;
	std::vector<T_in>           maxes;
	int                         count;

	extmap(){ count = 0; }
	~extmap(){
	    for (auto l : lists)
		delete l;
	}
	
	// debug code
	//
	void verify_max(void) {
	}

	void first(T _e) {
	    auto vec = new extent_vector();
	    vec->reserve(_load);
	    vec->push_back(_e);
	    lists.push_back(vec);
	    maxes.push_back(_e.limit());
	    count = 1;
	}

	// iterator gets used both internally and externally
	// (returned by lookup function)
	//
	class iterator {
	    
	public:
	    extmap *m;
	    int     i;
	    typename extent_vector::iterator it;
	    
	    using iterator_category = std::random_access_iterator_tag;
	    using value_type = T;
	    using difference_type = std::ptrdiff_t;
	    using pointer = T*;
	    using reference = T&;

	    iterator() {}
	    
	    bool  operator==(const iterator &other) const {
		return m == other.m && i == other.i && it == other.it;
	    }

	    bool operator!=(const iterator &other) const {
		return m != other.m || i != other.i || it != other.it;
	    }
	    
	    iterator(extmap *m, int i, typename extent_vector::iterator it) {
		this->m = m;
		this->i = i;
		this->it = it;
	    }
	    
	    reference operator*() const {
		return *it;
	    }

	    pointer operator->() {
		return &(*it);
	    }
	    iterator& operator++(int) {
		assert(it < m->lists[i]->end());
		it++;
		if (it == m->lists[i]->end()) {
		    if (i+1 <  (int)m->lists.size()) {
			i++;
			it = m->lists[i]->begin();
		    }
		}
		return *this;
	    }
	    
	    // TODO: (begin()--)++ doesn't work correctly
	    iterator& operator--(int) {
		if (it == m->lists[i]->begin()) {
		    if (i > 0) {
			i--;
			it = m->lists[i]->end() - 1;
		    }
		}
		else
		    it--;
		return *this;
	    }

	    // it+1 / it-1 is useful in several cases. We never need to
	    // add/subtract more than 1, so the overhead of recursive solution
	    // isn't a big deal
	    //
	    iterator operator-(std::ptrdiff_t n) {
		if (n > 1)
		    return (*this + 1) + (n-1);
		if (it == m->lists[i]->begin() && i == 0)
		    return m->begin();
		if ((it == m->lists[i]->begin()))
		    return iterator(m, i-1, m->lists[i-1]->end() - 1);
		return iterator(m, i, it-1);
	    }
	    iterator operator+(std::ptrdiff_t n) {
		if (n > 1)
		   return (*this + 1) + (n-1);
		if (it+1 == m->lists[i]->end()) {
		    if (i+1 == (int)m->lists.size())
			return m->end();
		    else
			return iterator(m, i+1, m->lists[i+1]->begin());
		}
		return iterator(m, i, it+1);
	    }

//	    bool operator<(T_in val) const {
//		return (*this == m->end() || it->base < val);
//	    }
	};
	
	extent_vector _tmp; // HACK! HACK!
	iterator begin() {
            if (lists.size() == 0)
                return iterator(this, 0, _tmp.begin());
	    return iterator(this, 0, lists[0]->begin());
	}

	iterator end() {
	    if (lists.size() == 0) 
		return iterator(this, 0, _tmp.end());
	    int n = lists.size()-1;
	    return iterator(this, n, lists[n]->end());
	}
	
	iterator lower_bound(T_in base) {
	    if (lists.size() == 0) 	// should never get called in this case?
		return end();

	    // search maxes to find the list containing @base
	    // remember that max is 1+highest legal addr
	    //
	    auto max_iter = std::lower_bound(maxes.begin(), maxes.end(), base);
	    if (max_iter != maxes.end() && base == *max_iter)
		max_iter++; // TODO delete? search base+1?
	    if (max_iter == maxes.end())
		return end();

	    // find lowest entry with base >= @base
	    //
	    auto i = max_iter - maxes.begin();
	    T _key(base);
	    auto list_iter = std::lower_bound(lists[i]->begin(), lists[i]->end(), _key);

	    // whoops, previous entry could have limit > @base...
	    //
	    if (list_iter != lists[i]->begin()) {
		auto prev = list_iter - 1;
		if (prev->base() <= base && prev->limit() > base)
		    return iterator(this, i, prev);
	    }

	    // this shouldn't happen??? because we searched max
	    //
	    if (i < (int)(lists.size()-1) && list_iter == lists[i]->end())
		return iterator(this, i+1, lists[i+1]->begin());

	    return iterator(this, i, list_iter);
	}

	// Following logic from Python sorted containers, by Grant Jenks.
	// https://github.com/grantjenks/python-sortedcontainers
	// sortedlist.py in particular
	//
	
	// Python-style list slicing - remove [len]..[end] and return it
	//
	static std::vector<T> *_slice(std::vector<T> *A, int len) {
	    auto half = new std::vector<T>();
	    half->reserve(_load);
	    for (auto it = A->begin()+len; it != A->end(); it++)
		half->push_back(*it);
	    A->resize(len);
	    return half;
	}

	// sortedlist._expand(self, pos)
	//
	iterator _expand(iterator it) {
	    if (lists[it.i]->size() >= _load * 2) {
		int j = it.it - lists[it.i]->begin();
		auto half = _slice(lists[it.i], _load);
		maxes[it.i] = lists[it.i]->back().limit();
		lists.insert(lists.begin()+it.i+1, half);
		maxes.insert(maxes.begin()+it.i+1, half->back().limit());
		if (j >= _load) {
		    j -= _load;
		    it.i++;
		}
		it.it = lists[it.i]->begin()+j;
	    }
	    return it;
	}

	// insert just before iterator 'it', return pointer to inserted value
	//
	iterator _insert(iterator it, T _e) {
	    if (count == 0) {
		first(_e);
		return begin();
	    }
	    it.it = lists[it.i]->insert(it.it, _e);
	    maxes[it.i] = lists[it.i]->back().limit();
	    count++;
	    return _expand(it);
	}
	
	// sortedlist._delete
	//
	iterator _erase(iterator it) {
	    it.it = lists[it.i]->erase(it.it);

	    // if there's only one list, this might delete it down to zero
	    if (lists[it.i]->size() > 0) 
		maxes[it.i] = lists[it.i]->back().limit();
	    else {
		delete lists[it.i];
		lists.erase(lists.begin()+it.i);
		maxes.erase(maxes.begin()+it.i);
	    }
	    count--;
	    if (lists.size() == 0)
		return end();
	    
	    if (lists[it.i]->size() > _load / 2)
		;
	    else if (lists.size() > 1) {
		int j = it.it - lists[it.i]->begin();
		int pos = (it.i > 0) ? it.i : it.i+1;
		int prev = pos-1;
		if (pos == it.i)
		    j += lists[prev]->size();

		lists[prev]->insert(lists[prev]->end(),
				    lists[pos]->begin(), lists[pos]->end());
		maxes[prev] = lists[prev]->back().limit();

		delete lists[pos];
		lists.erase(lists.begin()+pos);
		maxes.erase(maxes.begin()+pos);

		it = _expand(iterator(this, prev, lists[prev]->begin() + j));
	    }
	    else if (lists[it.i]->size() > 0)
		maxes[it.i] = lists[it.i]->back().limit();

	    if (it.it == lists[it.i]->end() && it.i != (int)lists.size()-1) {
		it.i++;
		it.it = lists[it.i]->begin();
	    }
	    return it;
	}

	// helper - can we merge these two extents?
	//
	static bool adjacent(T left, T right) {
	    return left.limit() == right.base() &&
		left.s.ptr + (left.limit() - left.base()) == right.s.ptr;
	}

	// update the map
	//    trim=T : just unmap [@base..@limit)
	//    trim=F : replace with [@base..@limit) -> [@e..@e+len)
	// removed extents are returned in del[] (if non-null) for GC tracking
	//
	void _update(T_in base, T_in limit, T_out e, bool trim, extent_vector *del) {
	    //= {.base = base, .limit = limit, .ext = e};
	    T _e(base, limit-base, e);

	    verify_max();

	    // special case inserting the first entry
	    //
	    if (maxes.size() == 0) {
		first(_e);
		return;
	    }

	    // TODO - old fixit() function?
	    verify_max();

	    // find the first extent with base >= @base
	    //
	    auto it = lower_bound(base);

	    if (it != end()) {
		// we bisect an extent
		//   [-----------------]       *it          _new
		//          [+++++]       -> [-----][+++++][----]
		//
		if (it->base() < base && it->limit() > limit) {
		    if (del != nullptr) {
			T _old(base,		    // base
			       limit - base,	    // len
			       it->s.ptr + (base - it->base())); // ptr
			del->push_back(_old);
		    }
		    T _new(limit, /* base */
			   it->limit() - limit, /* len */
			   it->s.ptr + (limit - it->base()));
		    it->relimit(base);
		    maxes[it.i] = lists[it.i]->back().limit();
		    it = _insert(it+1, _new);
		    verify_max();
		}
		
		// left-hand overlap
		//   [---------]
		//       [++++++++++]  ->  [----][++++++++++]
		//
		else if (it->base() < base && it->limit() > base) {
		    if (del != nullptr) {
			T _old(base,		    // base
			       it->limit() - base,  // len
			       it->s.ptr + (base - it->base())); // ptr
			del->push_back(_old);
		    }
		    it->relimit(base);
		    maxes[it.i] = lists[it.i]->back().limit();
		    it++;
		    verify_max();
		}

		// erase any extents fully overlapped
		//       [----] [---] 
		//   [+++++++++++++++++] -> [+++++++++++++++++]
		//
		while (it != end()) {
		    if (it->base() >= base && it->limit() <= limit) {
			if (del != nullptr) 
			    del->push_back(*it);
			it = _erase(it);
		    } else
			break;
		}

		// update right-hand overlap
		//        [---------]
		//   [++++++++++]        -> [++++++++++][----]
		//
		if (it != end() && limit > it->base()) {
		    if (del != nullptr) {
			T _old(it->base(),	   // base
			       limit - it->base(),  // len
			       it->s.ptr);
			del->push_back(_old);
		    }
		    //it->s.ptr += (limit - it->base()); // TODO is this right???
		    it->rebase(limit);
		    verify_max();
		}
	    }

	    // insert before 'it'
	    if (!trim) {
		if (count == 0) { // avoid valgrind error on (it-1)
		    _insert(it, _e);
		    return;
		}
		auto prev = it-1;
		if (it != begin() && adjacent(*prev, _e)) {
		    // we can merge with the previous extent
		    //
		    prev->relimit(limit);
		    maxes[prev.i] = lists[prev.i]->back().limit();
		    if (it != end() && adjacent(*prev, *it)) {
			// we plug a hole, and can merge with the next extent
			//
			prev->relimit(it->limit());
			maxes[prev.i] = lists[prev.i]->back().limit();
			_erase(it);
			verify_max();
			return;
		    }
		}
		else if (it != end() && adjacent(_e, *it)) {
		    // we can merge with the next extent
		    //
		    //it->s.ptr += (base - limit); // subtract
		    it->rebase(base);
		}
		else {
		    // no merging, just insert the damn thing
		    //
		    _insert(it, _e);
		    verify_max();
		}
	    }
	}

//    public:
	int size() {
	    return count;
	}

	int capacity() {
	    int sum = 0;
	    for (auto list : lists)
		sum += list->capacity();
	    return sum;
	}
	
	// lookup - returns iterator pointing to one of:
	// - extent containing @base
	// - lowest extent with base > @base
	// - end()

	// usage:
	// for (auto it = lookup(base); it != end() && it->base < limit; it++) 
	//    auto [base, limit, ptr] = it->vals(base, limit);
	//
	iterator lookup(T_in base) {
	    return lower_bound(base);
	}

	// various ways of calling _update...
	//
	void update(T_in base, T_in limit, T_out e, std::vector<T> *del) {
	    _update(base, limit, e, false, del);
	}
	void update(T_in base, T_in limit, T_out e) {
	    _update(base, limit, e, false, nullptr);
	}

	void trim(T_in base, T_in limit, std::vector<T> *del) {
	    static T_out unused;
	    _update(base, limit, unused, true, del);
	}
	void trim(T_in base, T_in limit) {
	    static T_out unused; // get rid of that damn "ininitialized message"
	    _update(base, limit, unused, true, nullptr);
	}
	void reset(void) {
	    for (auto l : lists)
		delete l;
	    lists.resize(0);
	    maxes.resize(0);
	    count = 0;
	}
    };

    // template <class T, class T_in, class T_out>
    typedef extmap<lba2obj,int64_t,obj_offset> objmap;
    typedef extmap<obj2lba,obj_offset,int64_t> cachemap;
    typedef extmap<lba2buf,int64_t,sector_ptr> bufmap;
    typedef extmap<lba2lba,int64_t,int64_t>    cachemap2;
}

#endif
