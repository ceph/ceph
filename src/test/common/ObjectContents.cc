// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "ObjectContents.h"
#include "include/buffer.h"
#include <iostream>
#include <map>

bool test_object_contents()
{
  ObjectContents c, d;
  assert(!c.exists());
  c.debug(std::cerr);
  c.write(10, 10, 10);
  assert(c.exists());
  assert(c.size() == 20);

  c.debug(std::cerr);
  bufferlist bl;
  for (ObjectContents::Iterator iter = c.get_iterator();
       iter.valid();
       ++iter) {
    bl.append(*iter);
  }
  assert(bl.length() == 20);

  bufferlist bl2;
  for (unsigned i = 0; i < 8; ++i) bl2.append(bl[i]);
  c.write(10, 8, 4);
  c.debug(std::cerr);
  ObjectContents::Iterator iter = c.get_iterator();
  iter.seek_to(8);
  for (uint64_t i = 8;
       i < 12;
       ++i, ++iter) {
    bl2.append(*iter);
  }
  for (unsigned i = 12; i < 20; ++i) bl2.append(bl[i]);
  assert(bl2.length() == 20);

  for (ObjectContents::Iterator iter3 = c.get_iterator();
       iter.valid();
       ++iter) {
    assert(bl2[iter3.get_pos()] == *iter3);
  }

  assert(bl2[0] == '\0');
  assert(bl2[7] == '\0');

  interval_set<uint64_t> to_clone;
  to_clone.insert(5, 10);
  d.clone_range(c, to_clone);
  assert(d.size() == 15);

  c.debug(std::cerr);
  d.debug(std::cerr);

  ObjectContents::Iterator iter2 = d.get_iterator();
  iter2.seek_to(5);
  for (uint64_t i = 5; i < 15; ++i, ++iter2) {
    std::cerr << "i is " << i << std::endl;
    assert(iter2.get_pos() == i);
    assert(*iter2 == bl2[i]);
  }
  return true;
}


unsigned int ObjectContents::Iterator::get_state(uint64_t _pos)
{
  if (parent->seeds.count(_pos)) {
    return parent->seeds[_pos];
  }
  seek_to(_pos - 1);
  return current_state;
}

void ObjectContents::clone_range(ObjectContents &other,
				 interval_set<uint64_t> &intervals)
{
  interval_set<uint64_t> written_to_clone;
  written_to_clone.intersection_of(intervals, other.written);

  interval_set<uint64_t> zeroed = intervals;
  zeroed.subtract(written_to_clone);

  written.union_of(intervals);
  written.subtract(zeroed);

  for (interval_set<uint64_t>::iterator i = written_to_clone.begin();
       i != written_to_clone.end();
       ++i) {
    uint64_t start = i.get_start();
    uint64_t len = i.get_len();

    unsigned int seed = get_iterator().get_state(start+len);

    seeds[start+len] = seed;
    seeds.erase(seeds.lower_bound(start), seeds.lower_bound(start+len));

    seeds[start] = other.get_iterator().get_state(start);
    seeds.insert(other.seeds.upper_bound(start),
		 other.seeds.lower_bound(start+len));
  }

  if (intervals.range_end() > _size)
    _size = intervals.range_end();
  _exists = true;
  return;
}

void ObjectContents::write(unsigned int seed,
			   uint64_t start,
			   uint64_t len)
{
  _exists = true;
  unsigned int _seed = get_iterator().get_state(start+len);
  seeds[start+len] = _seed;
  seeds.erase(seeds.lower_bound(start),
	      seeds.lower_bound(start+len));
  seeds[start] = seed;

  interval_set<uint64_t> to_write;
  to_write.insert(start, len);
  written.union_of(to_write);

  if (start + len > _size)
    _size = start + len;
  return;
}
