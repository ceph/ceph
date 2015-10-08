// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>
#include <iostream>

#include "Object.h"

void ContDesc::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(objnum, bl);
  ::encode(cursnap, bl);
  ::encode(seqnum, bl);
  ::encode(prefix, bl);
  ::encode(oid, bl);
  ENCODE_FINISH(bl);
}

void ContDesc::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(objnum, bl);
  ::decode(cursnap, bl);
  ::decode(seqnum, bl);
  ::decode(prefix, bl);
  ::decode(oid, bl);
  DECODE_FINISH(bl);
}

ostream &operator<<(ostream &out, const ContDesc &rhs)
{
  return out << "(ObjNum " << rhs.objnum
	     << " snap " << rhs.cursnap
	     << " seq_num " << rhs.seqnum
    //<< " prefix " << rhs.prefix
	     << ")";
}

void AppendGenerator::get_ranges_map(
  const ContDesc &cont, map<uint64_t, uint64_t> &out) {
  RandWrap rand(cont.seqnum);
  uint64_t pos = off;
  uint64_t limit = off + get_append_size(cont);
  while (pos < limit) {
    uint64_t segment_length = round_up(
      rand() % (max_append_size - min_append_size),
      alignment) + min_append_size;
    assert(segment_length >= min_append_size);
    if (segment_length + pos > limit) {
      segment_length = limit - pos;
    }
    if (alignment)
      assert(segment_length % alignment == 0);
    out.insert(pair<uint64_t, uint64_t>(pos, segment_length));
    pos += segment_length;
  }
}

void VarLenGenerator::get_ranges_map(
  const ContDesc &cont, map<uint64_t, uint64_t> &out) {
  RandWrap rand(cont.seqnum);
  uint64_t pos = 0;
  uint64_t limit = get_length(cont);
  bool include = false;
  while (pos < limit) {
    uint64_t segment_length = (rand() % (max_stride_size - min_stride_size)) + min_stride_size;
    assert(segment_length < max_stride_size);
    assert(segment_length >= min_stride_size);
    if (segment_length + pos > limit) {
      segment_length = limit - pos;
    }
    if (include) {
      out.insert(pair<uint64_t, uint64_t>(pos, segment_length));
      include = false;
    } else {
      include = true;
    }
    pos += segment_length;
  }
}

ObjectDesc::iterator &ObjectDesc::iterator::advance(bool init) {
  assert(pos < limit);
  assert(!end());
  if (!init) {
    pos++;
  }
  if (end()) {
    return *this;
  }
  while (pos == limit) {
    cur_cont = stack.begin()->first;
    limit = stack.begin()->second;
    stack.pop_front();
  }

  if (cur_cont == obj.layers.end()) {
    return *this;
  }

  interval_set<uint64_t> ranges;
  cur_cont->first->get_ranges(cur_cont->second, ranges);
  while (!ranges.contains(pos)) {
    stack.push_front(pair<list<pair<ceph::shared_ptr<ContentsGenerator>,
				    ContDesc> >::iterator,
		     uint64_t>(cur_cont, limit));
    uint64_t length = cur_cont->first->get_length(cur_cont->second);
    uint64_t next;
    if (pos >= length) {
      next = limit;
      cur_cont = obj.layers.end();
    } else if (ranges.empty() || pos >= ranges.range_end()) {
      next = length;
      ++cur_cont;
    } else {
      next = ranges.start_after(pos);
      ++cur_cont;
    }
    if (next < limit) {
      limit = next;
    }
    if (cur_cont == obj.layers.end()) {
      break;
    }

    ranges.clear();
    cur_cont->first->get_ranges(cur_cont->second, ranges);
  }

  if (cur_cont == obj.layers.end()) {
    return *this;
  }

  if (!cont_iters.count(cur_cont->second)) {
    cont_iters.insert(pair<ContDesc,ContentsGenerator::iterator>(
			cur_cont->second,
			cur_cont->first->get_iterator(cur_cont->second)));
  }
  map<ContDesc,ContentsGenerator::iterator>::iterator j = cont_iters.find(
    cur_cont->second);
  assert(j != cont_iters.end());
  j->second.seek(pos);
  return *this;
}

const ContDesc &ObjectDesc::most_recent() {
  return layers.begin()->second;
}

void ObjectDesc::update(ContentsGenerator *gen, const ContDesc &next) {
  layers.push_front(pair<ceph::shared_ptr<ContentsGenerator>, ContDesc>(ceph::shared_ptr<ContentsGenerator>(gen), next));
  return;
}

bool ObjectDesc::check(bufferlist &to_check) {
  iterator i = begin();
  uint64_t pos = 0;
  for (bufferlist::iterator p = to_check.begin();
       !p.end();
       ++p, ++i, ++pos) {
    if (i.end()) {
      std::cout << "reached end of iterator first" << std::endl;
      return false;
    }
    if (*i != *p) {
      std::cout << "incorrect buffer at pos " << pos << std::endl;
      return false;
    }
  }
  uint64_t size = layers.empty() ? 0 : 
    most_recent_gen()->get_length(most_recent());
  if (pos != size) {
    std::cout << "only read " << pos << " out of size " << size << std::endl;
    return false;
  }
  return true;
}

bool ObjectDesc::check_sparse(const std::map<uint64_t, uint64_t>& extents,
			      bufferlist &to_check) {
  auto i = begin();
  auto p = to_check.begin();
  uint64_t pos = 0;
  for (auto extent : extents) {
    const uint64_t start = extent.first;
    const uint64_t end = start + extent.second;
    for (; pos < end; ++i, ++pos) {
      if (i.end()) {
	std::cout << "reached end of iterator first" << std::endl;
	return false;
      }
      if (pos < start) {
	// check the hole
	if (*i != '\0') {
	  std::cout << "incorrect buffer at pos " << pos << std::endl;
	  return false;
	}
      } else {
	// then the extent
	if (*i != *p) {
	  std::cout << "incorrect buffer at pos " << pos << std::endl;
	  return false;
	}
	++p;
      }
    }
  }
  uint64_t size = layers.empty() ? 0 :
    most_recent_gen()->get_length(most_recent());
  if (pos != size) {
    std::cout << "only read " << pos << " out of size " << size << std::endl;
    return false;
  }
  return true;
}
