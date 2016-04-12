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

std::ostream &operator<<(std::ostream &out, const ContDesc &rhs)
{
  return out << "(ObjNum " << rhs.objnum
	     << " snap " << rhs.cursnap
	     << " seq_num " << rhs.seqnum
    //<< " prefix " << rhs.prefix
	     << ")";
}

void AppendGenerator::get_ranges_map(
  const ContDesc &cont, std::map<uint64_t, uint64_t> &out) {
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
    out.insert(std::pair<uint64_t, uint64_t>(pos, segment_length));
    pos += segment_length;
  }
}

void VarLenGenerator::get_ranges_map(
  const ContDesc &cont, std::map<uint64_t, uint64_t> &out) {
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
      out.insert(std::pair<uint64_t, uint64_t>(pos, segment_length));
      include = false;
    } else {
      include = true;
    }
    pos += segment_length;
  }
}

void ObjectDesc::iterator::adjust_stack() {
  while (!stack.empty() && pos >= stack.front().second.next) {
    assert(pos == stack.front().second.next);
    size = stack.front().second.size;
    current = stack.front().first;
    stack.pop_front();
  }

  if (stack.empty()) {
    cur_valid_till = std::numeric_limits<uint64_t>::max();
  } else {
    cur_valid_till = stack.front().second.next;
  }

  while (current != layers.end() && !current->covers(pos)) {
    uint64_t next = current->next(pos);
    if (next < cur_valid_till) {
      stack.push_front(
	make_pair(
	  current,
	  StackState{next, size}
	  )
	);
      cur_valid_till = next;
    }

    ++current;
  }

  if (current == layers.end()) {
    size = 0;
  } else {
    current->iter.seek(pos);
    size = std::min(size, current->get_size());
    cur_valid_till = std::min(
      current->valid_till(pos),
      cur_valid_till);
  }
}

const ContDesc &ObjectDesc::most_recent() {
  return layers.begin()->second;
}

void ObjectDesc::update(ContentsGenerator *gen, const ContDesc &next) {
  layers.push_front(std::pair<ceph::shared_ptr<ContentsGenerator>, ContDesc>(ceph::shared_ptr<ContentsGenerator>(gen), next));
  return;
}

bool ObjectDesc::check(bufferlist &to_check) {
  iterator objiter = begin();
  uint64_t error_at = 0;
  if (!objiter.check_bl_advance(to_check, &error_at)) {
    std::cout << "incorrect buffer at pos " << error_at << std::endl;
    return false;
  }

  uint64_t size = layers.begin()->first->get_length(layers.begin()->second);
  if (to_check.length() < size) {
    std::cout << "only read " << to_check.length()
	      << " out of size " << size << std::endl;
    return false;
  }
  return true;
}

bool ObjectDesc::check_sparse(const std::map<uint64_t, uint64_t>& extents,
			      bufferlist &to_check)
{
  uint64_t off = 0;
  uint64_t pos = 0;
  auto objiter = begin();
  for (auto &&extiter : extents) {
    // verify hole
    {
      bufferlist bl;
      bl.append_zero(extiter.first - pos);
      uint64_t error_at = 0;
      if (!objiter.check_bl_advance(bl, &error_at)) {
	std::cout << "sparse read omitted non-zero data at "
		  << error_at << std::endl;
	return false;
      }
    }

    assert(off <= to_check.length());
    pos = extiter.first;
    objiter.seek(pos);

    {
      bufferlist bl;
      bl.substr_of(
	to_check,
	off,
	std::min(to_check.length() - off, extiter.second));
      uint64_t error_at = 0;
      if (!objiter.check_bl_advance(bl, &error_at)) {
	std::cout << "incorrect buffer at pos " << error_at << std::endl;
	return false;
      }
      off += extiter.second;
      pos += extiter.second;
    }

    if (pos < extiter.first + extiter.second) {
      std::cout << "reached end of iterator first" << std::endl;
      return false;
    }
  }

  // final hole
  bufferlist bl;
  uint64_t size = layers.begin()->first->get_length(layers.begin()->second);
  bl.append_zero(size - pos);
  uint64_t error_at;
  if (!objiter.check_bl_advance(bl, &error_at)) {
    std::cout << "sparse read omitted non-zero data at "
	      << error_at << std::endl;
    return false;
  }
  return true;
}
