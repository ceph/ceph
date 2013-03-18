// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>

#include "Object.h"

ostream &operator<<(ostream &out, const ContDesc &rhs)
{
  return out << "ObjNum: " << rhs.objnum
	     << " snap: " << rhs.cursnap
	     << " seqnum: " << rhs.seqnum
	     << " prefix: " << rhs.prefix;
}

void VarLenGenerator::get_ranges(const ContDesc &cont, interval_set<uint64_t> &out) {
  RandWrap rand(cont.seqnum);
  uint64_t pos = get_header_length(cont);
  uint64_t limit = get_length(cont);
  out.insert(0, pos);
  bool include = false;
  while (pos < limit) {
    uint64_t segment_length = (rand() % (max_stride_size - min_stride_size)) + min_stride_size;
    assert(segment_length < max_stride_size);
    assert(segment_length >= min_stride_size);
    if (segment_length + pos >= limit) {
      segment_length = limit - pos;
    }
    if (include) {
      out.insert(pos, segment_length);
      include = false;
    } else {
      include = true;
    }
    pos += segment_length;
  }
}

void VarLenGenerator::write_header(const ContDesc &in, bufferlist &output) {
  int data[6];
  data[0] = 0xDEADBEEF;
  data[1] = in.objnum;
  data[2] = in.cursnap;
  data[3] = (int)in.seqnum;
  data[4] = in.prefix.size();
  data[5] = 0xDEADBEEF;
  output.append((char *)data, sizeof(data));
  output.append(in.prefix.c_str(), in.prefix.size());
  output.append((char *)data, sizeof(data[0]));
}

bool VarLenGenerator::read_header(bufferlist::iterator &p, ContDesc &out) {
  try {
    int data[6];
    p.copy(sizeof(data), (char *)data);
    if ((unsigned)data[0] != 0xDEADBEEF || (unsigned)data[5] != 0xDEADBEEF) return false;
    out.objnum = data[1];
    out.cursnap = data[2];
    out.seqnum = (unsigned) data[3];
    int prefix_size = data[4];
    if (prefix_size >= 1000 || prefix_size <= 0) {
      std::cerr << "prefix size is " << prefix_size << std::endl;
      return false;
    }
    char buffer[1000];
    p.copy(prefix_size, buffer);
    buffer[prefix_size] = 0;
    out.prefix = buffer;
    unsigned test;
    p.copy(sizeof(test), (char *)&test);
    if (test != 0xDEADBEEF) return false;
  } catch (ceph::buffer::end_of_buffer &e) {
    std::cerr << "end_of_buffer" << endl;
    return false;
  }
  return true;
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
    limit = *stack.begin();
    stack.pop_front();
    --cur_cont;
  }

  if (cur_cont == obj.layers.end()) {
    return *this;
  }

  interval_set<uint64_t> ranges;
  cont_gen->get_ranges(*cur_cont, ranges);
  while (!ranges.contains(pos)) {
    stack.push_front(limit);
    uint64_t next;
    if (pos >= ranges.range_end()) {
      next = limit;
    } else {
      next = ranges.start_after(pos);
    }
    if (next < limit) {
      limit = next;
    }
    ++cur_cont;
    if (cur_cont == obj.layers.end()) {
      break;
    }

    ranges.clear();
    cont_gen->get_ranges(*cur_cont, ranges);
  }

  if (cur_cont == obj.layers.end()) {
    return *this;
  }

  if (!cont_iters.count(*cur_cont)) {
    cont_iters.insert(pair<ContDesc,ContentsGenerator::iterator>(*cur_cont, 
								 cont_gen->get_iterator(*cur_cont)));
  }
  map<ContDesc,ContentsGenerator::iterator>::iterator j = cont_iters.find(*cur_cont);
  assert(j != cont_iters.end());
  j->second.seek(pos);
  return *this;
}

const ContDesc &ObjectDesc::most_recent() {
  return *layers.begin();
}

void ObjectDesc::update(const ContDesc &next) {
  layers.push_front(next);
  return;
  /*
  interval_set<uint64_t> fall_through;
  fall_through.insert(0, cont_gen->get_length(next));
  for (list<ContDesc>::iterator i = layers.begin();
       i != layers.end();
       ) {
    interval_set<uint64_t> valid;
    cont_gen->get_ranges(*i, valid);
    valid.intersection_of(fall_through);
    if (valid.empty()) {
      layers.erase(i++);
      continue;
    }
    fall_through.subtract(valid);
    ++i;
  }
  */
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
  return true;
}
