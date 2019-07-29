// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap allocator replay tool.
 * Author: Igor Fedotov, ifedotov@suse.com
 */
#include <iostream>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/Cycles.h"
#include "global/global_init.h"
#include "os/bluestore/Allocator.h"


void usage(const string &name) {
  cerr << "Usage: " << name << " <log_to_replay> "
       << std::endl;
}

int replay_and_check_for_duplicate(char* fname)
{
  unique_ptr<Allocator> alloc;

  FILE* f = fopen(fname, "r");
  if (!f) {
    std::cerr << "error: unable to open " << fname << std::endl;
    return -1;
  }

  PExtentVector tmp;
  bool init_done = false;
  char s[4096];
  char* sp, *token;
  while (true) {
    if (fgets(s, sizeof(s), f) == nullptr) {
      break;
    }
    sp = strstr(s, "init_add_free");
    if (sp) {
      //2019-05-30 03:23:46.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_add_free 0x100000~680000000
      // or
      //2019-05-30 03:23:46.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_add_free done
      if (!init_done) { 
	std::cerr << "error: no allocator init before: " << s << std::endl;
	return -1;
      }
      if (strstr(sp, "done") != nullptr) {
	continue;
      }
      uint64_t offs, len;
      strtok(sp, " ~");
      token = strtok(nullptr, " ~");
      ceph_assert(token);
      offs = strtoul(token, nullptr, 16);
      token = strtok(nullptr, " ~");
      ceph_assert(token);
      len = strtoul(token, nullptr, 16);
      if (len == 0) {
	std::cerr << "error: init_add_free: " << s << std::endl;
	return -1;
      }
      alloc->init_add_free(offs, len);
      continue;
    }
    sp = strstr(s, "init_rm_free");
    if (sp) {
      //2019-05-30 03:23:46.912 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_rm_free 0x100000~680000000
      // or 
      // 2019-05-30 03:23:46.916 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_rm_free done
      if (!init_done) { 
	std::cerr << "error: no allocator init before: " << s << std::endl;
	return -1;
      }
      if (strstr(sp, "done") != nullptr) {
	continue;
      }
      uint64_t offs, len;
      strtok(sp, " ~");
      token = strtok(nullptr, " ~");
      ceph_assert(token);
      offs = strtoul(token, nullptr, 16);
      token = strtok(nullptr, " ~");
      ceph_assert(token);
      len = strtoul(token, nullptr, 16);
      if (len == 0) {
	std::cerr << "error: init_rm_free: " << s << std::endl;
	return -1;
      }
      alloc->init_rm_free(offs, len);
      continue;
    }
    sp = strstr(s, "allocate");
    if (sp) {
      //2019-05-30 03:23:48.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 allocate 0x80000000/100000,0,0
      // and need to bypass  
      // 2019-05-30 03:23:48.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 allocate 0x69d400000~200000/100000,0,0
      if (!init_done) { 
	std::cerr << "error: no allocator init before: " << s << std::endl;
	return -1;
      }
      // Very simple and stupid check to bypass actual allocations
      if (strstr(sp, "~") != nullptr) {
	continue;
      }
      uint64_t want, alloc_unit;
      strtok(sp, " /");
      token = strtok(nullptr, " /");
      ceph_assert(token);
      want = strtoul(token, nullptr, 16);
      token = strtok(nullptr, " ~");
      ceph_assert(token);
      alloc_unit = strtoul(token, nullptr, 16);
      if (want == 0 || alloc_unit == 0) {
	std::cerr << "error: allocate: " << s << std::endl;
	return -1;
      }
      auto allocated = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
      std::cout << "allocated TOTAL: " << allocated << std::endl;
      interval_set<uint64_t> intervals;
      for (auto& e : tmp) {
	if (intervals.intersects(e.offset, e.length)) {
  	  std::cerr << "error: duplicate extent: " << std::hex
		    << e.offset << "~" << e.length
	            << " dumping all allocations:" << std::dec << std::endl;
	  for (auto& ee : tmp) {
	    std::cerr <<"dump: extent: " << std::hex
	              << ee.offset << "~" << ee.length
		      << std::dec << std::endl;
	  }
	  std::cerr <<"dump completed." << std::endl;
	  return -1;
	} else {
	  intervals.insert(e.offset, e.length);
	}
      }
      continue;
    }

    sp = strstr(s, "BitmapAllocator");
    if (sp) {
      // 2019-05-30 03:23:43.460 7f889a5edf00 10 fbmap_alloc 0x5642ed36e900 BitmapAllocator 0x15940000000/100000
      if (init_done) { 
	std::cerr << "error: duplicate init: " << s << std::endl;
	return -1;
      }
      uint64_t total, alloc_unit;
      strtok(sp, " /");
      token = strtok(nullptr, " /");
      ceph_assert(token);
      total = strtoul(token, nullptr, 16);
      token = strtok(nullptr, " /");
      ceph_assert(token);
      alloc_unit = strtoul(token, nullptr, 16);
      if (total == 0 || alloc_unit == 0) {
	std::cerr << "error: invalid init: " << s << std::endl;
      return -1;
      }
      alloc.reset(Allocator::create(g_ceph_context, string("bitmap"), total,
				    alloc_unit));

      init_done = true;
      continue;
    }
  }
  fclose(f);
  return 0;
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);

  if (argc < 2) {
    usage(argv[0]);
    return 1;
  }

  return replay_and_check_for_duplicate(argv[1]);
}
