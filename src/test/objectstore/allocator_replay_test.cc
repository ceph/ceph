// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Allocator replay tool.
 * Author: Igor Fedotov, ifedotov@suse.com
 */
#include <iostream>

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/Cycles.h"
#include "common/errno.h"
#include "common/ceph_json.h"
#include "common/admin_socket.h"
#include "global/global_init.h"
#include "os/bluestore/Allocator.h"

using namespace std;

void usage(const string &name) {
  cerr << "Usage: " << name << " <log_to_replay> <raw_duplicate|free_dump|try_alloc count want alloc_unit|replay_alloc alloc_list_file>" << std::endl;
}

void usage_replay_alloc(const string &name) {
  cerr << "Detailed replay_alloc usage: " << name << " <allocator_dump_JSON> replay_alloc <alloc_list_file> [number of replays]" << std::endl;
  cerr << "The number of replays defaults to 1." << std::endl;
  cerr << "The \"alloc_list_file\" parameter should be a file with allocation requests, one per line." << std::endl;
  cerr << "Allocation request format (space separated, optional parameters are 0 if not given): want unit [max] [hint]" << std::endl;
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
  interval_set<uint64_t> owned_by_app;
  while (true) {
    if (fgets(s, sizeof(s), f) == nullptr) {
      break;
    }
    sp = strstr(s, "init_add_free");
    if (!sp) {
      sp = strstr(s, "release");
    }
    if (sp) {
      //2019-05-30 03:23:46.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_add_free 0x100000~680000000
      // or
      //2019-05-30 03:23:46.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_add_free done
      // or
      // 2019 - 10 - 08T16:19 : 32.257 + 0300 7f5679f3fe80 10 fbmap_alloc 0x564fab96f100 release 0x450000~10000
      // or
      // 2019 - 10 - 08T16 : 19 : 32.257 + 0300 7f5679f3fe80 10 fbmap_alloc 0x564fab96f100 release done
      if (strstr(sp, "done") != nullptr) {
        continue;
      }
      std::cout << s << std::endl;
      if (!init_done) {
	std::cerr << "error: no allocator init before: " << s << std::endl;
	return -1;
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
	std::cerr << "error: " << sp <<": " << s << std::endl;
	return -1;
      }
      if (!owned_by_app.contains(offs, len)) {
        std::cerr << "error: unexpected return to allocator, not owned by app: "
		  << s << std::endl;
	return -1;
      }
      owned_by_app.erase(offs, len);
      if (strstr(sp, "init_add_free") != nullptr) {
        alloc->init_add_free(offs, len);
      } else {
        PExtentVector release_set;
        release_set.emplace_back(offs, len);
        alloc->release(release_set);
      }
      continue;
    }
    sp = strstr(s, "init_rm_free");
    if (sp) {
      //2019-05-30 03:23:46.912 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_rm_free 0x100000~680000000
      // or 
      // 2019-05-30 03:23:46.916 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 init_rm_free done

      if (strstr(sp, "done") != nullptr) {
        continue;
      }
      std::cout << s << std::endl;
      if (!init_done) {
	std::cerr << "error: no allocator init before: " << s << std::endl;
	return -1;
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
	std::cerr << "error: " << sp <<": " << s << std::endl;
	return -1;
      }
      alloc->init_rm_free(offs, len);

      if (owned_by_app.intersects(offs, len)) {
        std::cerr
         << "error: unexpected takeover from allocator, already owned by app: "
	 << s << std::endl;
	return -1;
      } else {
	owned_by_app.insert(offs, len);
      }

      continue;
    }
    sp = strstr(s, "allocate");
    if (sp) {
      //2019-05-30 03:23:48.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 allocate 0x80000000/100000,0,0
      // and need to bypass  
      // 2019-05-30 03:23:48.780 7f889a5edf00 10 fbmap_alloc 0x5642ed370600 allocate 0x69d400000~200000/100000,0,0

      // Very simple and stupid check to bypass actual allocations
      if (strstr(sp, "~") != nullptr) {
        continue;
      }

      std::cout << s << std::endl;
      if (!init_done) {
	std::cerr << "error: no allocator init before: " << s << std::endl;
	return -1;
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
      tmp.clear();
      auto allocated = alloc->allocate(want, alloc_unit, 0, 0, &tmp);
      std::cout << "allocated TOTAL: " << allocated << std::endl;
      for (auto& ee : tmp) {
        std::cerr << "dump extent: " << std::hex
          << ee.offset << "~" << ee.length
          << std::dec << std::endl;
      }
      std::cerr << "dump completed." << std::endl;
      for (auto& e : tmp) {
	if (owned_by_app.intersects(e.offset, e.length)) {
	  std::cerr << "error: unexpected allocated extent: " << std::hex
		    << e.offset << "~" << e.length
	            << " dumping all allocations:" << std::dec << std::endl;
	  for (auto& ee : tmp) {
	    std::cerr <<"dump extent: " << std::hex
	              << ee.offset << "~" << ee.length
		      << std::dec << std::endl;
	  }
	  std::cerr <<"dump completed." << std::endl;
	  return -1;
	} else {
	  owned_by_app.insert(e.offset, e.length);
	}
      }
      continue;
    }

    string alloc_type = "bitmap";
    sp = strstr(s, "BitmapAllocator");
    if (!sp) {
      alloc_type = "avl";
      sp = strstr(s, "AvlAllocator");
    }
    if (!sp) {
      alloc_type = "hybrid";
      sp = strstr(s, "HybridAllocator");
    }
    if (!sp) {
      alloc_type = "stupid";
      sp = strstr(s, "StupidAllocator");
    }
    if (sp) {
      // 2019-05-30 03:23:43.460 7f889a5edf00 10 fbmap_alloc 0x5642ed36e900 BitmapAllocator 0x15940000000/100000
      std::cout << s << std::endl;
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
      alloc.reset(Allocator::create(g_ceph_context, alloc_type, total,
				    alloc_unit));
      owned_by_app.insert(0, total);

      init_done = true;
      continue;
    }
  }
  fclose(f);
  return 0;
}

/*
* This replays allocator dump (in JSON) reported by 
  "ceph daemon <osd> bluestore allocator dump <name>"
  command and applies custom method to it
*/
int replay_free_dump_and_apply(char* fname,
    std::function<int (Allocator*, const string& aname)> fn)
{
  string alloc_type;
  string alloc_name;
  uint64_t capacity = 0;
  uint64_t alloc_unit = 0;

  JSONParser p;
  std::cout << "parsing..." << std::endl;
  bool b = p.parse(fname);
  if (!b) {
    std::cerr << "Failed to parse json: " << fname << std::endl;
    return -1;
  }

  JSONObj::data_val v;
  ceph_assert(p.is_object());

  auto *o = p.find_obj("alloc_type");
  ceph_assert(o);
  alloc_type = o->get_data_val().str;

  o = p.find_obj("alloc_name");
  ceph_assert(o);
  alloc_name = o->get_data_val().str;

  o = p.find_obj("capacity");
  ceph_assert(o);
  decode_json_obj(capacity, o);
  o = p.find_obj("alloc_unit");
  ceph_assert(o);
  decode_json_obj(alloc_unit, o);

  o = p.find_obj("extents");
  ceph_assert(o);
  ceph_assert(o->is_array());
  std::cout << "parsing completed!" << std::endl;

  unique_ptr<Allocator> alloc;
  alloc.reset(Allocator::create(g_ceph_context, alloc_type,
				capacity, alloc_unit, 0, 0, alloc_name));

  auto it = o->find_first();
  while (!it.end()) {
    auto *item_obj = *it;
    uint64_t offset = 0;
    uint64_t length = 0;
    string offset_str, length_str;

    bool b = JSONDecoder::decode_json("offset", offset_str, item_obj);
    ceph_assert(b);
    b = JSONDecoder::decode_json("length", length_str, item_obj);
    ceph_assert(b);

    char* p;
    offset = strtol(offset_str.c_str(), &p, 16);
    length = strtol(length_str.c_str(), &p, 16);

    // intentionally skip/trim entries that are above the capacity,
    // just to be able to "shrink" allocator by editing that field
    if (offset < capacity) {
      if (offset + length > capacity) {
        length = offset + length - capacity;
      }
      alloc->init_add_free(offset, length);
    }

    ++it;
  }

  int r = fn(alloc.get(), alloc_name);

  return r;
}

void dump_alloc(Allocator* alloc, const string& aname)
{
  AdminSocket* admin_socket = g_ceph_context->get_admin_socket();
  ceph_assert(admin_socket);

  ceph::bufferlist in, out;
  ostringstream err;

  string cmd = "{\"prefix\": \"bluestore allocator dump " + aname + "\"}";
  auto r = admin_socket->execute_command(
    { cmd },
    in, err, &out);
  if (r != 0) {
    cerr << "failure querying: " << cpp_strerror(r) << std::endl;
  }
  else {
    std::cout << std::string(out.c_str(), out.length()) << std::endl;
  }
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);

  if (argc < 3) {
    usage(argv[0]);
    return 1;
  }
  if (strcmp(argv[2], "raw_duplicate") == 0) {
    return replay_and_check_for_duplicate(argv[1]);
  } else if (strcmp(argv[2], "free_dump") == 0) {
    return replay_free_dump_and_apply(argv[1],
      [&](Allocator* a, const string& aname) {
        ceph_assert(a);
        std::cout << "Fragmentation:" << a->get_fragmentation()
                  << std::endl;
        std::cout << "Fragmentation score:" << a->get_fragmentation_score()
                  << std::endl;
        std::cout << "Free:" << std::hex << a->get_free() << std::dec
                  << std::endl;
        {
        // stub to implement various testing stuff on properly initialized allocator
        // e.g. one can dump allocator back via dump_alloc(a, aname);
        }
        return 0;
      });
  } else if (strcmp(argv[2], "try_alloc") == 0) {
    if (argc < 6) {
      std::cerr << "Error: insufficient arguments for \"try_alloc\" operation."
                << std::endl;
      usage(argv[0]);
      return 1;
    }
    auto count = strtoul(argv[3], nullptr, 10);
    auto want = strtoul(argv[4], nullptr, 10);
    auto alloc_unit = strtoul(argv[5], nullptr, 10);

    return replay_free_dump_and_apply(argv[1],
      [&](Allocator* a, const string& aname) {
        ceph_assert(a);
        std::cout << "Fragmentation:" << a->get_fragmentation()
                  << std::endl;
        std::cout << "Fragmentation score:" << a->get_fragmentation_score()
                  << std::endl;
        std::cout << "Free:" << std::hex << a->get_free() << std::dec
                  << std::endl;
        {
          PExtentVector extents;
          for(size_t i = 0; i < count; i++) {
              extents.clear();
              auto r = a->allocate(want, alloc_unit, 0, &extents);
              if (r < 0) {
                  std::cerr << "Error: allocation failure at step:" << i + 1
                            << ", ret = " << r << std::endl;
              return -1;
            }
	  }
        }
        std::cout << "Successfully allocated: " << count << " * " << want
                  << ", unit:" << alloc_unit << std::endl;
        return 0;
      });
  } else if (strcmp(argv[2], "replay_alloc") == 0) {
    if (argc < 4) {
      std::cerr << "Error: insufficient arguments for \"replay_alloc\" option."
                << std::endl;
      usage_replay_alloc(argv[0]);
      return 1;
    }
    return replay_free_dump_and_apply(argv[1],
      [&](Allocator *a, const string &aname) {
        ceph_assert(a);
        std::cout << "Fragmentation:" << a->get_fragmentation()
                  << std::endl;
        std::cout << "Fragmentation score:" << a->get_fragmentation_score()
                  << std::endl;
        std::cout << "Free:" << std::hex << a->get_free() << std::dec
                  << std::endl;
        {
          /* replay a set of allocation requests */
          char s[4096];

          FILE *f_alloc_list = fopen(argv[3], "r");
          if (!f_alloc_list) {
            std::cerr << "error: unable to open " << argv[3] << std::endl;
            return -1;
          }

          /* Replay user specified number of times to simulate extended activity
           * Defaults to 1 replay.
           */
          auto replay_count = 1;
          if (argc == 5) {
            replay_count = atoi(argv[4]);
          }

          for (auto i = 0; i < replay_count; ++i) {
            while (fgets(s, sizeof(s), f_alloc_list) != nullptr) {
              /* parse allocation request */
              uint64_t want = 0, unit = 0, max = 0, hint = 0;

              if (std::sscanf(s, "%ji %ji %ji %ji", &want, &unit, &max, &hint) < 2)
              {
                cerr << "Error: malformed allocation request:" << std::endl;
                cerr << s << std::endl;
                /* do not attempt to allocate a malformed request */
                continue;
              }

              /* timestamp for allocation start */
              auto t0 = ceph::mono_clock::now();

              /* allocate */
              PExtentVector extents;
              auto r = a->allocate(want, unit, max, hint, &extents);
              if (r < 0) {
                /* blind replays of allocations may run out of space, provide info for easy confirmation */
                std::cerr << "Error: allocation failure code: " << r
                          << " requested want/unit/max/hint (hex): " << std::hex
                          << want << "/" << unit << "/" << max << "/" << hint
                          << std::dec << std::endl;
                std::cerr << "Fragmentation:" << a->get_fragmentation()
                          << std::endl;
                std::cerr << "Fragmentation score:" << a->get_fragmentation_score()
                          << std::endl;
                std::cerr << "Free:" << std::hex << a->get_free() << std::dec
                          << std::endl;
                /* return 0 if the allocator ran out of space */
                if (r == -ENOSPC) {
                  return 0;
                }
                return -1;
              }

              /* Outputs the allocation's duration in nanoseconds and the allocation request parameters */
              std::cout << "Duration (ns): " << (ceph::mono_clock::now() - t0).count()
                        << " want/unit/max/hint (hex): " << std::hex
                        << want << "/" << unit << "/" << max << "/" << hint
                        << std::dec << std::endl;

              /* Do not release. */
              //alloc->release(extents);
              extents.clear();
            }
            fseek(f_alloc_list, 0, SEEK_SET);
          }
          fclose(f_alloc_list);
          std::cout << "Fragmentation:" << a->get_fragmentation()
                    << std::endl;
          std::cout << "Fragmentation score:" << a->get_fragmentation_score()
                    << std::endl;
          std::cout << "Free:" << std::hex << a->get_free() << std::dec
                    << std::endl;
        }
        return 0;
    });
  }
}
