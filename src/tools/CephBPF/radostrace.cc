/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Dongdong Tao <dongdong.tao@canonical.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <bpf/libbpf.h>
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <sys/resource.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <dlfcn.h>
#include <link.h>
#include <dirent.h>
#include <ctype.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <fstream>

// C++ compatibility: bpftool generates skeleton code using _Bool (C99 type).
// glibc's stdbool.h defines _Bool as bool in C++ mode, but Clang's stdbool.h
// only does this when __STRICT_ANSI__ is not set. With -std=c++XX (strict mode),
// Clang defines __STRICT_ANSI__, causing _Bool to be undefined.
// This is the same fix used by Linux kernel BPF selftests.
#ifndef _Bool
#define _Bool bool
#endif
#include "radostrace.skel.h"

#include "bpf_ceph_types.h"
#include "dwarf_parser.h"
#include "utils.h"

// Ceph headers for computing struct offsets at runtime
#include "osd/osd_types.h"
#include "include/rados.h"
#include "include/buffer.h"

using namespace std;

typedef map<string, int> func_id_t;

vector<string> probe_units = {"Objecter.cc"};

func_id_t func_id = {
      {"Objecter::_send_op", 0},
      {"Objecter::_finish_op", 20}

};


map<string, int> func_progid = {
      {"Objecter::_send_op", 0},
      {"Objecter::_finish_op", 1}

};


DwarfParser::probes_t rados_probes = {
      {"Objecter::_send_op",
       {{"op", "tid"},
	{"this", "monc", "global_id"},
        {"op", "target", "osd"},
        {"op", "target", "base_oid", "name", "_M_string_length"},
        {"op", "target", "base_oid", "name", "_M_dataplus", "_M_p"},
        {"op", "target", "flags"},
        {"op", "target", "actual_pgid", "pgid", "m_pool"},
        {"op", "target", "actual_pgid", "pgid", "m_seed"},
        {"op", "target", "acting", "_M_impl", "_M_start"},
        {"op", "target", "acting", "_M_impl", "_M_finish"},
	{"op", "ops", "m_holder", "m_start"},
	{"op", "ops", "m_holder", "m_size"}}},

      {"Objecter::_finish_op", 
       {{"op", "tid"},
	{"this", "monc", "global_id"},
	{"op", "target", "osd"}}}
};

volatile sig_atomic_t timeout_occurred = 0;

const char * ceph_osd_op_str(int opc) {
    const char *op_str = NULL;
#define GENERATE_CASE_ENTRY(op, opcode, str)	case CEPH_OSD_OP_##op: op_str=str; break;
    switch (opc) {
    __CEPH_FORALL_OSD_OPS(GENERATE_CASE_ENTRY)
    }
    return op_str;
}

void set_ceph_offsets(struct radostrace_bpf *skel) {
  // Buffer internal offsets - computed from class layout
  // buffer::list layout: buffers_t _buffers, ptr_node* _carriage, unsigned _len, _num
  // buffers_t contains: ptr_hook _root (8 bytes), ptr_hook* _tail (8 bytes) = 16 bytes total
  size_t buffers_t_size = sizeof(ceph::buffer::ptr_hook) + sizeof(ceph::buffer::ptr_hook*);

  // buffer::raw layout (polymorphic class with vtable):
  //   offset 0:  vtable pointer (8 bytes)
  //   offset 8:  bptr_storage (sizeof(ptr_node) = 24 bytes)
  //   offset 32: char* data
  size_t raw_data_offset = sizeof(void*) + sizeof(ceph::buffer::ptr_node);

  // Set BPF global variables before loading
  skel->rodata->CEPH_OSDOP_SIZE = sizeof(OSDOp);
  skel->rodata->CEPH_EXTENT_OFFSET = offsetof(struct ceph_osd_op, extent.offset);
  skel->rodata->CEPH_EXTENT_LENGTH = offsetof(struct ceph_osd_op, extent.length);
  skel->rodata->CEPH_CLS_CLASS = offsetof(struct ceph_osd_op, cls.class_len);
  skel->rodata->CEPH_CLS_METHOD = offsetof(struct ceph_osd_op, cls.method_len);
  skel->rodata->CEPH_BUFFER_CARRIAGE = offsetof(OSDOp, indata) + buffers_t_size;
  skel->rodata->CEPH_BUFFER_RAW = sizeof(ceph::buffer::ptr_hook);
  skel->rodata->CEPH_BUFFER_DATA = raw_data_offset;

  debug_print("Ceph struct offsets set in BPF globals:\n");
  debug_print("  CEPH_OSDOP_SIZE: ", skel->rodata->CEPH_OSDOP_SIZE, "\n");
  debug_print("  CEPH_EXTENT_OFFSET: ", skel->rodata->CEPH_EXTENT_OFFSET, "\n");
  debug_print("  CEPH_EXTENT_LENGTH: ", skel->rodata->CEPH_EXTENT_LENGTH, "\n");
  debug_print("  CEPH_BUFFER_CARRIAGE: ", skel->rodata->CEPH_BUFFER_CARRIAGE,
       " (indata@", offsetof(OSDOp, indata), " + ", buffers_t_size, ")\n");
  debug_print("  CEPH_BUFFER_RAW: ", skel->rodata->CEPH_BUFFER_RAW, "\n");
  debug_print("  CEPH_BUFFER_DATA: ", skel->rodata->CEPH_BUFFER_DATA,
       " (vtable@", sizeof(void*), " + bptr_storage@", sizeof(ceph::buffer::ptr_node), ")\n");
}

void fill_map_hprobes(string mod_path, DwarfParser &dwarfparser, struct bpf_map *hprobes) {
  string mod_basename = get_basename(mod_path);
  auto &func2vf = dwarfparser.mod_func2vf[mod_basename];
  for (auto x : func2vf) {
    string funcname = x.first;
    int key_idx = func_id[funcname];
    for (auto vf : x.second) {
      struct VarField_Kernel vfk;
      vfk.varloc = vf.varloc;
      debug_print("fill_map_hprobes: ",
           "function ", funcname, " var location : register ",
           vfk.varloc.reg, " offset ", vfk.varloc.offset, " stack ",
           vfk.varloc.stack, "\n");
      vfk.size = vf.fields.size();
      for (int i = 0; i < vfk.size; ++i) {
        vfk.fields[i] = vf.fields[i];
      }
      bpf_map__update_elem(hprobes, &key_idx, sizeof(key_idx), &vfk,
                           sizeof(vfk), 0);
      ++key_idx;
    }
  }
}

void signal_handler(int signum){
  clog << "Caught signal " << signum << std::endl;
  if (signum == SIGINT) {
      clog << "process killed" << std::endl;
  }
  exit(signum);
}

void timeout_handler(int signum) {
    if (signum == SIGALRM) {
        timeout_occurred = 1;
    }
}

// Library discovery functions

// Find library path by parsing /proc/<pid>/maps
// This is more reliable than dlopen when targeting a specific process,
// especially for vstart clusters or containerized environments where
// libraries are loaded from non-standard paths.
string find_library_path_from_maps(const string& lib_name, int pid) {
    string maps_path = "/proc/" + to_string(pid) + "/maps";
    ifstream maps_file(maps_path);

    if (!maps_file.is_open()) {
        debug_print("Failed to open ", maps_path, "\n");
        return "";
    }

    string line;
    while (getline(maps_file, line)) {
        // Look for the library name in the mapped file path
        // /proc/pid/maps format: address perms offset dev inode pathname
        if (line.find(lib_name) != string::npos && line.find("(deleted)") == string::npos) {
            // Find the path - it starts after the inode field
            // Example line:
            // 7f1234567000-7f1234568000 r-xp 00000000 08:01 12345 /path/to/lib.so
            size_t path_start = line.find('/');
            if (path_start != string::npos) {
                string path = line.substr(path_start);
                // Remove trailing whitespace/newline
                size_t end_pos = path.find_last_not_of(" \n\r\t");
                if (end_pos != string::npos) {
                    path = path.substr(0, end_pos + 1);
                }
                debug_print("Found library ", lib_name, " from maps at: ", path, "\n");
                return path;
            }
        }
    }

    debug_print("Library ", lib_name, " not found in ", maps_path, "\n");
    return "";
}

string find_library_path(const string& lib_name, int pid) {
    string result;

    // If PID is specified, first try to find library from /proc/<pid>/maps
    if (pid != -1) {
        result = find_library_path_from_maps(lib_name, pid);
        if (!result.empty()) {
            return result;
        }
        debug_print("Could not find ", lib_name, " in process maps, falling back to other methods\n");
    }

    // Try to find the library using dlopen
    void* handle = dlopen(lib_name.c_str(), RTLD_LAZY | RTLD_NOLOAD);
    if (!handle) {
        handle = dlopen(lib_name.c_str(), RTLD_LAZY);
    }

    if (handle) {
        struct link_map* link_map;
        if (dlinfo(handle, RTLD_DI_LINKMAP, &link_map) == 0 && link_map) {
            string path = link_map->l_name;
            dlclose(handle);
            if (!path.empty() && path != lib_name) {
                debug_print("Found library ", lib_name, " via dlopen at: ", path, "\n");
                return path;
            }
        }
        dlclose(handle);
    }

    // Fallback: search in common library directories
    vector<string> search_dirs = {
        "./lib", "/lib", "/lib64", "/lib64/ceph",
        "/usr/lib", "/usr/lib64", "/usr/lib64/ceph",
        "/lib/x86_64-linux-gnu", "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/x86_64-linux-gnu/ceph", "/usr/local/lib"
    };

    vector<string> possible_names;
    if (lib_name.find(".so") == string::npos) {
        possible_names.push_back("lib" + lib_name + ".so");
        possible_names.push_back("lib" + lib_name + ".so.1");
        possible_names.push_back("lib" + lib_name + ".so.2");
    } else {
        possible_names.push_back(lib_name);
    }

    for (const auto& dir : search_dirs) {
        for (const auto& name : possible_names) {
            string full_path = dir + "/" + name;
            if (access(full_path.c_str(), F_OK) == 0) {
                debug_print("Found library ", lib_name, " at: ", full_path, "\n");
                return full_path;
            }
        }
    }

    return "";
}

bool check_process_library_deleted(int pid, const string& lib_name) {
    string maps_path = "/proc/" + to_string(pid) + "/maps";
    ifstream maps_file(maps_path);

    if (!maps_file.is_open()) {
        return false;
    }

    string line;
    bool found_deleted = false;

    while (getline(maps_file, line)) {
        if (line.find(lib_name) != string::npos && line.find("(deleted)") != string::npos) {
            found_deleted = true;
            break;
        }
    }

    maps_file.close();
    return found_deleted;
}

bool check_library_deleted(int process_id, const string& lib_name) {
    if (process_id > 0) {
        return check_process_library_deleted(process_id, lib_name);
    } else {
        DIR* proc_dir = opendir("/proc");
        if (!proc_dir) {
            cerr << "Error: Could not open /proc directory" << std::endl;
            return true;
        }

        struct dirent* entry;
        while ((entry = readdir(proc_dir)) != NULL) {
            if (entry->d_type == DT_DIR && isdigit(entry->d_name[0])) {
                int pid = stoi(entry->d_name);
                string maps_path = "/proc/" + to_string(pid) + "/maps";
                ifstream maps_file(maps_path);

                if (maps_file.is_open()) {
                    string line;
                    bool has_lib = false;

                    while (getline(maps_file, line)) {
                        if (line.find(lib_name) != string::npos) {
                            has_lib = true;
                            break;
                        }
                    }

                    if (has_lib && check_process_library_deleted(pid, lib_name)) {
                        closedir(proc_dir);
                        return true;
                    }
                    maps_file.close();
                }
            }
        }
        closedir(proc_dir);
    }
    return false;
}

static int libbpf_print_fn(enum libbpf_print_level level, const char *format,
                           va_list args) {
  if (level == LIBBPF_DEBUG) return 0;
  return vfprintf(stderr, format, args);
}

int attach_uprobe(struct radostrace_bpf *skel,
	           DwarfParser &dp,
	           string path,
		   string funcname,
		   int process_id = -1,
		   int v = 0) {

  string pid_path = path;
  if (process_id != -1) {
    pid_path = "/proc/" + to_string(process_id) + "/root" + path;
  }

  string path_basename = get_basename(path);
  auto &func2pc = dp.mod_func2pc[path_basename];
  size_t func_addr = func2pc[funcname];
  if (func_addr == 0) {
    debug_print("Path ", path, " function ", funcname, " func_addr is zero, not attaching\n");
    return 0;
  }
  if (v > 0)
      funcname = funcname + "_v" + to_string(v);
  int prog_id = func_progid[funcname];
  struct bpf_link *ulink = bpf_program__attach_uprobe(
      *skel->skeleton->progs[prog_id].prog,
      false /* not uretprobe */,
      process_id,  // Use the specified process ID
      pid_path.c_str(), func_addr);
  if (!ulink) {
    cerr << "Failed to attach uprobe to " << funcname << std::endl;
    return -errno;
  }

  if (process_id > 0) {
    debug_print("uprobe ", funcname, " attached to process ", process_id, "\n");
  } else {
    debug_print("uprobe ", funcname, " attached to all processes\n");
  }
  return 0;
}

int attach_retuprobe(struct radostrace_bpf *skel,
	           DwarfParser &dp,
	           string path,
		   string funcname,
		   int process_id = -1,
		   int v = 0) {

  string pid_path = path;
  if (process_id != -1) {
    pid_path = "/proc/" + to_string(process_id) + "/root" + path;
  }

  string path_basename = get_basename(path);
  auto &func2pc = dp.mod_func2pc[path_basename];
  size_t func_addr = func2pc[funcname];
  if (v > 0)
      funcname = funcname + "_v" + to_string(v);
  int prog_id = func_progid[funcname];
  struct bpf_link *ulink = bpf_program__attach_uprobe(
      *skel->skeleton->progs[prog_id].prog,
      true /* uretprobe */,
      process_id,  // Use the specified process ID
      pid_path.c_str(), func_addr);
  if (!ulink) {
    cerr << "Failed to attach uretprobe to " << funcname << std::endl;
    return -errno;
  }

  if (process_id > 0) {
    debug_print("uretprobe ", funcname, " attached to process ", process_id, "\n");
  } else {
    debug_print("uretprobe ", funcname, " attached to all processes\n");
  }
  return 0;
}

static int handle_event(void *ctx, void *data, size_t size) {
    (void)ctx;
    (void)size;
    struct client_op_v * op_v = (struct client_op_v *)data;
    stringstream ss;
    ss << hex << op_v->m_seed;
    string pgid(ss.str());

    // Define field widths based on actual data
    struct FieldWidths {
        int pid = 8;
        int client = 8; 
        int tid = 8;
        int pool = 6;
        int pg = 4;
        int acting = 18;
        int wr = 3;
        int size = 7;
        int latency = 8;
    };
    
    static FieldWidths widths;
    static bool firsttime = true;
    
    // Compile acting OSD list
    stringstream acting_osd_list;
    acting_osd_list << "[";
    {
        bool first = true;
        for (int i = 0; i < MAX_ACTING_SIZE; ++i) {
            if (op_v->acting[i] < 0) break;
            if (!first) acting_osd_list << ",";
            acting_osd_list << op_v->acting[i];
            first = false;
        }
        acting_osd_list << "]";
    }
    string acting_str = acting_osd_list.str();

    // Compile Ops list
    stringstream ops_list;
    bool print_offset_length = false;
    ops_list << "[";
    for (__u32 i = 0; i < op_v->ops_size; ++i) {
        if (i) ops_list << " ";
        if (ceph_osd_op_uses_extent(op_v->ops[i])) {
            ops_list << ceph_osd_op_str(op_v->ops[i]);
            print_offset_length = true;
        } else if (CEPH_OSD_OP_CALL == op_v->ops[i]) {
            ops_list << "call(" << op_v->cls_ops[i].cls_name
                   << "." << op_v->cls_ops[i].method_name << ")";
        } else {
            ops_list << ceph_osd_op_str(op_v->ops[i]);
        }
    }
    ops_list << "]";
    string ops_str = ops_list.str();

    long long latency_us = (op_v->finish_stamp - op_v->sent_stamp) / 1000;
    string wr_str = (op_v->rw & CEPH_OSD_FLAG_WRITE) ? "W" : "R";

    // Standard output
    if (firsttime) {
        // Calculate field widths based on actual data from first event
        widths.pid = max(8, (int)to_string(op_v->pid).length() + 1);
        widths.client = max(8, (int)to_string(op_v->cid).length() + 1);
        widths.tid = max(8, (int)to_string(op_v->tid).length() + 1);
        widths.pool = max(6, (int)to_string(op_v->m_pool).length() + 1);
        widths.pg = max(4, (int)pgid.length() + 1);
        widths.acting = max(15, (int)acting_str.length() + 1);
        widths.wr = 4;
        widths.size = max(9, (int)to_string(op_v->length).length() + 1);
        widths.latency = max(9, (int)to_string(latency_us).length() + 1);
        
        // Print header using calculated widths
        printf("%*s%*s%*s%*s%*s%*s%*s%*s%*s%s\n", 
               widths.pid, "pid",
               widths.client, "client", 
               widths.tid, "tid",
               widths.pool, "pool", 
               widths.pg, "pg",
               widths.acting, "acting",
               widths.wr, "WR",
               widths.size, "size",
               widths.latency, "latency",
               "     object[ops]");
        
        firsttime = false;
    }

    // Format output using calculated widths
    printf("%*d%*lld%*lld%*lld%*s", 
           widths.pid, op_v->pid,
           widths.client, op_v->cid, 
           widths.tid, op_v->tid,
           widths.pool, op_v->m_pool, 
           widths.pg, pgid.c_str()); 

    printf("%*s", widths.acting, acting_str.c_str());

    printf("%*s%*lld%*lld",
           widths.wr, wr_str.c_str(),
           widths.size, op_v->length,
           widths.latency, latency_us);

    // Object name and operations (no fixed width needed)
    printf("     %s ", op_v->object_name);
    printf("%s", ops_str.c_str());

    if (print_offset_length) {
        printf("[%lld, %lld]\n", op_v->offset, op_v->length);
    } else {
        printf("\n");
    }

    return 0;
}


int main(int argc, char **argv) {
  signal(SIGINT, signal_handler); 

  /* Default to unlimited execution time */
  int timeout = -1;
  int process_id = -1;  // Default to -1 (all processes)

  /* Parse arguments */
  for (int i = 1; i < argc; ++i) {
      string arg = argv[i];
      if ((arg == "-t" || arg == "--timeout") && i + 1 < argc) {
          try {
              timeout = stoi(argv[++i]);
              if (timeout <= 0) throw invalid_argument("Negative timeout");
          } catch (...) {
              cerr << "Invalid timeout value. Must be a positive integer.\n";
              return 1;
          }
      } else if (arg == "-p" || arg == "--pid") {
          if (i + 1 < argc) {
              try {
                  process_id = stoi(argv[++i]);
                  if (process_id <= 0) throw invalid_argument("Invalid PID");
              } catch (...) {
                  cerr << "Invalid process ID. Must be a positive integer.\n";
                  return 1;
              }
          } else {
              cerr << "Error: -p/--pid requires a process ID argument\n";
              return 1;
          }
      } else if (arg == "-d" || arg == "--debug") {
          get_debug_mode() = true;
      } else if (arg == "-h" || arg == "--help") {
          cout << "Usage: " << argv[0] << " [-t <timeout seconds>] [-p <pid>] [-d]\n";
          cout << "  -t, --timeout <seconds>    Set execution timeout in seconds\n";
          cout << "  -p, --pid <pid>            Attach uprobes only to the specified process ID\n";
          cout << "  -d, --debug                Enable debug output (userspace and BPF)\n";
          cout << "                             BPF debug: cat /sys/kernel/debug/tracing/trace_pipe\n";
          cout << "  -h, --help                 Show this help message\n";
          return 0;
      }
  }

  // Validate process_id if specified
  if (process_id != -1) {
    string proc_path = "/proc/" + to_string(process_id);
    if (access(proc_path.c_str(), F_OK) != 0) {
      cerr << "Error: Process ID " << process_id << " does not exist" << std::endl;
      return 1;
    }
  }

  struct radostrace_bpf *skel;
  int ret = 0;
  struct ring_buffer *rb;

  DwarfParser dwarfparser(rados_probes, probe_units);

  // Use the new function to find library paths dynamically
  string librbd_path = find_library_path("librbd.so.1", process_id);
  string librados_path = find_library_path("librados.so.2", process_id);
  string libceph_common_path = find_library_path("libceph-common.so.2", process_id);

  if(librbd_path.empty() || librados_path.empty() || libceph_common_path.empty()) {
    cerr << "Error: Could not find one or more required Ceph libraries:" << std::endl;
    if (librbd_path.empty()) cerr << "  - librbd.so.1 not found" << std::endl;
    if (librados_path.empty()) cerr << "  - librados.so.2 not found" << std::endl;
    if (libceph_common_path.empty()) cerr << "  - libceph-common.so.2 not found" << std::endl;
    return 1;
  } else {
    debug_print("Libraries to be traced: ", librbd_path, ", ", librados_path, ", ", libceph_common_path, "\n");
  }

  if (check_library_deleted(process_id, "librados")) {
     cerr << "Error: librados library mismatch detected!" << std::endl;
     cerr << "The ceph package has been upgraded on disk, but one or more processes are still using the old version in memory." << std::endl;
     cerr << "Please restart the affected processes to use the new version." << std::endl;
     return 1;
   }

  debug_print("Start to parse dwarf info\n");
  dwarfparser.add_module(librbd_path);
  dwarfparser.add_module(librados_path);
  dwarfparser.add_module(libceph_common_path);
  dwarfparser.parse();

  libbpf_set_strict_mode(LIBBPF_STRICT_ALL);

  /* Set up libbpf errors and debug info callback */
  libbpf_set_print(libbpf_print_fn);

  /* Load and verify BPF application */
  debug_print("Start to load uprobe\n");

  /* Open BPF skeleton first (before loading) */
  skel = radostrace_bpf__open();
  if (!skel) {
    cerr << "Failed to open BPF skeleton" << std::endl;
    return 1;
  }

  /* Set BPF global variables (rodata) - must be done after open but before load */
  set_ceph_offsets(skel);
  skel->rodata->DEBUG_OUTPUT = get_debug_mode();
  if (get_debug_mode()) {
    debug_print("BPF debug output enabled (view with: cat /sys/kernel/debug/tracing/trace_pipe)\n");
  }

  /* Now load the BPF program with the configured globals */
  ret = radostrace_bpf__load(skel);
  if (ret) {
    cerr << "Failed to load BPF skeleton: " << ret << std::endl;
    radostrace_bpf__destroy(skel);
    return 1;
  }

  // Fill BPF maps with data from userspace
  fill_map_hprobes(libceph_common_path, dwarfparser, skel->maps.hprobes);

  debug_print("BPF prog loaded\n");

  attach_uprobe(skel, dwarfparser, librados_path, "Objecter::_send_op", process_id);
  attach_uprobe(skel, dwarfparser, librbd_path, "Objecter::_send_op", process_id);
  attach_uprobe(skel, dwarfparser, libceph_common_path, "Objecter::_send_op", process_id);
  attach_uprobe(skel, dwarfparser, librados_path, "Objecter::_finish_op", process_id);
  attach_uprobe(skel, dwarfparser, librbd_path, "Objecter::_finish_op", process_id);
  attach_uprobe(skel, dwarfparser, libceph_common_path, "Objecter::_finish_op", process_id);

  debug_print("New a ring buffer\n");

  rb = ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, NULL, NULL);
  if (!rb) {
    cerr << "failed to setup ring_buffer" << std::endl;
    goto cleanup;
  }

  debug_print("Started polling from ring buffer\n");

  /* Set up timeout now, after all initialization is complete */
  if (timeout > 0) {
      signal(SIGALRM, timeout_handler);
      alarm(timeout);
      cout << "Execution timeout set to " << timeout << " seconds.\n";
  } else {
      cout << "No execution timeout set (unlimited).\n";
  }

  while ((!timeout_occurred || timeout == -1) && (ret = ring_buffer__poll(rb, 1000)) >= 0) {
      // Continue polling while timeout hasn't occurred or if unlimited execution time
  }

  if (timeout_occurred) {
      cerr << "Timeout occurred. Exiting." << std::endl;
  }

cleanup:
  debug_print("Clean up the eBPF program\n");
  ring_buffer__free(rb);
  radostrace_bpf__destroy(skel);
  return timeout_occurred ? -1 : -errno;
}

