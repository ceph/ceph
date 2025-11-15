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

#include <errno.h>
#include <getopt.h>
#include <linux/types.h>
#include <stdio.h>
#include <sys/resource.h>
#include <time.h>

#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include <queue>
#include <fstream>

extern "C" {
#include <dwarf.h>
#include <elf.h>
#include <elfutils/libdw.h>
#include <elfutils/libdwfl.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
}
#include "bpf_ceph_types.h"
#include "dwarf_parser.h"
#include "utils.h"

using namespace std;

bool DwarfParser::die_has_loclist(Dwarf_Die *begin_die) const {
  Dwarf_Die die;
  Dwarf_Attribute loc;

  if (dwarf_child(begin_die, &die) != 0) return false;

  do {
    switch (dwarf_tag(&die)) {
      case DW_TAG_formal_parameter:
      case DW_TAG_variable:
        if (dwarf_attr_integrate(&die, DW_AT_location, &loc) &&
            dwarf_whatform(&loc) == DW_FORM_sec_offset)
          return true;
        break;
      default:
        if (dwarf_haschildren(&die))
          if (die_has_loclist(&die)) return true;
        break;
    }
  } while (dwarf_siblingof(&die, &die) == 0);

  return false;
}

bool DwarfParser::has_loclist() const {
  assert(cur_cu);
  return die_has_loclist(cur_cu);
}

bool DwarfParser::func_entrypc(Dwarf_Die *func, Dwarf_Addr *addr) {
  assert(func);

  *addr = 0;

  if (dwarf_entrypc(func, addr) == 0 && *addr != 0) return true;

  Dwarf_Addr start = 0, end;
  if (dwarf_ranges(func, 0, addr, &start, &end) >= 0) {
    if (*addr == 0) *addr = start;

    return *addr != 0;
  }

  return false;
}

Dwarf_Die *DwarfParser::resolve_typedecl(Dwarf_Die *type) {
  const char *name = dwarf_diename(type);
  if (!name) return NULL;

  string type_name = cache_type_prefix(type) + string(name);

  for (auto &p : global_type_cache) {
    auto cus = p.second;
    for (auto i = cus.begin(); i != cus.end(); ++i) {
      auto &v = (*i).second;
      if (v.find(type_name) != v.end()) return &(v[type_name]);
    }
  }

  cerr << "Couldn't resolve type " << type_name << endl; 

  return NULL;
}

const char *DwarfParser::cache_type_prefix(Dwarf_Die *type) const {
  switch (dwarf_tag(type)) {
    case DW_TAG_enumeration_type:
      return "enum ";
    case DW_TAG_structure_type:
    case DW_TAG_class_type:
      // treating struct/class as equals
      return "struct ";
    case DW_TAG_union_type:
      return "union ";
  }
  return "";
}

int DwarfParser::iterate_types_in_cu(mod_cu_type_cache_t & mcu, Dwarf_Die *cu_die) {
  assert(cu_die);
  assert(dwarf_tag(cu_die) == DW_TAG_compile_unit ||
         dwarf_tag(cu_die) == DW_TAG_type_unit ||
         dwarf_tag(cu_die) == DW_TAG_partial_unit);

  if (dwarf_tag(cu_die) == DW_TAG_partial_unit) return DWARF_CB_OK;

  cu_type_cache_t &v = mcu[cu_die->addr];
  // TODO inner types process
  // bool has_inner_types = dwarf_srclang(cu_die) == DW_LANG_C_plus_plus;

  int rc = DWARF_CB_OK;
  Dwarf_Die die;

  if (dwarf_child(cu_die, &die) != 0) return rc;

  do
    /* We're only currently looking for named types,
     * although other types of declarations exist */
    switch (dwarf_tag(&die)) {
      case DW_TAG_base_type:
      case DW_TAG_enumeration_type:
      case DW_TAG_structure_type:
      case DW_TAG_class_type:
      case DW_TAG_typedef:
      case DW_TAG_union_type: {
        const char *name = dwarf_diename(&die);
        if (!name || dwarf_hasattr(&die, DW_AT_declaration)
            /*TODO || has_only_decl_members(die)*/)
          continue;
        string type_name = cache_type_prefix(&die) + string(name);
        if (v.find(type_name) == v.end()) v[type_name] = die;

      }

      break;

      case DW_TAG_namespace:
        break;
      case DW_TAG_imported_unit:
        break;
    }
  while (rc == DWARF_CB_OK && dwarf_siblingof(&die, &die) == 0);

  return rc;
}

void DwarfParser::traverse_module(Dwfl_Module *mod, Dwarf *dw, bool want_type) {
  assert(dw && mod);

  Dwarf_Off off = 0;
  size_t cuhl;
  Dwarf_Off noff;

  mod_cu_type_cache_t &mcu = global_type_cache[mod];
  while (dwarf_nextcu(dw, off, &noff, &cuhl, NULL, NULL, NULL) == 0) {
    Dwarf_Die die_mem;
    Dwarf_Die *die;
    die = dwarf_offdie(dw, off + cuhl, &die_mem);
    /* Skip partial units. */
    if (dwarf_tag(die) == DW_TAG_compile_unit) {
       	iterate_types_in_cu(mcu, die);
    }
    off = noff;
  }

  if (want_type) {
    // Process type units.
    Dwarf_Off off = 0;
    size_t cuhl;
    Dwarf_Off noff;
    uint64_t type_signature;
    while (dwarf_next_unit(dw, off, &noff, &cuhl, NULL, NULL, NULL, NULL,
                           &type_signature, NULL) == 0) {
      Dwarf_Die die_mem;
      Dwarf_Die *die;
      die = dwarf_offdie_types(dw, off + cuhl, &die_mem);
      if (dwarf_tag(die) == DW_TAG_type_unit) iterate_types_in_cu(mcu, die);
      off = noff;
    }
  }
}

Dwarf_Die DwarfParser::find_param(Dwarf_Die *func, string symbol) {
  Dwarf_Die vardie;

  dwarf_getscopevar(func, 1, symbol.c_str(), 0, NULL, 0, 0, &vardie);

  return vardie;
}

Dwarf_Attribute *DwarfParser::find_func_frame_base(
    Dwarf_Die *func, Dwarf_Attribute *fb_attr_mem) {
  assert(dwarf_tag(func) == DW_TAG_subprogram);

  Dwarf_Attribute *fb_attr = NULL;
  fb_attr = dwarf_attr_integrate(func, DW_AT_frame_base, fb_attr_mem);
  return fb_attr;
}

VarLocation DwarfParser::translate_param_location(Dwarf_Die *func,
                                                  string symbol, Dwarf_Addr pc,
                                                  Dwarf_Die &vardie) {
  vardie = find_param(func, symbol);
  Dwarf_Attribute fb_attr_mem;
  Dwarf_Attribute *fb_attr = find_func_frame_base(func, &fb_attr_mem);

  // Assume the vardie must has a DW_AT_location
  Dwarf_Attribute loc_attr;
  dwarf_attr_integrate(&vardie, DW_AT_location, &loc_attr);

  Dwarf_Op *expr;
  size_t len;
  int r = dwarf_getlocation_addr(&loc_attr, pc, &expr, &len, 1);
  if (r != 1 || len <= 0) {
    cerr << "Get param location expr failed for symbol " << symbol << endl;
  }

  VarLocation varloc;
  translate_expr(fb_attr, expr, pc, varloc);
  return varloc;
}

bool DwarfParser::find_prologue(Dwarf_Die *func, Dwarf_Addr &pc) {
  Dwarf_Addr entrypc;
  string funcname = dwarf_diename(func);
  if (func_entrypc(func, &entrypc) == false) {
    cerr << "Error in func_entrypc " << funcname << endl;
    return false;
  }

  int dwbias = 0;
  entrypc += dwbias;

  // identify whether it's compiled with -O2 -g
  if (has_loclist()) {
    pc = entrypc;
    return true;
  }

  Dwarf_Addr *bkpts = NULL;
  int bcnt = dwarf_entry_breakpoints(func, &bkpts);
  if (bcnt <= 0) {
    cerr << "Couldn't find prologue for function " << funcname << endl;
    return false;
  }

  if (bcnt > 1) {
    debug_print("Found more than 1 prologue for function ", funcname, "\n");
  }
  pc = bkpts[0];
  free(bkpts);
  debug_print("prologue is at ", pc, "\n");
  return true;
}

void DwarfParser::dwarf_die_type(Dwarf_Die *die, Dwarf_Die *typedie_mem) const {
  Dwarf_Attribute attr_mem, *attr;
  attr = dwarf_attr_integrate(die, DW_AT_type, &attr_mem);
  Dwarf_Die *tmpdie = dwarf_formref_die(attr, typedie_mem);
  if (tmpdie != NULL && dwarf_tag(tmpdie) == DW_TAG_unspecified_type) {
    debug_print("detects unspecified type\n");
  } else if (tmpdie == NULL) {
    debug_print("no type detected\n");
  }
}

Dwarf_Die * DwarfParser::dwarf_attr_die(Dwarf_Die *die, unsigned int attr_flag, Dwarf_Die *result)
{
  Dwarf_Attribute attr_mem, *attr;
  attr = dwarf_attr_integrate(die, attr_flag, &attr_mem);
  if (dwarf_formref_die (attr, result) != NULL)
    {
      /* Get the actual DIE type*/
      if (attr_flag == DW_AT_type)
	{
	  Dwarf_Attribute sigm;
	  Dwarf_Attribute *sig = dwarf_attr (result, DW_AT_signature, &sigm);
	  if (sig != NULL)
	    result = dwarf_formref_die (sig, result);

	  /* A DW_AT_signature might point to a type_unit, then
	     the actual type DIE we want is the first child.  */
	  if (result != NULL && dwarf_tag (result) == DW_TAG_type_unit)
	    dwarf_child (result, result);
	}
      return result;
    }
  return NULL;
}

void DwarfParser::find_class_member(Dwarf_Die *vardie, Dwarf_Die *typedie,
                                    string member, Dwarf_Attribute *attr) {
  // TODO deal with inheritance later

  std::queue<Dwarf_Die> die_queue;
  die_queue.push(*typedie);
  while (!die_queue.empty()) {
    bool found = false;
    Dwarf_Die die;
    int r = dwarf_child(&die_queue.front(), &die);
    if (r != 0) {
      cerr << "the class " << dwarf_diename(typedie)
           << " has no children, unexpected and exit" << endl;
      return;
    }
    do {
      int tag = dwarf_tag(&die);
      if (tag != DW_TAG_member && tag != DW_TAG_inheritance &&
          tag != DW_TAG_enumeration_type)
        continue;

      const char *name = dwarf_diename(&die);
      if (tag == DW_TAG_inheritance) {
        Dwarf_Die inheritee;
        if (dwarf_attr_die (&die, DW_AT_type, &inheritee))
          die_queue.push(inheritee);
      } else if (tag == DW_TAG_enumeration_type) {
        // TODO
      } else if (name == NULL) {
        // TODO
      } else if (name == member) {
        *vardie = die;
	found = true;
	break;
      }

    } while (dwarf_siblingof(&die, &die) == 0);
    die_queue.pop();
    if (found) 
	break;
  }

  if (dwarf_hasattr_integrate(vardie, DW_AT_data_member_location)) {
    dwarf_attr_integrate(vardie, DW_AT_data_member_location, attr);
  } else if (dwarf_hasattr_integrate(vardie, DW_AT_data_bit_offset)) {
    // TODO deal with bit member
  }
}

void DwarfParser::translate_fields(Dwarf_Die *vardie, Dwarf_Die *typedie,
                                   Dwarf_Addr pc, vector<string> fields,
                                   vector<Field> &res) {
  int i = 1;
  for (auto &x : res) {
    x.pointer = false;
    x.offset = 0;
  }
  while (i < (int)fields.size()) {
    switch (dwarf_tag(typedie)) {
      case DW_TAG_typedef:
      case DW_TAG_const_type:
      case DW_TAG_volatile_type:
      case DW_TAG_restrict_type:
        /* Just iterate on the referent type.  */
        dwarf_die_type(typedie, typedie);
        break;

      case DW_TAG_reference_type:
      case DW_TAG_rvalue_reference_type:
        res[i].pointer = true;
        dwarf_die_type(typedie, typedie);
        break;
      case DW_TAG_pointer_type:
        /* A pointer with no type is a void* -- can't dereference it. */
        if (!dwarf_hasattr_integrate(typedie, DW_AT_type)) {
          debug_print("invalid access pointer ", fields[i], "\n");
          return;
        }
        res[i].pointer = true;
        dwarf_die_type(typedie, typedie);
        break;
      case DW_TAG_array_type:
        // TODO
        break;
      case DW_TAG_structure_type:
      case DW_TAG_union_type:
      case DW_TAG_class_type: {
        if (dwarf_hasattr(typedie, DW_AT_declaration)) {
          Dwarf_Die *tmpdie = resolve_typedecl(typedie);
          if (tmpdie == NULL) {
            debug_print("couldn't resolve type at ", fields[i], "\n");
            return;
          }

          *typedie = *tmpdie;
        }
        Dwarf_Attribute attr;
        find_class_member(vardie, typedie, fields[i], &attr);
        Dwarf_Op *expr;
        size_t len;
        if (dwarf_getlocation_addr(&attr, pc, &expr, &len, 1) != 1) {
          debug_print("failed to get location of attr for ", fields[i], "\n");
          return;
        }
        VarLocation varloc;
        translate_expr(NULL, expr, pc, varloc);
        res[i].offset = varloc.offset;

        dwarf_die_type(vardie, typedie);
        ++i;
      } break;
      case DW_TAG_enumeration_type:
      case DW_TAG_base_type:
        debug_print("invalid access enum or base type ", fields[i], "\n");
        break;
      default:
        debug_print("unexpected type ", fields[i], "\n");
        break;
    }
  }
}

bool DwarfParser::filter_func(string funcname) {
  for (auto x : probes) {
    size_t found = x.first.find_last_of(":");
    string name = x.first.substr(found + 1);
    if (funcname == name) return true;
  }
  return false;
}

bool DwarfParser::filter_cu(string unitname) {
  size_t found = unitname.find_last_of("/");
  string name = unitname.substr(found + 1);

  for (auto x : probe_units) {
    if (x == name) return true;
  }
  return false;
}

std::string DwarfParser::special_inlined_function_scope(const char *funcname){
  if (strcmp(funcname, "log_latency") == 0)
    return "BlueStore";
  if (strcmp(funcname, "log_latency_fn") == 0)
    return "BlueStore";
  return "";
}

// implement the callback function handle_attr
int handle_attr(Dwarf_Attribute *attr, void *data) {
  (void)data;
  unsigned int code = dwarf_whatattr(attr);
  unsigned int form = dwarf_whatform(attr);
  // print code name and form name
  debug_print("  code: ", DwarfParser::dwarf_attr_string(code), ", form: ", DwarfParser::dwarf_form_string(form), "\n");
  return 0;
}

int handle_function(Dwarf_Die *die, void *data) {
  assert(data != NULL);
  DwarfParser *dp = (DwarfParser *)data;
  const char *funcname = dwarf_diename(die);
  if (!dp->filter_func(funcname)) return 0;
  Dwarf_Die func_abstract = *die;
  // in case of compiler's lto optimization, need to find the abstract function die 
  // in the source code module
  if (dwarf_hasattr(die, DW_AT_abstract_origin)) {
    Dwarf_Attribute attr_mem;
    Dwarf_Attribute *tmpattr =
        dwarf_attr_integrate(die, DW_AT_abstract_origin, &attr_mem);
    dwarf_formref_die(tmpattr, &func_abstract);
  }

  Dwarf_Die func_spec = func_abstract;
  if (dwarf_hasattr(&func_abstract, DW_AT_specification)) {
    Dwarf_Attribute attr_mem;
    Dwarf_Attribute *tmpattr =
        dwarf_attr_integrate(&func_abstract, DW_AT_specification, &attr_mem);
    dwarf_formref_die(tmpattr, &func_spec);
  }
  Dwarf_Die *scopes;
  int nscopes = dwarf_getscopes_die(&func_spec, &scopes);
  
  string fullname = funcname;

  if (nscopes > 1) {
    string scopename = dp->special_inlined_function_scope(funcname);
    if (dwarf_tag(&scopes[1]) == DW_TAG_class_type ||
                    dwarf_tag(&scopes[1]) == DW_TAG_structure_type) {
      scopename = dwarf_diename(&scopes[1]);
    }
    if (!scopename.empty()) {
      fullname = scopename + "::" + fullname;
    }
  }

  if (dp->probes.find(fullname) == dp->probes.end()) {
    return 0;
  }

  //TODO Need to find all the instances of the inlined function, now we just filter those inline function
  if (dwarf_func_inline(die) != 0) {
     // Refer to elfutils/tests: we can iterate all inlined instances via below function
     // dwarf_func_inline_instances(die, &handle_instance, NULL);
     return 0;
  } 
  
  if (dwarf_getattrs(die, handle_attr, NULL, 0) != 1) {
    cerr << "dwarf_getattrs failed" << endl;
  }

  // TODO need to check if the class name matches
  Dwarf_Addr pc;
  if (!dp->find_prologue(die, pc)) {
    // LTO optimization will not generate the low_pc/high_pc/rangs for the abstract function
    return 0;
  }
  auto &func2pc = dp->mod_func2pc[dp->cur_mod_name];
  func2pc[fullname] = pc;

  auto &func2vf = dp->mod_func2vf[dp->cur_mod_name];
  auto &vf = func2vf[fullname];
  auto arr = dp->probes[fullname];
  vf.resize(arr.size());

  for (int i = 0; i < (int)arr.size(); ++i) {
    string varname = arr[i][0];
    Dwarf_Die vardie, typedie;
    VarLocation varloc = dp->translate_param_location(die, varname, pc, vardie);
    vf[i].varloc = varloc;

    // translate fileds
    dp->dwarf_die_type(&vardie, &typedie);
    vf[i].fields.resize(arr[i].size());
    dp->translate_fields(&vardie, &typedie, pc, arr[i], vf[i].fields);
  }
  return 0;
}

void DwarfParser::translate_expr(Dwarf_Attribute *fb_attr, Dwarf_Op *expr,
                                 Dwarf_Addr pc, VarLocation &varloc) {
  int atom = expr->atom;

  // TODO can put a debug message to print the atom's name in string

  switch (atom) {
    case DW_OP_deref:
    case DW_OP_dup:
    case DW_OP_drop:
    case DW_OP_over:
    case DW_OP_swap:
    case DW_OP_rot:
    case DW_OP_xderef:
    case DW_OP_abs:
    case DW_OP_and:
    case DW_OP_div:
    case DW_OP_minus:
    case DW_OP_mod:
    case DW_OP_mul:
    case DW_OP_neg:
    case DW_OP_not:
    case DW_OP_or:
    case DW_OP_plus:
    case DW_OP_shl:
    case DW_OP_shr:
    case DW_OP_shra:
    case DW_OP_xor:
    case DW_OP_eq:
    case DW_OP_ge:
    case DW_OP_gt:
    case DW_OP_le:
    case DW_OP_lt:
    case DW_OP_ne:
    case DW_OP_lit0 ... DW_OP_lit31:
    case DW_OP_nop:
    case DW_OP_stack_value:
    case DW_OP_form_tls_address:
      /* No arguments. */
      debug_print("atom ", atom, "\n");
      break;

    case DW_OP_bregx:
      varloc.reg = expr->number;
      varloc.offset = expr->number2;
      break;

    case DW_OP_breg0 ... DW_OP_breg31:
      varloc.reg = expr->atom - DW_OP_breg0;
      varloc.offset = expr->number;
      break;

    case DW_OP_fbreg: {
      Dwarf_Op *fb_expr;
      size_t fb_exprlen;
      int res = dwarf_getlocation_addr(fb_attr, pc, &fb_expr, &fb_exprlen, 1);
      if (res != 1) {
        cerr << "translate_expr get fb_expr failed" << endl;
      }

      translate_expr(fb_attr, fb_expr, pc, varloc);
      varloc.offset += expr->number;
      varloc.stack = true;
    } break;

    case DW_OP_call_frame_cfa: {
      Dwarf_Op *cfa_ops = NULL;
      size_t cfa_nops = 0;
      // Try .debug_frame first
      Dwarf_Frame *frame = NULL;
      if (cfi_debug != NULL) {
        if (dwarf_cfi_addrframe(cfi_debug, pc, &frame) == 0) {
          dwarf_frame_cfa(frame, &cfa_ops, &cfa_nops);
        } else {
          cerr << "dwarf_frame_cfa add debug frame failed" << endl;
        }
      }

      if (cfa_ops == NULL) {
        if (dwarf_cfi_addrframe(cfi_eh, pc, &frame) == 0) {
          dwarf_frame_cfa(frame, &cfa_ops, &cfa_nops);
        } else {
          cerr << "dwarf_frame_cfa add eh frame failed" << endl;
        }
      }

      translate_expr(fb_attr, cfa_ops, pc, varloc);
    } break;
    case DW_OP_reg0 ... DW_OP_reg31:
      varloc.reg = expr->atom - DW_OP_reg0;
      break;

    case DW_OP_plus_uconst:
      varloc.offset = expr->number;
      break;

    default:
      break;
  }
}

Dwfl *DwarfParser::create_dwfl(int fd, const char *fname) {
  int dwfl_fd = dup(fd);
  Dwfl *dwfl = NULL;
  if (dwfl_fd < 0) {
    cerr << "create_dwfl dup failed" << endl;
    return 0;
  }

  static const Dwfl_Callbacks callbacks = {
      .find_elf = dwfl_linux_proc_find_elf,
      .find_debuginfo = dwfl_standard_find_debuginfo,
      .section_address = dwfl_offline_section_address,
      .debuginfo_path = nullptr};

  dwfl = dwfl_begin(&callbacks);

  if (dwfl_report_offline(dwfl, fname, fname, dwfl_fd) == NULL) {
    cerr << "dwfl_report_offline open dwfl failed" << endl;
    close(dwfl_fd);
    dwfl = NULL;
  } else
    dwfl_report_end(dwfl, NULL, NULL);

  return dwfl;
}


int preprocess_module(Dwfl_Module *dwflmod, void **userdata,
                         const char *name, Dwarf_Addr base, void *arg) {
  (void)userdata;
  (void)name;
  (void)base;

  DwarfParser *dp = (DwarfParser *)arg;
  assert(dwflmod != NULL && dp != NULL);

  dp->cur_mod = dwflmod;
  const char* mod_path = dwfl_module_info(dwflmod, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  dp->cur_mod_name = get_basename(mod_path);
  Dwarf_Addr modbias;
  Dwarf *dwarf = dwfl_module_getdwarf(dwflmod, &modbias);

  if (!dwarf) {
    cerr << "preprocess_module dwarf get error" << endl;
    return EXIT_FAILURE;
  }


  dp->traverse_module(dwflmod, dwarf, true); 
  return 0;
}


int handle_module(Dwfl_Module *dwflmod, void **userdata,
                         const char *name, Dwarf_Addr base, void *arg) {
  (void)userdata;
  (void)name;
  (void)base;

  DwarfParser *dp = (DwarfParser *)arg;
  assert(dwflmod != NULL && dp != NULL);

  dp->cur_mod = dwflmod;
  const char* mod_path = dwfl_module_info(dwflmod, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  dp->cur_mod_name = get_basename(mod_path);
  Dwarf_Addr modbias;
  Dwarf *dwarf = dwfl_module_getdwarf(dwflmod, &modbias);

  if (!dwarf) {
    cerr << "handle_module dwarf get error" << endl;
    return EXIT_FAILURE;
  }

  int start_time = clock();

  Dwarf_Off offset = 0;
  Dwarf_Off next_offset;
  size_t header_size;
  Dwarf_Die cu_die;

  while (dwarf_nextcu(dwarf, offset, &next_offset, &header_size, nullptr,
                      nullptr, nullptr) == 0) {
    if (dwarf_offdie(dwarf, offset + header_size, &cu_die) != nullptr) {
      dp->cfi_debug = dwfl_module_dwarf_cfi(dwflmod, &dp->cfi_debug_bias);
      dp->cfi_eh = dwfl_module_eh_cfi(dwflmod, &dp->cfi_eh_bias);
      assert(dp->cfi_debug == NULL || dp->cfi_debug_bias == 0);

      string cu_name = dwarf_diename(&cu_die) ?: "<unknown>";
      if (dp->filter_cu(cu_name) || cu_name == "<artificial>") {
        dp->cur_cu = &cu_die;
        dwarf_getfuncs(&cu_die, (int (*)(Dwarf_Die *, void *))handle_function,
                       dp, 0);
      }
    }
    offset = next_offset;
  }
  int end_process_funcs_time = clock();
  double elapsed_sec = (double)(end_process_funcs_time - start_time) / CLOCKS_PER_SEC;
  debug_print("handle_module ", dp->cur_mod_name, " processing time: ", elapsed_sec, " seconds\n");

  return 0;
}

int DwarfParser::parse() {
  if(getenv("DEBUGINFOD_URLS") == NULL) {
    //If the DEBUGINFOD_URLS is not set, set it to https://debuginfod.ubuntu.com as default
    char envs[] = "DEBUGINFOD_URLS=https://debuginfod.ubuntu.com";
    putenv(envs);
  }

  for (auto dwfl: dwfls) {
    dwfl_getmodules(dwfl, preprocess_module, this, 0);
  }
  for (auto dwfl: dwfls) {
    dwfl_getmodules(dwfl, handle_module, this, 0);
  }
  return 0;
}

void DwarfParser::add_module(string path) {
  const char *fname = path.c_str();
  int fd = open(fname, O_RDONLY);
  if (fd == -1) {
    cerr << "cannot open input file " << fname;
  }
  
  Dwfl *dwfl = create_dwfl(fd, fname);
  dwfls.push_back(dwfl);
}

DwarfParser::DwarfParser(probes_t ps, vector<string> pus)
    : probe_units(pus),
      probes(ps),
      cur_mod(NULL),
      cur_cu(NULL),
      cfi_debug(NULL),
      cfi_eh(NULL) {
}

DwarfParser::~DwarfParser() {}

const char* DwarfParser::dwarf_attr_string(unsigned int attrnum) {
  switch (attrnum) {
    #define DWARF_ONE_KNOWN_DW_AT(NAME, CODE) case CODE: return  "DW_AT_"#NAME;
    DWARF_ALL_KNOWN_DW_AT
    #undef DWARF_ONE_KNOWN_DW_AT
    default:
      return "DW_AT_<unknown>";
  }
}

const char* DwarfParser::dwarf_form_string(unsigned int form) {
  switch (form) {
    #define DWARF_ONE_KNOWN_DW_FORM(NAME, CODE) case CODE: return "DW_FORM_"#NAME;
    DWARF_ALL_KNOWN_DW_FORM
    #undef DWARF_ONE_KNOWN_DW_FORM
    default:
      return "DW_FORM_<unknown>";
  }
}
