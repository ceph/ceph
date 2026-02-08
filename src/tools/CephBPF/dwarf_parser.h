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

#ifndef DWARF_PARSER_H
#define DWARF_PARSER_H

#include <dwarf.h>
#include <elf.h>
#include <elfutils/libdw.h>
#include <elfutils/libdwfl.h>
#include <elfutils/known-dwarf.h>
#include <vector>
class DwarfParser;
// Forward declarations for callback functions
int handle_function(Dwarf_Die *, void *);
int handle_module(Dwfl_Module *, void **, const char *, Dwarf_Addr,
                         void *);
int preprocess_module(Dwfl_Module *, void **, const char *, Dwarf_Addr,
                         void *);
int handle_attr(Dwarf_Attribute *, void *);

class DwarfParser {
 private:
  friend int handle_module(Dwfl_Module *, void **, const char *, Dwarf_Addr,
                           void *);
  friend int handle_function(Dwarf_Die *, void *);
  friend int preprocess_module(Dwfl_Module *, void **, const char *, Dwarf_Addr,
                           void *);

  typedef std::unordered_map<std::string, Dwarf_Die> cu_type_cache_t;
  typedef std::unordered_map<void *, cu_type_cache_t> mod_cu_type_cache_t;
  typedef std::unordered_map<void *, mod_cu_type_cache_t> global_mod_cu_type_cache_t; 
  

  typedef std::map<std::string, std::vector<VarField>> func2vf_t;
  typedef std::map<std::string, Dwarf_Addr> func2pc_t;
  typedef std::map<std::string, func2vf_t> mod_func2vf_t;
  typedef std::map<std::string, func2pc_t> mod_func2pc_t;

 public:
  typedef std::map<std::string, std::vector<std::vector<std::string>>> probes_t;
  mod_func2vf_t mod_func2vf;
  mod_func2pc_t mod_func2pc;
  global_mod_cu_type_cache_t global_type_cache;
  std::vector<std::string> probe_units;
  probes_t probes;

 private:
  std::vector<Dwfl *> dwfls;
  Dwfl_Module *cur_mod;
  std::string cur_mod_name;
  Dwarf_Die *cur_cu;
  Dwarf_CFI *cfi_debug;
  Dwarf_CFI *cfi_eh;
  Dwarf_Addr cfi_debug_bias;
  Dwarf_Addr cfi_eh_bias;

 public:
  int parse();

  DwarfParser(probes_t probes, std::vector<std::string> probe_units);

  ~DwarfParser();
  void add_module(std::string);
  bool die_has_loclist(Dwarf_Die *) const;
  bool has_loclist() const;
  Dwarf_Die *resolve_typedecl(Dwarf_Die *);
  const char *cache_type_prefix(Dwarf_Die *) const;
  int iterate_types_in_cu(mod_cu_type_cache_t &, Dwarf_Die *);
  void traverse_module(Dwfl_Module *, Dwarf *, bool);
  Dwarf_Die find_param(Dwarf_Die *, std::string);
  Dwarf_Attribute *find_func_frame_base(Dwarf_Die *, Dwarf_Attribute *);
  VarLocation translate_param_location(Dwarf_Die *, std::string, Dwarf_Addr,
                                       Dwarf_Die &);
  bool func_entrypc(Dwarf_Die *, Dwarf_Addr *);
  bool find_prologue(Dwarf_Die *func, Dwarf_Addr &pc);
  void dwarf_die_type(Dwarf_Die *, Dwarf_Die *) const;
  void find_class_member(Dwarf_Die *, Dwarf_Die *, std::string,
                         Dwarf_Attribute *);
  void translate_fields(Dwarf_Die *, Dwarf_Die *, Dwarf_Addr,
                        std::vector<std::string>, std::vector<Field> &);
  bool filter_func(std::string);
  bool filter_cu(std::string);
  void translate_expr(Dwarf_Attribute *, Dwarf_Op *, Dwarf_Addr, VarLocation &);
  Dwfl *create_dwfl(int, const char *);
  std::string special_inlined_function_scope(const char *);
  Dwarf_Die * dwarf_attr_die(Dwarf_Die*, unsigned int, Dwarf_Die*);

  static const char* dwarf_attr_string(unsigned int attrnum);
  static const char* dwarf_form_string(unsigned int form);
};

#endif
