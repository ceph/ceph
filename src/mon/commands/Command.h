// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC <contact@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MON_COMMAND_H
#define CEPH_MON_COMMAND_H

#include <string>
#include <set>
#include <sstream>

#include <boost/intrusive_ptr.hpp>

#include "mon/Monitor.h"
#include "mon/MonOpRequest.h"

#include "include/types.h"
#include "include/encoding.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/cmdparse.h"
#include "common/RefCountedObj.h"

using namespace std;

/*
 * CommandCommon
 *
 * Define members and functions common to commands.
 */
struct Common
{

  explicit Common(
      Monitor *_mon,
      PaxosService *_svc,
      CephContext *_cct) :
    mon(_mon),
    service(_svc),
    cct(_cct)
  { }

  virtual ~Common() { }

 private:
  Common() = delete;
  void wait_for_finished_proposal(MonOpRequestRef op, Context *ctx) {
    service->wait_for_finished_proposal(op, ctx);
  }

 protected:
  Monitor *mon;
  PaxosService *service;
  CephContext *cct;

  version_t get_last_committed() {
    return service->get_last_committed();
  }

 public:
  bool reply_with_data(MonOpRequestRef op,
		       int retcode,
		       stringstream &ss,
		       bufferlist &retdata,
		       version_t version) {
    string retstr;
    getline(ss, retstr);

    bool ret = true;

    if (retcode < 0 && retstr.length() == 0) {
      retstr = cpp_strerror(retcode);
      ret = false;
    }
    mon->reply_command(op, retcode, retstr, retdata, version);
    return ret;
  }

  virtual bool reply(
      MonOpRequestRef op,
      int retcode,
      stringstream &ss,
      version_t version) {
    bufferlist retdata;
    return reply_with_data(op, retcode, ss, retdata, version);
  }

  bool update(MonOpRequestRef op,
	      stringstream &ss,
	      version_t version) {

    string retstr;
    getline(ss, retstr);

    wait_for_finished_proposal(op,
	new Monitor::C_Command(mon, op, 0, retstr, version));
    return true;
  }

  bool wait_retry(MonOpRequestRef op,
	     stringstream &ss,
	     version_t version) {
    string retstr;
    getline(ss, retstr);

    wait_for_finished_proposal(op,
	new Monitor::C_RetryMessage(mon, op));
    return true;
  }
};


/* Command
 *
 * Define a command. It will be templated with
 *  T	    = a paxos service
 *  Stable  = the typename of the paxos service's stable map
 *  Pending = the typename of the paxos service's pending map
 *
 * Depending on a paxos service's specificities, Stable and Pending may be
 * the same.
 *
 * This class doesn't use these types for much more than defining the prepare
 * and preprocess functions to be called by derived classes, but still
 * requires them to be defined to properly abstract command definitions.
 */

typedef std::shared_ptr<Formatter> FormatterRef;

template<typename T, typename Stable, typename Pending>
struct Command : public Common
{

  explicit Command(
      Monitor *_mon,
      PaxosService *_service,
      CephContext *_cct) :
    Common(_mon, _service, _cct)
  { }
  
  virtual ~Command() { };

  virtual bool preprocess(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      const Stable &stable_map) {

    stringstream ss;
    bufferlist rdata;

    string format;
    cmd_getval(cct, cmdmap, "format", format, string("plain"));
    FormatterRef f(Formatter::create(format));

    string prefix;
    cmd_getval(cct, cmdmap, "prefix", prefix);

    return do_preprocess(op, prefix, cmdmap, ss, rdata, f, stable_map);
  }

  virtual bool prepare(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      Pending &pending_map,
      Stable &stable_map) {

    op->mark_osdmon_event(__func__);
    stringstream ss;
    bufferlist rdata;

    string format;
    cmd_getval(cct, cmdmap, "format", format, string("plain"));
    FormatterRef f(Formatter::create(format));

    string prefix;
    cmd_getval(cct, cmdmap, "prefix", prefix);

    return do_prepare(op, prefix, cmdmap,
	ss, rdata, f, pending_map, stable_map);
  }

  virtual bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      const Stable &stable_map) = 0;

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      Pending &pending_map,
      Stable &stable_map) = 0;

  virtual bool handles_command(const string &prefix) = 0;

 private:
  Command() = delete;
};


/* ReadCommand
 *
 * Defines a templated Read-only command. These will be called during
 * preprocess. This class abstracts a derived class' command definition, by
 * redefining the preprocess command from the 'Command' class, and then
 * calling a function to be implemented by the class deriving from this one.
 *
 * Preparing is still left defined as the 'Command' class has it defined,
 * which is basically returning 'false'.
 */
template<typename T, typename Stable, typename Pending>
struct ReadCommand : public Command<T, Stable, Pending>
{

  explicit ReadCommand(
      Monitor *_mon,
      PaxosService *_svc,
      CephContext *_cct) :
    Command<T, Stable, Pending>(_mon, _svc, _cct)
  { };
  virtual ~ReadCommand() { }

  using Command<T, Stable, Pending>::prepare;
  virtual bool prepare(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      Pending &pending_map,
      Stable &stable_map) final {
    ceph_assert(0 == "read-only commands do not perform prepare phase");
    return false;
  }

  virtual bool do_prepare(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      Pending &pending_map,
      Stable &stable_map) final {
    ceph_assert(0 == "read-only commands do not perform prepare phase");
    return false;
  }

 private:
  ReadCommand() = delete;

};

/* WriteCommand
 *
 * Defines a templated a Write command. These will be called during prepare.
 * This class abstracts a derived class' command definition for the prepare
 * stage, redefining prepare function inherited from the 'Command' class.
 *
 * For commands that will have both the preprocess and prepare stages, said
 * command implementation will require to derive from both the 'ReadCommand'
 * and 'WriteCommand' classes, and implement both 'do_{preprocess,prepare}'
 * functions.
 */
template<typename T, typename Stable, typename Pending>
struct WriteCommand : public Command<T, Stable, Pending>
{

  explicit WriteCommand(
      Monitor *_mon,
      PaxosService *_svc,
      CephContext *_cct) :
    Command<T, Stable, Pending>(_mon, _svc, _cct)
  { }
  virtual ~WriteCommand() { }

  using Command<T, Stable, Pending>::preprocess;
  virtual bool preprocess(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      Stable &stable_map) final {
    ceph_assert(0 == "write commands do not perform preprocess phase");
    return false;
  }

  virtual bool do_preprocess(
      MonOpRequestRef op,
      const string &prefix,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      const Stable &stable_map) final {
    ceph_assert(0 == "write commands do not perform preprocess phase");
    return false;
  }

 private:
  WriteCommand() = delete;
};

template<typename T, typename Stable, typename Pending>
struct ReadWriteCommand : public Command<T, Stable, Pending>
{
  explicit ReadWriteCommand<T, Stable, Pending>(
      Monitor *_mon,
      PaxosService *_svc,
      CephContext *_cct) :
    Command<T, Stable, Pending>(_mon, _svc, _cct)
  { }
  
  virtual ~ReadWriteCommand() { }

  private:
  ReadWriteCommand() = delete;
};

#endif // CEPH_MON_COMMAND_H
