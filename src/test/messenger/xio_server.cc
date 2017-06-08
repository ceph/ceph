// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "msg/xio/XioMessenger.h"
#include "msg/xio/FastStrategy.h"
#include "msg/xio/QueueStrategy.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "perfglue/heap_profiler.h"
#include "common/address_helper.h"
#include "xio_dispatcher.h"

#define dout_subsys ceph_subsys_simple_server


int main(int argc, const char **argv)
{
	vector<const char*> args;
	Messenger *messenger;
	Dispatcher *dispatcher;
	std::vector<const char*>::iterator arg_iter;
	std::string val;
	entity_addr_t bind_addr;
	int r = 0;

	using std::endl;

	std::string addr = "localhost";
	std::string port = "1234";
	bool dfast = false;

	cout << "Xio Server starting..." << endl;

	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	global_init(NULL, args, CEPH_ENTITY_TYPE_ANY, CODE_ENVIRONMENT_DAEMON,
		    0);

	for (arg_iter = args.begin(); arg_iter != args.end();) {
	  if (ceph_argparse_witharg(args, arg_iter, &val, "--addr",
				    (char*) NULL)) {
	    addr = val;
	  } else if (ceph_argparse_witharg(args, arg_iter, &val, "--port",
				    (char*) NULL)) {
	    port = val;
	  }  else if (ceph_argparse_flag(args, arg_iter, "--dfast",
					   (char*) NULL)) {
	    dfast = true;
	  } else {
	    ++arg_iter;
	  }
	};

	string dest_str = "tcp://";
	dest_str += addr;
	dest_str += ":";
	dest_str += port;
	entity_addr_from_url(&bind_addr, dest_str.c_str());

	DispatchStrategy* dstrategy;
	if (dfast)
	  dstrategy = new FastStrategy();
	else
	  dstrategy = new QueueStrategy(2);

	messenger = new XioMessenger(g_ceph_context,
				     entity_name_t::MON(-1),
				     "xio_server",
				     0 /* nonce */, XIO_ALL_FEATURES,
				     dstrategy);

	static_cast<XioMessenger*>(messenger)->set_magic(
	  MSG_MAGIC_REDUPE /* resubmit messages on delivery (REQUIRED) */ |
	  MSG_MAGIC_TRACE_CTR /* timing prints */);

	messenger->set_default_policy(
	  Messenger::Policy::stateless_server(CEPH_FEATURES_ALL, 0));

	r = messenger->bind(bind_addr);
	if (r < 0)
		goto out;

	// Set up crypto, daemonize, etc.
	//global_init_daemonize(g_ceph_context, 0);
	common_init_finish(g_ceph_context);

	dispatcher = new XioDispatcher(messenger);

	messenger->add_dispatcher_head(dispatcher); // should reach ready()
	messenger->start();
	messenger->wait(); // can't be called until ready()

	// done
	delete messenger;

out:
	cout << "Simple Server exit" << endl;
	return r;
}

