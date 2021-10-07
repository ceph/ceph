// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/common_fwd.h"

#include <functional>
#include <string>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/optional.hpp>

namespace rgw::amqp_1 {

	struct connection_t;

	typedef boost::intrusive_ptr<connection_t> connection_ptr_t;

	void intrusive_ptr_add_ref(const connection_t* p);
	void intrusive_ptr_release(const connection_t* p);

	typedef std::function<void(int)> reply_callback_t;

	bool init(CephContext* cct);

	void shutdown();

	// TODO: the security configuration needs more params maybe
	connection_ptr_t connect(const std::string& url);

	int publish(connection_ptr_t& conn, const std::string& topic, const
			std::string& message);

	int publish_with_confirm(connection_ptr_t& conn, const std::string& topic, const
			std::string& message, reply_callback_t cb);

	// convert the integer status returned from the "publish" function to a string
	std::string status_to_string(int s);

	// number of connections
	size_t get_connection_count();

	// return the number of messages that were sent
	// to broker, but were not yet acked/nacked/timedout
	size_t get_inflight();

	// running counter of successfully queued messages
	size_t get_queued();

	// running counter of dequeued messages
	size_t get_dequeued();

	// number of maximum allowed connections
	size_t get_max_connections();

	// number of maximum allowed inflight messages
	size_t get_max_inflight();

	// display connection as string
	std::string to_string(const connection_ptr_t& conn);

}

