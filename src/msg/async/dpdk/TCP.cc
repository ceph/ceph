// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "TCP.h"
#include "IP.h"
#include "DPDKStack.h"

void tcp_option::parse(uint8_t* beg, uint8_t* end) {
	while (beg < end) {
		auto kind = option_kind(*beg);
		if (kind != option_kind::nop && kind != option_kind::eol) {
			// Make sure there is enough room for this option
			auto len = *(beg + 1);
			if (beg + len > end) {
				return;
			}
		}
		switch (kind) {
			case option_kind::mss:
				_mss_received = true;
				_remote_mss = ntoh(reinterpret_cast<mss*>(beg)->mss);
				beg += option_len::mss;
				break;
			case option_kind::win_scale:
				_win_scale_received = true;
				_remote_win_scale = reinterpret_cast<win_scale*>(beg)->shift;
				// We can turn on win_scale option, 7 is Linux's default win scale size
				_local_win_scale = 7;
				beg += option_len::win_scale;
				break;
			case option_kind::sack:
				_sack_received = true;
				beg += option_len::sack;
				break;
			case option_kind::nop:
				beg += option_len::nop;
				break;
			case option_kind::eol:
				return;
			default:
				// Ignore options we do not understand
				auto len = *(beg + 1);
				beg += len;
				// Prevent infinite loop
				if (len == 0) {
					return;
				}
				break;
		}
	}
}

uint8_t tcp_option::fill(tcp_hdr* th, uint8_t options_size) {
	auto hdr = reinterpret_cast<uint8_t*>(th);
	auto off = hdr + sizeof(tcp_hdr);
	uint8_t size = 0;
	bool syn_on = th->f_syn;
	bool ack_on = th->f_ack;

	if (syn_on) {
		if (_mss_received || !ack_on) {
			auto mss = new (off) tcp_option::mss;
			mss->mss = _local_mss;
			off += mss->len;
			size += mss->len;
			*mss = mss->hton();
		}
		if (_win_scale_received || !ack_on) {
			auto win_scale = new (off) tcp_option::win_scale;
			win_scale->shift = _local_win_scale;
			off += win_scale->len;
			size += win_scale->len;
		}
	}
	if (size > 0) {
		// Insert NOP option
		auto size_max = align_up(uint8_t(size + 1), tcp_option::align);
		while (size < size_max - uint8_t(option_len::eol)) {
			new (off) tcp_option::nop;
			off += option_len::nop;
			size += option_len::nop;
		}
		new (off) tcp_option::eol;
		size += option_len::eol;
	}
	assert(size == options_size);

	return size;
}

uint8_t tcp_option::get_size(bool syn_on, bool ack_on) {
	uint8_t size = 0;
	if (syn_on) {
		if (_mss_received || !ack_on) {
			size += option_len::mss;
		}
		if (_win_scale_received || !ack_on) {
			size += option_len::win_scale;
		}
	}
	if (size > 0) {
		size += option_len::eol;
		// Insert NOP option to align on 32-bit
		size = align_up(size, tcp_option::align);
	}
	return size;
}

ipv4_tcp::ipv4_tcp(ipv4& inet)
		: _inet_l4(inet), _tcp(std::make_unique<tcp<ipv4_traits>>(_inet_l4)) {
}

ipv4_tcp::~ipv4_tcp() {
}

void ipv4_tcp::received(packet p, ipv4_address from, ipv4_address to) {
	_tcp->received(std::move(p), from, to);
}

bool ipv4_tcp::forward(forward_hash& out_hash_data, packet& p, size_t off) {
	return _tcp->forward(out_hash_data, p, off);
}

ServerSocket tcpv4_listen(tcp<ipv4_traits>& tcpv4, uint16_t port, socket_options opts) {
	return ServerSocket(std::make_unique<DPDKServerSocketImpl<tcp<ipv4_traits>>>(
			tcpv4, port, opts));
}

ConnectedSocket tcpv4_connect(tcp<ipv4_traits>& tcpv4, const entity_addr_t &addr) {
	auto conn = tcpv4.connect(addr);
	std::unique_ptr<ConnectedSocketImpl> csi(new NativeConnectedSocketImpl<tcp<ipv4_traits>>(std::move(conn)));
	return ConnectedSocket(std::move(csi));
}
