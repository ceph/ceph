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

// tcp/network-stack integration

#ifndef CEPH_MSG_DPDK_TCP_STACK_H
#define CEPH_MSG_DPDK_TCP_STACK_H

class socket_options;
class ServerSocket;
class ConnectedSocket;

class ipv4_traits;
template <typename InetTraits>
class tcp;

int tcpv4_listen(tcp<ipv4_traits>& tcpv4, uint16_t port, const SocketOptions &opts,
                 ServerSocket *sa);

int tcpv4_connect(tcp<ipv4_traits>& tcpv4, const entity_addr_t &addr,
                  ConnectedSocket *sa);

#endif
