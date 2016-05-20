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
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_PACKET_UTIL_H_
#define CEPH_MSG_PACKET_UTIL_H_

#include <map>
#include <iostream>

#include "Packet.h"

template <typename Offset, typename Tag>
class packet_merger {
 private:
  static uint64_t& linearizations_ref() {
    static thread_local uint64_t linearization_count;
    return linearization_count;
  }
 public:
  std::map<Offset, Packet> map;

  static uint64_t linearizations() {
    return linearizations_ref();
  }

  void merge(Offset offset, Packet p) {
    bool insert = true;
    auto beg = offset;
    auto end = beg + p.len();
    // Fisrt, try to merge the packet with existing segment
    for (auto it = map.begin(); it != map.end();) {
      auto& seg_pkt = it->second;
      auto seg_beg = it->first;
      auto seg_end = seg_beg + seg_pkt.len();
      // There are 6 cases:
      if (seg_beg <= beg && end <= seg_end) {
        // 1) seg_beg beg end seg_end
        // We already have data in this packet
        return;
      } else if (beg <= seg_beg && seg_end <= end) {
        // 2) beg seg_beg seg_end end
        // The new segment contains more data than this old segment
        // Delete the old one, insert the new one
        it = map.erase(it);
        insert = true;
        break;
      } else if (beg < seg_beg && seg_beg <= end && end <= seg_end) {
        // 3) beg seg_beg end seg_end
        // Merge two segments, trim front of old segment
        auto trim = end - seg_beg;
        seg_pkt.trim_front(trim);
        p.append(std::move(seg_pkt));
        // Delete the old one, insert the new one
        it = map.erase(it);
        insert = true;
        break;
      } else if (seg_beg <= beg && beg <= seg_end && seg_end < end) {
        // 4) seg_beg beg seg_end end
        // Merge two segments, trim front of new segment
        auto trim = seg_end - beg;
        p.trim_front(trim);
        // Append new data to the old segment, keep the old segment
        seg_pkt.append(std::move(p));
        seg_pkt.linearize();
        ++linearizations_ref();
        insert = false;
        break;
      } else {
        // 5) beg end < seg_beg seg_end
        //   or
        // 6) seg_beg seg_end < beg end
        // Can not merge with this segment, keep looking
        it++;
        insert = true;
      }
    }

    if (insert) {
      p.linearize();
      ++linearizations_ref();
      map.emplace(beg, std::move(p));
    }

    // Second, merge adjacent segments after this packet has been merged,
    // becasue this packet might fill a "whole" and make two adjacent
    // segments mergable
    for (auto it = map.begin(); it != map.end();) {
      // The first segment
      auto& seg_pkt = it->second;
      auto seg_beg = it->first;
      auto seg_end = seg_beg + seg_pkt.len();

      // The second segment
      auto it_next = it;
      it_next++;
      if (it_next == map.end()) {
        break;
      }
      auto& p = it_next->second;
      auto beg = it_next->first;
      auto end = beg + p.len();

      // Merge the the second segment into first segment if possible
      if (seg_beg <= beg && beg <= seg_end && seg_end < end) {
        // Merge two segments, trim front of second segment
        auto trim = seg_end - beg;
        p.trim_front(trim);
        // Append new data to the first segment, keep the first segment
        seg_pkt.append(std::move(p));

        // Delete the second segment
        map.erase(it_next);

        // Keep merging this first segment with its new next packet
        // So we do not update the iterator: it
        continue;
      } else if (end <= seg_end) {
        // The first segment has all the data in the second segment
        // Delete the second segment
        map.erase(it_next);
        continue;
      } else if (seg_end < beg) {
        // Can not merge first segment with second segment
        it = it_next;
        continue;
      } else {
        // If we reach here, we have a bug with merge.
        std::cout << "packet_merger: merge error\n";
        abort();
        break;
      }
    }
  }
};

#endif
