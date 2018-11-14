// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef DYNAMIC_PERF_STATS_H
#define DYNAMIC_PERF_STATS_H

#include "messages/MOSDOp.h"
#include "mgr/OSDPerfMetricTypes.h"
#include "osd/OpRequest.h"

class DynamicPerfStats {
public:
  DynamicPerfStats() {
  }

  DynamicPerfStats(const std::list<OSDPerfMetricQuery> &queries) {
    for (auto &query : queries) {
      data[query];
    }
  }

  void merge(const DynamicPerfStats &dps) {
    for (auto &query_it : dps.data) {
      auto &query = query_it.first;
      for (auto &key_it : query_it.second) {
        auto &key = key_it.first;
        auto counter_it = key_it.second.begin();
        auto update_counter_fnc =
            [&counter_it](const PerformanceCounterDescriptor &d,
                          PerformanceCounter *c) {
              c->first  += counter_it->first;
              c->second += counter_it->second;
              counter_it++;
            };

        ceph_assert(key_it.second.size() >= data[query][key].size());
        query.update_counters(update_counter_fnc, &data[query][key]);
      }
    }
  }

  void set_queries(const std::list<OSDPerfMetricQuery> &queries) {
    std::map<OSDPerfMetricQuery,
             std::map<OSDPerfMetricKey, PerformanceCounters>> new_data;
    for (auto &query : queries) {
      std::swap(new_data[query], data[query]);
    }
    std::swap(data, new_data);
  }

  bool is_enabled() {
    return !data.empty();
  }

  void add(const OpRequest& op, uint64_t inb, uint64_t outb,
           const utime_t &latency) {

    auto update_counter_fnc =
        [&op, inb, outb, &latency](const PerformanceCounterDescriptor &d,
                                   PerformanceCounter *c) {
          ceph_assert(d.is_supported());

          switch(d.type) {
          case PerformanceCounterType::WRITE_OPS:
            if (op.may_write() || op.may_cache()) {
              c->first++;
            }
            return;
          case PerformanceCounterType::READ_OPS:
            if (op.may_read()) {
              c->first++;
            }
            return;
          case PerformanceCounterType::WRITE_BYTES:
            if (op.may_write() || op.may_cache()) {
              c->first += inb;
              c->second++;
            }
            return;
          case PerformanceCounterType::READ_BYTES:
            if (op.may_read()) {
              c->first += outb;
              c->second++;
            }
            return;
          case PerformanceCounterType::WRITE_LATENCY:
            if (op.may_write() || op.may_cache()) {
              c->first += latency.to_nsec();
              c->second++;
            }
            return;
          case PerformanceCounterType::READ_LATENCY:
            if (op.may_read()) {
              c->first += latency.to_nsec();
              c->second++;
            }
            return;
          default:
            ceph_abort_msg("unknown counter type");
          }
        };

    auto get_subkey_fnc =
        [&op](const OSDPerfMetricSubKeyDescriptor &d,
              OSDPerfMetricSubKey *sub_key) {
          ceph_assert(d.is_supported());

          auto m = static_cast<const MOSDOp*>(op.get_req());
          std::string match_string;
          switch(d.type) {
          case OSDPerfMetricSubKeyType::CLIENT_ID:
            match_string = stringify(m->get_reqid().name);
            break;
          case OSDPerfMetricSubKeyType::POOL_ID:
            match_string = stringify(m->get_spg().pool());
            break;
          case OSDPerfMetricSubKeyType::OBJECT_NAME:
            match_string = m->get_oid().name;
            break;
          default:
            ceph_abort_msg("unknown counter type");
          }

          std::smatch match;
          if (!std::regex_search(match_string, match, d.regex)) {
            return false;
          }
          for (auto &sub_match : match) {
            sub_key->push_back(sub_match.str());
          }
          return true;
        };

    for (auto &it : data) {
      auto &query = it.first;
      OSDPerfMetricKey key;
      if (query.get_key(get_subkey_fnc, &key)) {
        query.update_counters(update_counter_fnc, &it.second[key]);
      }
    }
  }

  void add_to_reports(
      std::map<OSDPerfMetricQuery, OSDPerfMetricReport> *reports) {
    for (auto &it : data) {
      auto &query = it.first;
      auto &report = (*reports)[query];

      query.get_performance_counter_descriptors(
          &report.performance_counter_descriptors);

      for (auto &it_counters : it.second) {
        auto &bl = report.group_packed_performance_counters[it_counters.first];
        query.pack_counters(it_counters.second, &bl);
      }
    }
  }

private:
  std::map<OSDPerfMetricQuery,
           std::map<OSDPerfMetricKey, PerformanceCounters>> data;
};

#endif // DYNAMIC_PERF_STATS_H
