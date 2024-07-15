# 1\. 源码跟踪

## 1.1 get_cluster_status
[https://github.com/ceph/ceph/blob/2a724a2ff313701fd7f6278ce8ed7f440bb355e0/src/mon/Monitor.cc](https://github.com/ceph/ceph/blob/2a724a2ff313701fd7f6278ce8ed7f440bb355e0/src/mon/Monitor.cc)

ceph -s 会执行get_cluster_status函数， 在函数的最后会调用打印
mgrstatmon()->print_summary函数
```c/c++
void Monitor::get_cluster_status(stringstream &ss, Formatter *f)
{
  if (f)
    f->open_object_section("status");
  if (f) {
    f->dump_stream("fsid") << monmap->get_fsid();
    get_health_status(false, f, nullptr);
    f->dump_unsigned("election_epoch", get_epoch());
    {
      f->open_array_section("quorum");
      for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    f->dump_int("rank", *p);
      f->close_section();
      f->open_array_section("quorum_names");
      for (set<int>::iterator p = quorum.begin(); p != quorum.end(); ++p)
    f->dump_string("id", monmap->get_name(*p));
      f->close_section();
    }
    f->open_object_section("monmap");
    monmap->dump(f);
    f->close_section();
    f->open_object_section("osdmap");
    osdmon()->osdmap.print_summary(f, cout, string(12, ' '));
    f->close_section();
    f->open_object_section("pgmap");
    mgrstatmon()->print_summary(f, NULL);
    f->close_section();
    f->open_object_section("fsmap");
    mdsmon()->get_fsmap().print_summary(f, NULL);
    f->close_section();
    f->open_object_section("mgrmap");
    mgrmon()->get_map().print_summary(f, nullptr);
    f->close_section();
    f->dump_object("servicemap", mgrstatmon()->get_service_map());
    f->close_section();
  } else {
    ss << "  cluster:\n";
    ss << "    id:     " << monmap->get_fsid() << "\n";
    string health;
    get_health_status(false, nullptr, &health,
              "\n            ", "\n            ");
    ss << "    health: " << health << "\n";
    ss << "\n \n  services:\n";
    {
      size_t maxlen = 3;
      auto& service_map = mgrstatmon()->get_service_map();
      for (auto& p : service_map.services) {
    maxlen = std::max(maxlen, p.first.size());
      }
      string spacing(maxlen - 3, ' ');
      const auto quorum_names = get_quorum_names();
      const auto mon_count = monmap->mon_info.size();
      ss << "    mon: " << spacing << mon_count << " daemons, quorum "
     << quorum_names;
      if (quorum_names.size() != mon_count) {
    std::list<std::string> out_of_q;
    for (size_t i = 0; i < monmap->ranks.size(); ++i) {
      if (quorum.count(i) == 0) {
        out_of_q.push_back(monmap->ranks[i]);
      }
    }
    ss << ", out of quorum: " << joinify(out_of_q.begin(),
                         out_of_q.end(), std::string(", "));
      }
      ss << "\n";
      if (mgrmon()->in_use()) {
    ss << "    mgr: " << spacing;
    mgrmon()->get_map().print_summary(nullptr, &ss);
    ss << "\n";
      }
      if (mdsmon()->get_fsmap().filesystem_count() > 0) {
    ss << "    mds: " << spacing << mdsmon()->get_fsmap() << "\n";
      }
      ss << "    osd: " << spacing;
      osdmon()->osdmap.print_summary(NULL, ss, string(maxlen + 6, ' '));
      ss << "\n";
      for (auto& p : service_map.services) {
    ss << "    " << p.first << ": " << string(maxlen - p.first.size(), ' ')
       << p.second.get_summary() << "\n";
      }
    }
    ss << "\n \n  data:\n";
    mgrstatmon()->print_summary(NULL, &ss);
    ss << "\n ";
  }
}
```
## 1.2 mgrstatmon()->print_summary()

[https://github.com/ceph/ceph/blob/6c9d937828910664032fd58959a2ae8dd8678588/src/mon/PGMap.cc](https://github.com/ceph/ceph/blob/6c9d937828910664032fd58959a2ae8dd8678588/src/mon/PGMap.cc)
print_summary函数进行计算，统计相关指标
```c/c++
void PGMapDigest::print_summary(Formatter *f, ostream *out) const
{
  if (f)
    f->open_array_section("pgs_by_state");
  // list is descending numeric order (by count)
  multimap<int,int> state_by_count;  // count -> state
  for (auto p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    state_by_count.insert(make_pair(p->second, p->first));
  }
  if (f) {
    for (auto p = state_by_count.rbegin();
         p != state_by_count.rend();
         ++p)
    {
      f->open_object_section("pgs_by_state_element");
      f->dump_string("state_name", pg_state_string(p->second));
      f->dump_unsigned("count", p->first);
      f->close_section();
    }
  }
  if (f)
    f->close_section();
  if (f) {
    f->dump_unsigned("num_pgs", num_pg);
    f->dump_unsigned("num_pools", pg_pool_sum.size());
    f->dump_unsigned("num_objects", pg_sum.stats.sum.num_objects);
    f->dump_unsigned("data_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_unsigned("bytes_used", osd_sum.kb_used * 1024ull);
    f->dump_unsigned("bytes_avail", osd_sum.kb_avail * 1024ull);
    f->dump_unsigned("bytes_total", osd_sum.kb * 1024ull);
  } else {
    *out << "    pools:   " << pg_pool_sum.size() << " pools, "
         << num_pg << " pgs\n";
    *out << "    objects: " << si_u_t(pg_sum.stats.sum.num_objects) << " objects, "
         << byte_u_t(pg_sum.stats.sum.num_bytes) << "\n";
    *out << "    usage:   "
         << byte_u_t(osd_sum.kb_used << 10) << " used, "
         << byte_u_t(osd_sum.kb_avail << 10) << " / "
         << byte_u_t(osd_sum.kb << 10) << " avail\n";
    *out << "    pgs:     ";
  }
  bool pad = false;
  if (num_pg_unknown > 0) {
    float p = (float)num_pg_unknown / (float)num_pg;
    if (f) {
      f->dump_float("unknown_pgs_ratio", p);
    } else {
      char b[20];
      snprintf(b, sizeof(b), "%.3lf", p * 100.0);
      *out << b << "% pgs unknown\n";
      pad = true;
    }
  }
  int num_pg_inactive = num_pg - num_pg_active - num_pg_unknown;
  if (num_pg_inactive > 0) {
    float p = (float)num_pg_inactive / (float)num_pg;
    if (f) {
      f->dump_float("inactive_pgs_ratio", p);
    } else {
      if (pad) {
        *out << "             ";
      }
      char b[20];
      snprintf(b, sizeof(b), "%.3f", p * 100.0);
      *out << b << "% pgs not active\n";
      pad = true;
    }
  }
  list<string> sl;
  overall_recovery_summary(f, &sl);
  if (!f && !sl.empty()) {
    for (auto p = sl.begin(); p != sl.end(); ++p) {
      if (pad) {
        *out << "             ";
      }
      *out << *p << "\n";
      pad = true;
    }
  }
  sl.clear();
  if (!f) {
    unsigned max_width = 1;
    for (multimap<int,int>::reverse_iterator p = state_by_count.rbegin();
         p != state_by_count.rend();
         ++p)
    {
      std::stringstream ss;
      ss << p->first;
      max_width = std::max<size_t>(ss.str().size(), max_width);
    }
    for (multimap<int,int>::reverse_iterator p = state_by_count.rbegin();
         p != state_by_count.rend();
         ++p)
    {
      if (pad) {
        *out << "             ";
      }
      pad = true;
      out->setf(std::ios::left);
      *out << std::setw(max_width) << p->first
           << " " << pg_state_string(p->second) << "\n";
      out->unsetf(std::ios::left);
    }
  }
  ostringstream ss_rec_io;
  overall_recovery_rate_summary(f, &ss_rec_io);
  ostringstream ss_client_io;
  overall_client_io_rate_summary(f, &ss_client_io);
  ostringstream ss_cache_io;
  overall_cache_io_rate_summary(f, &ss_cache_io);
  if (!f && (ss_client_io.str().length() || ss_rec_io.str().length()
             || ss_cache_io.str().length())) {
    *out << "\n \n";
    *out << "  io:\n";
  }
  if (!f && ss_client_io.str().length())
    *out << "    client:   " << ss_client_io.str() << "\n";
  if (!f && ss_rec_io.str().length())
    *out << "    recovery: " << ss_rec_io.str() << "\n";
  if (!f && ss_cache_io.str().length())
    *out << "    cache:    " << ss_cache_io.str() << "\n";
}
```
## 1.3 ss_client_io
```c/c++
void PGMapDigest::overall_client_io_rate_summary(Formatter *f, ostream *out) const
{
  client_io_rate_summary(f, out, pg_sum_delta, stamp_delta);
}
void PGMapDigest::pool_client_io_rate_summary(Formatter *f, ostream *out,
                                        uint64_t poolid) const
{
  auto p = per_pool_sum_delta.find(poolid);
  if (p == per_pool_sum_delta.end())
    return;
  auto ts = per_pool_sum_deltas_stamps.find(p->first);
  assert(ts != per_pool_sum_deltas_stamps.end());
  client_io_rate_summary(f, out, p->second.first, ts->second);
}
```
