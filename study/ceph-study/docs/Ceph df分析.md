# 1. 查看集群使用容量
```shell
ceph df
GLOBAL:
    SIZE       AVAIL      RAW USED     %RAW USED
    61284G     26383G       34901G         56.95
POOLS:
    NAME                ID     USED       %USED     MAX AVAIL     OBJECTS
    rbd                 0        256k         0         6931G           4
    cephfs_metadata     1      43928k         0         6931G     1043729
    cephfs_data         2      11603G     62.60         6931G     7083637
```
**说明：**
ceph对容量的计算分为两个维度：
 - GLOBAL维度中有SIZE，AVAIL，RAW USED，%RAW USED
 - POOLS的维度中有 USED，%USED，MAX AVAIL，OBJECTS

GLOBAL中的RAW USED ：34901G,      AVAIL：26383G
POOLS 中USED：11603G*3 + (43928k/1024/1024)*3 = 34809.123G   MAX AVAIL：20793G
发现问题没，pools使用的跟global有偏差，少了一部分数据。

# 2. 分析mon源码
分析/src/mon/[Monitor.cc](http://monitor.cc/)代码，跟踪df逻辑如下：
![image.png](https://upload-images.jianshu.io/upload_images/2099201-e76df3042a136dca.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
从上面的代码可以知道，df命令的输出两个维度代码逻辑：
 - GLOBAL维度pgmon()->dump_fs_stats
 - POOLS这个维度pgmon()->dump_pool_stats

## 2.1 GLOBAL维度
```c/c++
void PGMapDigest::dump_fs_stats(stringstream *ss, Formatter *f, bool verbose) const
{
  if (f) {
    f->open_object_section("stats");
    f->dump_int("total_bytes", osd_sum.kb * 1024ull);
    f->dump_int("total_used_bytes", osd_sum.kb_used * 1024ull);
    f->dump_int("total_avail_bytes", osd_sum.kb_avail * 1024ull);
 
    if (verbose) {
      f->dump_int("total_objects", pg_sum.stats.sum.num_objects);
    }
 
    f->close_section();
  } else {
    assert(ss != nullptr);
 
    TextTable tbl;
    tbl.define_column("SIZE", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("RAW USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("%RAW USED", TextTable::LEFT, TextTable::RIGHT);
 
    if (verbose) {
      tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    }
    tbl << stringify(byte_u_t(osd_sum.kb*1024))
 
        << stringify(byte_u_t(osd_sum.kb_avail*1024))
 
        << stringify(byte_u_t(osd_sum.kb_used*1024));
 
    float used = 0.0;
    if (osd_sum.kb > 0) {
      used = ((float)osd_sum.kb_used / osd_sum.kb);
    }
    tbl << percentify(used*100);
    if (verbose) {
      tbl << stringify(si_u_t(pg_sum.stats.sum.num_objects));
    }
    tbl << TextTable::endrow;
    *ss << "GLOBAL:\n";
    tbl.set_indent(4);
    *ss << tbl;
  }
}
 
void PGMap::calc_stats()
{
  num_pg = 0;
  num_pg_active = 0;
  num_pg_unknown = 0;
  num_osd = 0;
  pg_pool_sum.clear();
  num_pg_by_pool.clear();
  pg_by_osd.clear();
  pg_sum = pool_stat_t();
  osd_sum = osd_stat_t();
  num_pg_by_state.clear();
  num_pg_by_osd.clear();
  for (auto p = pg_stat.begin();
       p != pg_stat.end();
       ++p) {
    stat_pg_add(p->first, p->second);
  }
  for (auto p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    stat_osd_add(p->first, p->second);
}
```
**说明：**
计算的数值输出主要依赖pg_map.osd_sum的值，而osd_sum就是osd_stat_t。

### 2.1.1 osd_stat_t分析
```c/c++
void OSDService::update_osd_stat(vector<int>& hb_peers)
{
  // load osd stats first
  struct store_statfs_t stbuf;
  int r = osd->store->statfs(&stbuf);
  if (r < 0) {
    derr << "statfs() failed: " << cpp_strerror(r) << dendl;
    return;
  }
  auto new_stat = set_osd_stat(stbuf, hb_peers, osd->num_pgs);
  dout(20) << "update_osd_stat " << new_stat << dendl;
  assert(new_stat.kb);
  float ratio = ((float)new_stat.kb_used) / ((float)new_stat.kb);
  check_full_status(ratio);
}
```
**说明：**
从上面我们可以看到update_osd_stat 主要是通过osd->store->statfs(&stbuf)，来更新osd_stat的
因为这里使用的是Filestore，所以需要进入FileStore看其是如何statfs的。

### 2.1.2 FileStore statfs分析
```c/c++
int KStore::statfs(struct store_statfs_t* buf0)
{
  struct statfs buf;
  buf0->reset();
  if (::statfs(basedir.c_str(), &buf) < 0) {
    int r = -errno;
    assert(r != -ENOENT);
    return r;
  }
  buf0->total = buf.f_blocks * buf.f_bsize;
  buf0->available = buf.f_bavail * buf.f_bsize;
  return 0;
}
```
**说明：**
可以看到上面FileStore主要是通过::statfs()这个系统调用来获取信息的，basedir.c_str()就是data目录。
所以osd_sum计算的就是将所有osd 数据目录的磁盘使用量加起来。会把该磁盘上的其它目录也算到Raw Used中。
这个统计和我们传统意义上的磁盘空间使用是一致的，比较准确地反应出了所有OSD的文件系统的 总体使用量和总体剩余空间。

## 2.2 POOLS维度
### 2.2.1 分析dump_pool_stats
```c/c++
void PGMapDigest::dump_pool_stats_full(
  const OSDMap &osd_map,
  stringstream *ss,
  Formatter *f,
  bool verbose) const
{
  TextTable tbl;
  if (f) {
    f->open_array_section("pools");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("ID", TextTable::LEFT, TextTable::LEFT);
    if (verbose) {
      tbl.define_column("QUOTA OBJECTS", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("QUOTA BYTES", TextTable::LEFT, TextTable::LEFT);
    }
    tbl.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("%USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("MAX AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("DIRTY", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("READ", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("WRITE", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("RAW USED", TextTable::LEFT, TextTable::RIGHT);
    }
  }
  map<int,uint64_t> avail_by_rule;
  for (auto p = osd_map.get_pools().begin();
       p != osd_map.get_pools().end(); ++p) {
    int64_t pool_id = p->first;
    if ((pool_id < 0) || (pg_pool_sum.count(pool_id) == 0))
      continue;
    const string& pool_name = osd_map.get_pool_name(pool_id);
    const pool_stat_t &stat = pg_pool_sum.at(pool_id);
    const pg_pool_t *pool = osd_map.get_pg_pool(pool_id);
    int ruleno = osd_map.crush->find_rule(pool->get_crush_rule(),
                                         pool->get_type(),
                                         pool->get_size());
    int64_t avail;
    float raw_used_rate;
    if (avail_by_rule.count(ruleno) == 0) {
      // FIXME: we don't guarantee avail_space_by_rule is up-to-date before this function is invoked
      avail = get_rule_avail(ruleno);
      if (avail < 0)
    avail = 0;
      avail_by_rule[ruleno] = avail;
    } else {
      avail = avail_by_rule[ruleno];
    }
    raw_used_rate = ::pool_raw_used_rate(osd_map, pool_id);
    if (f) {
      f->open_object_section("pool");
      f->dump_string("name", pool_name);
      f->dump_int("id", pool_id);
      f->open_object_section("stats");
    } else {
      tbl << pool_name
          << pool_id;
      if (verbose) {
        if (pool->quota_max_objects == 0)
          tbl << "N/A";
        else
          tbl << si_t(pool->quota_max_objects);
        if (pool->quota_max_bytes == 0)
          tbl << "N/A";
        else
          tbl << si_t(pool->quota_max_bytes);
      }
    }
    dump_object_stat_sum(tbl, f, stat.stats.sum, avail, raw_used_rate, verbose, pool);
    if (f)
      f->close_section();  // stats
    else
      tbl << TextTable::endrow;
    if (f)
      f->close_section();  // pool
  }
  if (f)
    f->close_section();
  else {
    assert(ss != nullptr);
    *ss << "POOLS:\n";
    tbl.set_indent(4);
    *ss << tbl;
  }
}
```
其中中间输出数据部分是dump_object_stat_sum .

### 2.2.2 dump_object_stat_sum
```c/c++
void PGMapDigest::dump_object_stat_sum(
  TextTable &tbl, Formatter *f,
  const object_stat_sum_t &sum, uint64_t avail,
  float raw_used_rate, bool verbose,
  const pg_pool_t *pool)
{
  float curr_object_copies_rate = 0.0;
  if (sum.num_object_copies > 0)
    curr_object_copies_rate = (float)(sum.num_object_copies - sum.num_objects_degraded) / sum.num_object_copies;
  float used = 0.0;
  // note avail passed in is raw_avail, calc raw_used here.
  if (avail) {
    used = sum.num_bytes * raw_used_rate * curr_object_copies_rate;
    used /= used + avail;
  } else if (sum.num_bytes) {
    used = 1.0;
  }
  if (f) {
    f->dump_int("kb_used", SHIFT_ROUND_UP(sum.num_bytes, 10));
    f->dump_int("bytes_used", sum.num_bytes);
    f->dump_format_unquoted("percent_used", "%.2f", (used*100));
    f->dump_unsigned("max_avail", avail / raw_used_rate);
    f->dump_int("objects", sum.num_objects);
    if (verbose) {
      f->dump_int("quota_objects", pool->quota_max_objects);
      f->dump_int("quota_bytes", pool->quota_max_bytes);
      f->dump_int("dirty", sum.num_objects_dirty);
      f->dump_int("rd", sum.num_rd);
      f->dump_int("rd_bytes", sum.num_rd_kb * 1024ull);
      f->dump_int("wr", sum.num_wr);
      f->dump_int("wr_bytes", sum.num_wr_kb * 1024ull);
      f->dump_int("raw_bytes_used", sum.num_bytes * raw_used_rate * curr_object_copies_rate);
    }
  } else {
    tbl << stringify(si_t(sum.num_bytes));
    tbl << percentify(used*100);
    tbl << si_t(avail / raw_used_rate);
    tbl << sum.num_objects;
    if (verbose) {
      tbl << stringify(si_t(sum.num_objects_dirty))
          << stringify(si_t(sum.num_rd))
          << stringify(si_t(sum.num_wr))
          << stringify(si_t(sum.num_bytes * raw_used_rate * curr_object_copies_rate));
    }
  }
}
```
pool的使用空间（USED）是通过osd来更新的，因为有update（write，truncate，delete等）操作的的时候，会更新ctx->delta_stats，具体请见ReplicatedPG::do_osd_ops。举例的话，可以从处理WRITE的op为入手点，当处理CEPH_OSD_OP_WRITE类型的op的时候，会调用write_update_size_and_usage()。里面会更新ctx->delta_stats。当IO处理完，也就是applied和commited之后，会publish_stats_to_osd()。

# 3. 总结
 - GLOBAL维度：osd的所在磁盘的statfs来计算所以还是比较准确的。
 - POOLS维度： 由于需要考虑到POOL的副本策略，CRUSH RULE，OSD WEIGHT，计算起来还是比较复杂的。
容量的管理主要是在OSD端，且OSD会把信息传递给MON，让MON来维护.


**说明：**
pool的使用空间（USED）是通过osd来更新的，因为有update（write，truncate，delete等）操作的的时候，会更新ctx->delta_stats，具体请见ReplicatedPG::do_osd_ops。举例的话，可以从处理WRITE的op为入手点，当处理CEPH_OSD_OP_WRITE类型的op的时候，会调用write_update_size_and_usage()。里面会更新ctx->delta_stats。当IO处理完，也就是applied和commited之后，会publish_stats_to_osd()

