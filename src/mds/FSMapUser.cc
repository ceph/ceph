#include "FSMapUser.h"

void FSMapUser::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(epoch, bl);
  encode(legacy_client_fscid, bl);
  std::vector<fs_info_t> fs_list;
  for (auto p = filesystems.begin(); p != filesystems.end(); ++p)
    fs_list.push_back(p->second);
  encode(fs_list, bl, features);
  ENCODE_FINISH(bl);
}

void FSMapUser::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(epoch, p);
  decode(legacy_client_fscid, p);
  std::vector<fs_info_t> fs_list;
  decode(fs_list, p);
  filesystems.clear();
  for (auto p = fs_list.begin(); p != fs_list.end(); ++p)
    filesystems[p->cid] = *p;
  DECODE_FINISH(p);
}

void FSMapUser::fs_info_t::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(cid, bl);
  encode(name, bl);
  ENCODE_FINISH(bl);
}

void FSMapUser::fs_info_t::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(cid, p);
  decode(name, p);
  DECODE_FINISH(p);
}

void FSMapUser::generate_test_instances(std::list<FSMapUser*>& ls)
{
  FSMapUser *m = new FSMapUser();
  m->epoch = 2;
  m->legacy_client_fscid = 1;
  m->filesystems[1].cid = 1;
  m->filesystems[2].name = "cephfs2";
  m->filesystems[2].cid = 2;
  m->filesystems[1].name = "cephfs1";
  ls.push_back(m);
}


void FSMapUser::print(ostream& out) const
{
  out << "e" << epoch << std::endl;
  out << "legacy_client_fscid: " << legacy_client_fscid << std::endl;
  for (auto &p : filesystems)
    out << " id " <<  p.second.cid << " name " << p.second.name << std::endl;
}

void FSMapUser::print_summary(Formatter *f, ostream *out)
{
  map<mds_role_t,string> by_rank;
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    for (auto &p : filesystems) {
      f->dump_unsigned("id", p.second.cid);
      f->dump_string("name", p.second.name);
    }
  } else {
    *out << "e" << get_epoch() << ":";
    for (auto &p : filesystems)
      *out << " " << p.second.name << "(" << p.second.cid << ")";
  }
}
