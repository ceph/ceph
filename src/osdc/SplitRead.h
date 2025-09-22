#pragma once

class SplitRead {

 protected:
  struct bl_split_merge {
    ceph::buffer::list split(
        uint64_t offset,
        uint64_t length,
        ceph::buffer::list &bl) const {
      ceph::buffer::list out;
      out.substr_of(bl, offset, length);
      return out;
    }

    bool can_merge(const ceph::buffer::list &left, const ceph::buffer::list &right) const {
      return true;
    }

    ceph::buffer::list merge(ceph::buffer::list &&left, ceph::buffer::list &&right) const {
      ceph::buffer::list bl{std::move(left)};
      bl.claim_append(right);
      return bl;
    }

    uint64_t length(const ceph::buffer::list &b) const { return b.length(); }
  };

  using extent = std::pair<uint64_t, uint64_t>;
  using extents_map = std::map<uint64_t, uint64_t>;
  using extent_set = interval_set<uint64_t, std::map, false>;
  using extent_map = interval_map<uint64_t, ceph::buffer::list, bl_split_merge,
                                  std::map, true>;

  struct Details {
    bufferlist bl;
    int rval;
    boost::system::error_code ec;
    std::optional<extents_map> e;
  };

  struct SubRead {
    ::ObjectOperation rd;
    mini_flat_map<int, Details> details;
    int rc = -EIO;

    SubRead(int count) : details(count) {}
  };

  // This structure self-destructs on each IO completions, using a legacy
  // C++ pattern (no shared_ptr). We use the finish callback to record the
  // RC, but otherwise rely on the shared_ptr destroying ec_read to deal with
  // completion of the parent IO.
  struct Finisher : Context {
    std::shared_ptr<SplitRead> split_read;
    SubRead &sub_read;

    Finisher(std::shared_ptr<SplitRead> split_read, SubRead &sub_read) : split_read(split_read), sub_read(sub_read) {}
    void finish(int r) override {
      sub_read.rc = r;
    }
  };

  void assemble_sparse_buffer(OSDOp &out_osd_op, int ops_index);
  int assemble_rc();
  virtual void assemble_buffer(bufferlist *bl_out, int ops_index) = 0;
  virtual void init_read(OSDOp &op, bool sparse, int ops_index) = 0;
  void init(OSDOp &op, int ops_index);

  Objecter::Op *orig_op;
  Objecter &objecter;
  mini_flat_map<shard_id_t, SubRead> sub_reads;
  CephContext *cct;
  bool abort = false; // Last minute abort... We want to keep this to a minimum.
  int flags = 0;
  std::optional<shard_id_t> primary_shard;
  std::map<shard_id_t, std::vector<int>> op_offset_map;

 public:
  SplitRead(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count) : orig_op(op), objecter(objecter), sub_reads(count), cct(cct) {}
  virtual ~SplitRead() = default;
  void complete();
  static bool create(Objecter::Op *op, Objecter &objecter,
    shunique_lock<ceph::shared_mutex>& sul, ceph_tid_t *ptid, int *ctx_budget, CephContext *cct);
};

class ECSplitRead : public SplitRead{
 public:
  using SplitRead::SplitRead;
  void assemble_buffer(bufferlist *bl_out, int ops_index) override;
  void init_read(OSDOp &op, bool sparse, int ops_index) override;
  ECSplitRead(Objecter::Op *op, Objecter &objecter, CephContext *cct, int count);
  ~ECSplitRead() {
    complete();
  }
};

class ReplicaSplitRead : public SplitRead {
 public:
  using SplitRead::SplitRead;
  void assemble_buffer(bufferlist *bl_out, int ops_index) override;
  void init_read(OSDOp &op, bool sparse, int ops_index) override;
  ReplicaSplitRead(Objecter::Op *op, Objecter &objecter, CephContext *cct, int pool_size);
  ~ReplicaSplitRead() {
    complete();
  }
};

