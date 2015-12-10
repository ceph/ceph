// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_MIRRORENV_H
#define CEPH_OS_BLUESTORE_MIRRORENV_H

#include <memory>
#include <string>

#include "rocksdb/status.h"
#include "rocksdb/env.h"

namespace rocksdb {

  class MirrorSequentialFile : public SequentialFile {
  public:
    unique_ptr<SequentialFile> a_, b_;
    string fname;
    MirrorSequentialFile(string f) : fname(f) {}

    Status Read(size_t n, Slice* result, char* scratch) {
      Slice aslice;
      Status as = a_->Read(n, &aslice, scratch);
      if (as == Status::OK()) {
	char bscratch[n];
	Slice bslice;
	unsigned off = 0, left = result->size();
	while (left) {
	  Status bs = b_->Read(left, &bslice, bscratch);
	  assert(as == bs);
	  assert(memcmp(bscratch, scratch + off, bslice.size()) == 0);
	  off += bslice.size();
	  left -= bslice.size();
	}
	*result = aslice;
      } else {
	Status bs = b_->Read(n, result, scratch);
	assert(as == bs);
      }
      return as;
    }

    Status Skip(uint64_t n) {
      Status as = a_->Skip(n);
      Status bs = b_->Skip(n);
      assert(as == bs);
      return as;
    }
    Status InvalidateCache(size_t offset, size_t length) {
      Status as = a_->InvalidateCache(offset, length);
      Status bs = b_->InvalidateCache(offset, length);
      assert(as == bs);
      return as;
    };
  };

  class MirrorRandomAccessFile : public RandomAccessFile {
  public:
    unique_ptr<RandomAccessFile> a_, b_;
    string fname;
    MirrorRandomAccessFile(string f) : fname(f) {}

    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
      Status as = a_->Read(offset, n, result, scratch);
      if (as == Status::OK()) {
	char bscratch[n];
	Slice bslice;
	unsigned off = 0, left = result->size();
	while (left) {
	  Status bs = b_->Read(offset + off, left, &bslice, bscratch);
	  assert(as == bs);
	  assert(memcmp(bscratch, scratch + off, bslice.size()) == 0);
	  off += bslice.size();
	  left -= bslice.size();
	}
      } else {
	Status bs = b_->Read(offset, n, result, scratch);
	assert(as == bs);
      }
      return as;
    }

    bool ShouldForwardRawRequest() const {
      // FIXME: not verified
      return a_->ShouldForwardRawRequest();
    }

    size_t GetUniqueId(char* id, size_t max_size) const {
      // FIXME: not verified
      return a_->GetUniqueId(id, max_size);
    }
  };

  class MirrorWritableFile : public WritableFile {
  public:
    unique_ptr<WritableFile> a_, b_;
    string fname;
    MirrorWritableFile(string f) : fname(f) {}

    Status Append(const Slice& data) override {
      Status as = a_->Append(data);
      Status bs = b_->Append(data);
      assert(as == bs);
      return as;
    }
    Status PositionedAppend(const Slice& data, uint64_t offset) override {
      Status as = a_->PositionedAppend(data, offset);
      Status bs = b_->PositionedAppend(data, offset);
      assert(as == bs);
      return as;
    }
    Status Truncate(uint64_t size) override {
      Status as = a_->Truncate(size);
      Status bs = b_->Truncate(size);
      assert(as == bs);
      return as;
    }
    Status Close() override {
      Status as = a_->Close();
      Status bs = b_->Close();
      assert(as == bs);
      return as;
    }
    Status Flush() override {
      Status as = a_->Flush();
      Status bs = b_->Flush();
      assert(as == bs);
      return as;
    }
    Status Sync() override {
      Status as = a_->Sync();
      Status bs = b_->Sync();
      assert(as == bs);
      return as;
    }
    Status Fsync() override {
      Status as = a_->Fsync();
      Status bs = b_->Fsync();
      assert(as == bs);
      return as;
    }
    bool IsSyncThreadSafe() const override {
      bool as = a_->IsSyncThreadSafe();
      bool bs = b_->IsSyncThreadSafe();
      assert(as == bs);
      return as;
    }
    void SetIOPriority(Env::IOPriority pri) override {
      a_->SetIOPriority(pri);
      b_->SetIOPriority(pri);
    }
    Env::IOPriority GetIOPriority() override {
      // FIXME: we don't verify this one
      return a_->GetIOPriority();
    }
    uint64_t GetFileSize() override {
      uint64_t as = a_->GetFileSize();
      uint64_t bs = b_->GetFileSize();
      assert(as == bs);
      return as;
    }
    void GetPreallocationStatus(size_t* block_size,
				size_t* last_allocated_block) override {
      // FIXME: we don't verify this one
      return a_->GetPreallocationStatus(block_size, last_allocated_block);
    }
    size_t GetUniqueId(char* id, size_t max_size) const override {
      // FIXME: we don't verify this one
      return a_->GetUniqueId(id, max_size);
    }
    Status InvalidateCache(size_t offset, size_t length) override {
      Status as = a_->InvalidateCache(offset, length);
      Status bs = b_->InvalidateCache(offset, length);
      assert(as == bs);
      return as;
    }

  protected:
    Status Allocate(off_t offset, off_t length) override {
      Status as = a_->Allocate(offset, length);
      Status bs = b_->Allocate(offset, length);
      assert(as == bs);
      return as;
    }
    Status RangeSync(off_t offset, off_t nbytes) override {
      Status as = a_->RangeSync(offset, nbytes);
      Status bs = b_->RangeSync(offset, nbytes);
      assert(as == bs);
      return as;
    }
  };

  class MirrorEnv : public EnvWrapper {
    Env *a_, *b_;
  public:
    MirrorEnv(Env *a, Env *b) : EnvWrapper(a), a_(a), b_(b) {}

    // The following text is boilerplate that forwards all methods to target()
    Status NewSequentialFile(const std::string& f, unique_ptr<SequentialFile>* r,
			     const EnvOptions& options) override {
      if (f[0] == '/')
	return a_->NewSequentialFile(f, r, options);
      MirrorSequentialFile *mf = new MirrorSequentialFile(f);
      r->reset(mf);
      Status as = a_->NewSequentialFile(f, &mf->a_, options);
      Status bs = b_->NewSequentialFile(f, &mf->b_, options);
      assert(as == bs);
      return as;
    }
    Status NewRandomAccessFile(const std::string& f,
			       unique_ptr<RandomAccessFile>* r,
			       const EnvOptions& options) override {
      if (f[0] == '/')
	return a_->NewRandomAccessFile(f, r, options);
      MirrorRandomAccessFile *mf = new MirrorRandomAccessFile(f);
      r->reset(mf);
      Status as = a_->NewRandomAccessFile(f, &mf->a_, options);
      Status bs = b_->NewRandomAccessFile(f, &mf->b_, options);
      assert(as == bs);
      return as;
    }
    Status NewWritableFile(const std::string& f, unique_ptr<WritableFile>* r,
			   const EnvOptions& options) override {
      if (f[0] == '/')
	return a_->NewWritableFile(f, r, options);
      MirrorWritableFile *mf = new MirrorWritableFile(f);
      r->reset(mf);
      Status as = a_->NewWritableFile(f, &mf->a_, options);
      Status bs = b_->NewWritableFile(f, &mf->b_, options);
      assert(as == bs);
      return as;
    }
    virtual Status NewDirectory(const std::string& name,
				unique_ptr<Directory>* result) override {
      unique_ptr<Directory> br;
      Status as = a_->NewDirectory(name, result);
      Status bs = b_->NewDirectory(name, &br);
      assert(as == bs);
      return as;
    }
    Status FileExists(const std::string& f) override {
      Status as = a_->FileExists(f);
      Status bs = b_->FileExists(f);
      assert(as == bs);
      return as;
    }
    Status GetChildren(const std::string& dir,
		       std::vector<std::string>* r) override {
      std::vector<std::string> ar, br;
      Status as = a_->GetChildren(dir, &ar);
      Status bs = b_->GetChildren(dir, &br);
      assert(as == bs);
      std::sort(ar.begin(), ar.end());
      std::sort(br.begin(), br.end());
      if (ar != br) {
	std::cout << "a: " << ar << std::endl;
	std::cout << "b: " << br << std::endl;
	assert(0 == "getchildren results don't match");
      }
      *r = ar;
      return as;
    }
    Status DeleteFile(const std::string& f) override {
      Status as = a_->DeleteFile(f);
      Status bs = b_->DeleteFile(f);
      assert(as == bs);
      return as;
    }
    Status CreateDir(const std::string& d) override {
      Status as = a_->CreateDir(d);
      Status bs = b_->CreateDir(d);
      assert(as == bs);
      return as;
    }
    Status CreateDirIfMissing(const std::string& d) override {
      Status as = a_->CreateDirIfMissing(d);
      Status bs = b_->CreateDirIfMissing(d);
      assert(as == bs);
      return as;
    }
    Status DeleteDir(const std::string& d) override {
      Status as = a_->DeleteDir(d);
      Status bs = b_->DeleteDir(d);
      assert(as == bs);
      return as;
    }
    Status GetFileSize(const std::string& f, uint64_t* s) override {
      uint64_t asize, bsize;
      Status as = a_->GetFileSize(f, &asize);
      Status bs = b_->GetFileSize(f, &bsize);
      assert(as == bs);
      assert(asize == bsize);
      *s = asize;
      return as;
    }

    Status GetFileModificationTime(const std::string& fname,
				   uint64_t* file_mtime) override {
      uint64_t amtime, bmtime;
      Status as = a_->GetFileModificationTime(fname, &amtime);
      Status bs = b_->GetFileModificationTime(fname, &bmtime);
      assert(as == bs);
      assert(amtime - bmtime < 10000 || bmtime - amtime < 10000);
      *file_mtime = amtime;
      return as;
    }

    Status RenameFile(const std::string& s, const std::string& t) override {
      Status as = a_->RenameFile(s, t);
      Status bs = b_->RenameFile(s, t);
      assert(as == bs);
      return as;
    }

    Status LinkFile(const std::string& s, const std::string& t) override {
      Status as = a_->LinkFile(s, t);
      Status bs = b_->LinkFile(s, t);
      assert(as == bs);
      return as;
    }

    class MirrorFileLock : public FileLock {
    public:
      FileLock *a_, *b_;
      MirrorFileLock(FileLock *a, FileLock *b) : a_(a), b_(b) {}
    };

    Status LockFile(const std::string& f, FileLock** l) override {
      FileLock *al, *bl;
      Status as = a_->LockFile(f, &al);
      Status bs = b_->LockFile(f, &bl);
      *l = new MirrorFileLock(al, bl);
      assert(as == bs);
      return as;
    }

    Status UnlockFile(FileLock* l) override {
      MirrorFileLock *ml = static_cast<MirrorFileLock*>(l);
      Status as = a_->UnlockFile(ml->a_);
      Status bs = b_->UnlockFile(ml->b_);
      assert(as == bs);
      return as;
    }

  };

}

#endif
