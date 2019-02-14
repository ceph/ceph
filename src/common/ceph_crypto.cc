// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010-2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_context.h"
#include "common/config.h"
#include "ceph_crypto.h"

#ifdef USE_NSS

// for SECMOD_RestartModules()
#include <secmod.h>
#include <nspr.h>

#endif /*USE_NSS*/

#ifdef USE_OPENSSL
#include <openssl/evp.h>

#ifdef USE_OPENSSL_ASYNC
#include <forward_list>
#include <functional>
#include <openssl/async.h>
#include "common/Thread.h"
#endif /* USE_OPENSSL_ASYNC */
#endif /*USE_OPENSSL*/

void ceph::crypto::init(CephContext *cct)
{
#ifdef USE_OPENSSL
  ceph::crypto::ssl::init(cct);
#endif
#ifdef USE_NSS
  ceph::crypto::nss::init(cct);
#endif
}

void ceph::crypto::shutdown(bool shared)
{
#ifdef USE_NSS
  ceph::crypto::nss::shutdown(shared);
#endif
#ifdef USE_OPENSSL
  ceph::crypto::ssl::shutdown(shared);
#endif
}

#ifdef USE_NSS

static pthread_mutex_t crypto_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint32_t crypto_refs = 0;
static NSSInitContext *crypto_context = NULL;
static pid_t crypto_init_pid = 0;

void ceph::crypto::nss::init(CephContext *cct)
{
  pid_t pid = getpid();
  pthread_mutex_lock(&crypto_init_mutex);
  if (crypto_init_pid != pid) {
    if (crypto_init_pid > 0) {
      SECMOD_RestartModules(PR_FALSE);
    }
    crypto_init_pid = pid;
  }

  if (++crypto_refs == 1) {
    NSSInitParameters init_params;
    memset(&init_params, 0, sizeof(init_params));
    init_params.length = sizeof(init_params);

    uint32_t flags = (NSS_INIT_READONLY | NSS_INIT_PK11RELOAD);
    if (cct->_conf->nss_db_path.empty()) {
      flags |= (NSS_INIT_NOCERTDB | NSS_INIT_NOMODDB);
    }
    crypto_context = NSS_InitContext(cct->_conf->nss_db_path.c_str(), "", "",
                                     SECMOD_DB, &init_params, flags);
  }
  pthread_mutex_unlock(&crypto_init_mutex);
  ceph_assert_always(crypto_context != NULL);
}

void ceph::crypto::nss::shutdown(bool shared)
{
  pthread_mutex_lock(&crypto_init_mutex);
  ceph_assert_always(crypto_refs > 0);
  if (--crypto_refs == 0) {
    NSS_ShutdownContext(crypto_context);
    if (!shared) {
      PR_Cleanup();
    }
    crypto_context = NULL;
    crypto_init_pid = 0;
  }
  pthread_mutex_unlock(&crypto_init_mutex);
}

ceph::crypto::HMAC::~HMAC()
{
  PK11_DestroyContext(ctx, PR_TRUE);
  PK11_FreeSymKey(symkey);
  PK11_FreeSlot(slot);
}

#else
# error "No supported crypto implementation found."
#endif /*USE_NSS*/

#ifdef USE_OPENSSL

#ifdef USE_OPENSSL_ASYNC
class OpenSSLAsyncEngine {
 public:
  int init() {return 0;}

  int run()
  {
    mStop = false;
    mThread = make_named_thread("openssl_async",
				&OpenSSLAsyncEngine::entry,
				this);
    return 0;
  }

  void stop()
  {
    {
      std::lock_guard<std::mutex> lock(mMutex);

      mStop = true;
    }

    mCond.notify_one();
  }

  void join()
  {
    if (mThread.joinable())
      mThread.join();
  }

  bool stopped()
  {
    return mStop;
  }

 private:
  static void entry(OpenSSLAsyncEngine *that)
  {
    that->main();
  }

  void main()
  {
    /* Initialise OpenSSL things that will be needed */
    ASYNC_init_thread(0, 10);
    ASYNC_WAIT_CTX *ctx  = ASYNC_WAIT_CTX_new();

    while (true) {
      jobs_type::iterator it;
      {
	std::unique_lock<std::mutex> lock(mMutex);

	if (mStop)
	  break;

	while (mJobs.empty()) {
	  mCond.wait(lock);
	  if (mStop)
	    break;
	}

	it = mJobs.begin();
      }

      jobs_type::iterator prev = mJobs.end();
      int ret;
      while (it != mJobs.end()) {
	if (mStop)
	  break;

	if (ASYNC_start_job(&(*it)->job,
			    ctx,
			    &ret,
			    &OpenSSLAsyncEngine::job_entry,
			    &(*it),
			    sizeof(JobData *)) == ASYNC_PAUSE) {
	  /* The job has been paused, safe to move on to the next job */
	  prev = it++;
	  continue;
	}

	/* Assuming job has completed notify waiting thread*/
	(*it)->failure_code = ret;
	{
	  std::lock_guard<std::mutex> lock((*it)->mutex);

	  (*it)->done = true;
	}
	(*it)->cond.notify_one();

	{
	  std::lock_guard<std::mutex> lock(mMutex);
	  if (prev == mJobs.end()) {
	    mJobs.pop_front();
	    it = mJobs.begin();
	  } else {
	    it = mJobs.erase_after(prev);
	  }
	}
      }

      if (mStop)
	break;
    }

    /* We have exited the loop, cleanup the thread */
    ASYNC_cleanup_thread();
  }

  static int job_entry(void *arg)
  {
    auto job = *reinterpret_cast<JobData **>(arg);


    int rval = 0;
    try {
      rval = job->f();
    } catch (...) {
      return -1;
    }

    return rval;
  }

 public:
  static OpenSSLAsyncEngine &instance()
  {
    /* In C++11 this is thread safe. */
    static OpenSSLAsyncEngine inst;
    return inst;
  }

  int submit_job_and_wait(const std::string &name, std::function<int ()> f)
  {
    JobData job(name, f);

    /* Submit job to be handled by the worker thread */
    {
      std::lock_guard<std::mutex> lock(mMutex);

      mJobs.push_front(&job);

      if (mStop) {
	/* The worker thread is not running, it needs to be restarted */
	run();
      }
    }

    /* If the worker thread is paused waiting for there to be jobs this will wake it up */
    mCond.notify_one();

    /* Wait until the job has completed */
    {
      std::unique_lock<std::mutex> lock(job.mutex);

      auto done = &job.done;
      job.cond.wait(lock, [done]{ return *done; });
    }


    return job.failure_code;
  }

  OpenSSLAsyncEngine(const OpenSSLAsyncEngine&) = delete;
  void operator=(OpenSSLAsyncEngine const&)  = delete;

 private:
  OpenSSLAsyncEngine()
    : mThread()
    , mMutex()
    , mCond()
    , mStop(false)
    , mJobs() {}

  ~OpenSSLAsyncEngine()
  {
    stop();
    join();
  }

 private:
  std::thread mThread;
  std::mutex mMutex;
  std::condition_variable mCond;

  bool mStop;

  struct JobData {
    std::string name;
    ASYNC_JOB *job;
    std::function<int ()> f;
    bool done;
    std::mutex mutex;
    std::condition_variable cond;
    int failure_code;

    JobData(const std::string &name, std::function<int ()> f)
      : name(name)
      , job(NULL)
      , f(std::move(f))
      , done(false)
      , mutex()
      , cond()
      , failure_code(0) {}
  };

  typedef std::forward_list<JobData *> jobs_type;
  jobs_type mJobs;
};

void ceph::crypto::ssl::init(CephContext *cct)
{
  OpenSSLAsyncEngine &async = OpenSSLAsyncEngine::instance();

  async.init();
  async.run();
}

void ceph::crypto::ssl::shutdown(bool shared)
{
  OpenSSLAsyncEngine &async = OpenSSLAsyncEngine::instance();

  async.stop();
  async.join();
}

ceph::crypto::ssl::OpenSSLDigest::OpenSSLDigest(const EVP_MD * _type)
  : mpContext(EVP_MD_CTX_create())
  , mpType(_type) {
  this->Restart();
}

ceph::crypto::ssl::OpenSSLDigest::~OpenSSLDigest() {
  EVP_MD_CTX_destroy(mpContext);
}

void ceph::crypto::ssl::OpenSSLDigest::Restart() {
  OpenSSLAsyncEngine &async = OpenSSLAsyncEngine::instance();

  std::function<int ()> func = std::bind(EVP_DigestInit_ex, mpContext, mpType, reinterpret_cast<ENGINE *>(NULL));
  async.submit_job_and_wait("EVP_DigestInit_ex", func);
}

void ceph::crypto::ssl::OpenSSLDigest::Update(const unsigned char *input, size_t length) {
  if (length) {
    OpenSSLAsyncEngine &async = OpenSSLAsyncEngine::instance();

    auto func = std::bind(EVP_DigestUpdate,
			  mpContext,
			  const_cast<void *>(reinterpret_cast<const void *>(input)),
			  length);
    async.submit_job_and_wait("EVP_DigestUpdate", func);
  }
}

void ceph::crypto::ssl::OpenSSLDigest::Final(unsigned char *digest) {
  OpenSSLAsyncEngine &async = OpenSSLAsyncEngine::instance();
  unsigned int s;
  auto func = std::bind(EVP_DigestFinal_ex, mpContext, digest, &s);
  async.submit_job_and_wait("EVP_DigestFinal_ex", func);
}

#else

void ceph::crypto::ssl::init(CephContext *cct)
{
}

void ceph::crypto::ssl::shutdown(bool shared)
{
}

ceph::crypto::ssl::OpenSSLDigest::OpenSSLDigest(const EVP_MD * _type)
  : mpContext(EVP_MD_CTX_create())
  , mpType(_type) {
  this->Restart();
}

ceph::crypto::ssl::OpenSSLDigest::~OpenSSLDigest() {
  EVP_MD_CTX_destroy(mpContext);
}

void ceph::crypto::ssl::OpenSSLDigest::Restart() {
  EVP_DigestInit_ex(mpContext, mpType, NULL);
}

void ceph::crypto::ssl::OpenSSLDigest::Update(const unsigned char *input, size_t length) {
  if (length) {
    EVP_DigestUpdate(mpContext, const_cast<void *>(reinterpret_cast<const void *>(input)), length);
  }
}

void ceph::crypto::ssl::OpenSSLDigest::Final(unsigned char *digest) {
  unsigned int s;
  EVP_DigestFinal_ex(mpContext, digest, &s);
}

#endif /* USE_OPENSSL_ASYNC */
#endif /*USE_OPENSSL*/
