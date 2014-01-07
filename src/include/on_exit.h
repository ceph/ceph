#ifndef CEPH_ON_EXIT_H
#define CEPH_ON_EXIT_H

#include <pthread.h>
#include <assert.h>
#include <vector>

/*
 * Create a static instance at the file level to get callbacks called when the
 * process exits via main() or exit().
 */

class OnExitManager {
  public:
    typedef void (*callback_t)(void *arg);

    OnExitManager() {
      int ret = pthread_mutex_init(&lock_, NULL);
      assert(ret == 0);
    }

    ~OnExitManager() {
      pthread_mutex_lock(&lock_);
      std::vector<struct cb>::iterator it;
      for (it = funcs_.begin(); it != funcs_.end(); it++) {
        it->func(it->arg);
      }
      funcs_.clear();
      pthread_mutex_unlock(&lock_);
    }

    void add_callback(callback_t func, void *arg) {
      pthread_mutex_lock(&lock_);
      struct cb callback = { func, arg };
      funcs_.push_back(callback);
      pthread_mutex_unlock(&lock_);
    }

  private:
    struct cb {
      callback_t func;
      void *arg;
    };

    std::vector<struct cb> funcs_;
    pthread_mutex_t lock_;
};

#endif
