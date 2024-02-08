#ifndef QCCCRYPTO_H
#define QCCCRYPTO_H

#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <thread>
#include <mutex>
#include <queue>
#include <memory>
#include "common/async/yield_context.h"
#include <memory>
#include "common/ceph_mutex.h"
#include <vector>
#include <functional>
#include <span>
#include <boost/circular_buffer.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
extern "C" {
#ifdef HAVE_QATDRV
#include "cpa.h"
#include "cpa_cy_sym_dp.h"
#include "cpa_cy_im.h"
#include "lac/cpa_cy_sym.h"
#include "lac/cpa_cy_im.h"
#include "qae_mem.h"
#include "icp_sal_user.h"
#include "icp_sal_poll.h"
#include "qae_mem_utils.h"
#else
#include <qat/cpa.h>
#include <qat/cpa_cy_im.h>
#include <qat/cpa_cy_sym_dp.h>
#include <qat/cpa_cy_sym.h>
#include <qat/qae_mem.h>
#include <qat/icp_sal_user.h>
#include <qat/icp_sal_poll.h>
#endif
}

class QccCrypto {
    friend class QatCrypto;
    size_t chunk_size{0};
    size_t max_requests{0};

    boost::asio::thread_pool my_pool{1};

    boost::circular_buffer<std::function<void(int)>> instance_completions;

    template <typename CompletionToken>
    auto async_get_instance(CompletionToken&& token);

  public:
    CpaCySymCipherDirection qcc_op_type;

    QccCrypto()  {};
    ~QccCrypto() { destroy(); };

    bool init(const size_t chunk_size, const size_t max_requests);
    bool destroy();
    bool perform_op_batch(unsigned char* out, const unsigned char* in, size_t size,
                          Cpa8U *iv,
                          Cpa8U *key,
                          CpaCySymCipherDirection op_type,
                          optional_yield y);

  private:
    // Currently only supporting AES_256_CBC.
    // To-Do: Needs to be expanded
    static const size_t AES_256_IV_LEN = 16;
    static const size_t AES_256_KEY_SIZE = 32;
    static const size_t MAX_NUM_SYM_REQ_BATCH = 32;

    /*
     * Struct to hold an instance of QAT to handle the crypto operations. These
     * will be identified at the start and held until the destructor is called
     * To-Do:
     * The struct was creating assuming that we will use all the instances.
     * Expand current implementation to allow multiple instances to operate
     * independently.
     */
    struct QCCINST {
      CpaInstanceHandle *cy_inst_handles;
      CpaBoolean *is_polled;
      Cpa16U num_instances;
    } *qcc_inst;

    /*
     * QAT Crypto Session
     * Crypto Session Context and setupdata holds
     * priority, type of crypto operation (cipher/chained),
     * cipher algorithm (AES, DES, etc),
     * single crypto or multi-buffer crypto.
     */
    struct QCCSESS {
      Cpa32U sess_ctx_sz;
      CpaCySymSessionCtx sess_ctx;
    } *qcc_sess;

    /*
     * Cipher Memory Allocations
     * Holds bufferlist, flatbuffer, cipher opration data and buffermeta needed
     * by QAT to perform the operation. Also buffers for IV, SRC, DEST.
     */
    struct QCCOPMEM {
      // Op common  items
      bool is_mem_alloc;
      bool op_complete;
      CpaCySymDpOpData *sym_op_data[MAX_NUM_SYM_REQ_BATCH];
      Cpa8U *src_buff[MAX_NUM_SYM_REQ_BATCH];
      Cpa8U *iv_buff[MAX_NUM_SYM_REQ_BATCH];
    } *qcc_op_mem;

    /*
     * Handle queue with free instances to handle op
     */
    boost::circular_buffer<int> open_instances;
    void QccFreeInstance(int entry);
    std::thread qat_poll_thread;
    bool thread_stop{false};

    /*
     * Contiguous Memory Allocator and de-allocator. We are using the usdm
     * driver that comes along with QAT to get us direct memory access using
     * hugepages.
     * To-Do: A kernel based one.
     */
    static inline void qcc_contig_mem_free(void **ptr) {
      if (*ptr) {
        qaeMemFreeNUMA(ptr);
        *ptr = NULL;
      }
    }

    static inline CpaStatus qcc_contig_mem_alloc(void **ptr, Cpa32U size, Cpa32U alignment = 1) {
      *ptr = qaeMemAllocNUMA(size, 0, alignment);
      if (NULL == *ptr)
      {
        return CPA_STATUS_RESOURCE;
      }
      return CPA_STATUS_SUCCESS;
    }

    /*
     * Malloc & free calls masked to maintain consistency and future kernel
     * alloc support.
     */
    static inline void qcc_os_mem_free(void **ptr) {
      if (*ptr) {
        free(*ptr);
        *ptr = NULL;
      }
    }

    static inline CpaStatus qcc_os_mem_alloc(void **ptr, Cpa32U size) {
      *ptr = malloc(size);
      if (*ptr == NULL)
      {
        return CPA_STATUS_RESOURCE;
      }
      return CPA_STATUS_SUCCESS;
    }

    std::atomic<bool> is_init = { false };

    /*
     * Function to cleanup memory if constructor fails
     */
    void cleanup();

    /*
     * Crypto Polling Function & helpers
     * This helps to retrieve data from the QAT rings and dispatching the
     * associated callbacks. For synchronous operation (like this one), QAT
     * library creates an internal callback for the operation.
     */
    void poll_instances(void);
    std::atomic<size_t> poll_retry_num{0};

    bool symPerformOp(int avail_inst,
                      CpaCySymSessionCtx sessionCtx,
                      const Cpa8U *pSrc,
                      Cpa8U *pDst,
                      Cpa32U size,
                      Cpa8U *pIv,
                      Cpa32U ivLen,
                      optional_yield y);

    CpaStatus initSession(CpaInstanceHandle cyInstHandle,
                          CpaCySymSessionCtx *sessionCtx,
                          Cpa8U *pCipherKey,
                          CpaCySymCipherDirection cipherDirection);

    CpaStatus updateSession(CpaCySymSessionCtx sessionCtx,
                            Cpa8U *pCipherKey,
                            CpaCySymCipherDirection cipherDirection);


};

class QatCrypto {
 private:
  std::function<void(CpaStatus stat)> completion_handler;
  std::atomic<std::size_t> count;
 public:
  void complete() {
    if (--count == 0) {
      completion_handler(CPA_STATUS_SUCCESS);
    }
    return ;
  }

  QatCrypto () : count(0) {}
  QatCrypto (const QatCrypto &qat) = delete;
  QatCrypto (QatCrypto &&qat) = delete;
  void operator=(const QatCrypto &qat) = delete;
  void operator=(QatCrypto &&qat) = delete;

  template <typename CompletionToken>
  auto async_perform_op(int avail_inst, std::span<CpaCySymDpOpData*> pOpDataVec, CompletionToken&& token);
};
#endif //QCCCRYPTO_H
