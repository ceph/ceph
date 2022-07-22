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
#include <condition_variable>
extern "C" {
#include "cpa.h"
#include "cpa_cy_sym_dp.h"
#include "cpa_cy_im.h"
#include "lac/cpa_cy_sym.h"
#include "lac/cpa_cy_im.h"
#include "qae_mem.h"
#include "icp_sal_user.h"
#include "icp_sal_poll.h"
#include "qae_mem_utils.h"
}

class CompletionHandle;
class RequestQueue {
  public:
    struct Node {
        unsigned char *out;
        const unsigned char *in;
        size_t size;
        Cpa8U *iv;
        Cpa8U *key;
        CpaCySymCipherDirection op_type;
        CompletionHandle* completion;
        Node(unsigned char* out,
             const unsigned char* in,
             size_t size,
             Cpa8U *iv,
             Cpa8U *key,
             CpaCySymCipherDirection op_type,
             CompletionHandle* completion):out(out), in(in), size(size),
             iv(iv), key(key), op_type(op_type), completion(completion) {}
        Node(): out(nullptr), in(nullptr), size(0),
             iv(nullptr), key(nullptr), completion(nullptr) {}
    };
    std::queue<Node> requests;
    std::mutex mutex;
    // std::mutex takeLock;
    std::condition_variable notEmpty;
    // std::mutex putLock;
    std::condition_variable notFull;
    size_t capacity;
    std::atomic<size_t> count;
    void push(unsigned char* out,
              const unsigned char* in,
              size_t size,
              Cpa8U *iv,
              Cpa8U *key,
              CpaCySymCipherDirection op_type,
              CompletionHandle* completion) {
      std::unique_lock lock{mutex};
      notFull.wait(lock, [this](){return count < capacity;});
      requests.emplace(out, in, size, iv, key, op_type, completion);
      count++;
      notEmpty.notify_one();
    }
    bool try_get(Node &node, size_t free_size) {
      std::unique_lock lock{mutex};
      bool not_timeout = notEmpty.wait_for(lock, std::chrono::milliseconds(1), [this](){
        return !requests.empty();
      });

      if (not_timeout && requests.front().size <= free_size) {
        std::swap(node, requests.front());
        requests.pop();
        count--;
        notFull.notify_one();
        return true;
      }
      return false;
    }
 public:
  RequestQueue(): capacity(128), count(0) {}
  ~RequestQueue() {
  }
};

class QccCrypto {
  size_t chunk_size;

  public:
    CpaCySymCipherDirection qcc_op_type;

    QccCrypto() {};
    ~QccCrypto() {};

    bool init(const size_t chunk_size);
    bool destroy();
    bool perform_op_batch(unsigned char* out, const unsigned char* in, size_t size,
                          Cpa8U *iv,
                          Cpa8U *key,
                          CpaCySymCipherDirection op_type);

  private:

    // Currently only supporting AES_256_CBC.
    // To-Do: Needs to be expanded
    static const size_t AES_256_IV_LEN = 16;
    static const size_t AES_256_KEY_SIZE = 32;
    static const size_t MAX_NUM_SYM_REQ_BATCH = 32;
    size_t threshold;

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
     * Cipher Memory Allocations
     * Holds cipher opration data and buffermeta needed
     * by QAT to perform the operation. Also buffers for IV, SRC, DEST,
     * Crypto Session Context.
     */
    struct QCCOPMEM {
      // Op common items
      CpaCySymDpOpData *sym_op_data[MAX_NUM_SYM_REQ_BATCH];
      Cpa8U *src_buff[MAX_NUM_SYM_REQ_BATCH];
      Cpa8U *iv_buff[MAX_NUM_SYM_REQ_BATCH];
      CpaCySymSessionCtx sess_ctx[MAX_NUM_SYM_REQ_BATCH];
    } *qcc_op_mem;

    /*
     * Handle queue with free instances to handle op
     */
    std::queue<int> open_instances;
    int QccGetFreeInstance();
    void QccFreeInstance(int entry);
    std::thread qat_poll_thread;
    std::thread qat_queue_thread;
    RequestQueue request_queue;
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

    void perform_queue(void);

    bool symPerformOp(int avail_inst,
                      CpaCySymSessionCtx sessionCtx,
                      const Cpa8U *pSrc,
                      Cpa8U *pDst,
                      Cpa32U size,
                      Cpa8U *pIv,
                      Cpa32U ivLen);
    bool doQueuePerformOp(int avail_inst);

    CpaStatus initSession(CpaInstanceHandle cyInstHandle,
                          CpaCySymSessionCtx *sessionCtx,
                          Cpa8U *pCipherKey,
                          CpaCySymCipherDirection cipherDirection);

    CpaStatus updateSession(CpaCySymSessionCtx sessionCtx,
                            Cpa8U *pCipherKey,
                            CpaCySymCipherDirection cipherDirection);
};

#endif //QCCCRYPTO_H
