#ifndef QCCCRYPTO_H
#define QCCCRYPTO_H

#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <queue>
extern "C" {
#include "cpa.h"
#include "lac/cpa_cy_sym.h"
#include "lac/cpa_cy_im.h"
#include "qae_mem.h"
#include "icp_sal_user.h"
#include "icp_sal_poll.h"
#include "qae_mem_utils.h"
}

class QccCrypto {

  public:
    CpaCySymCipherDirection qcc_op_type;

    QccCrypto() {};
    ~QccCrypto() {};

    bool init();
    bool destroy();
    bool perform_op(unsigned char* out, const unsigned char* in, size_t size,
        uint8_t *iv,
        uint8_t *key,
        CpaCySymCipherDirection op_type);

  private:

    // Currently only supporting AES_256_CBC.
    // To-Do: Needs to be expanded
    static const size_t AES_256_IV_LEN = 16;
    static const size_t AES_256_KEY_SIZE = 32;
    static const size_t QCC_MAX_RETRIES = 5000;

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
      CpaCySymSessionSetupData sess_stp_data;
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
      CpaStatus op_result;
      CpaCySymOpData *sym_op_data;
      Cpa32U buff_meta_size;
      Cpa32U num_buffers;
      Cpa32U buff_size;

      //Src data items
      Cpa8U *src_buff_meta;
      CpaBufferList *src_buff_list;
      CpaFlatBuffer *src_buff_flat;
      Cpa8U *src_buff;
      Cpa8U *iv_buff;
    } *qcc_op_mem;

    //QAT HW polling thread input structure
    struct qcc_thread_args {
      QccCrypto* qccinstance;
      int entry;
    };


    /*
     * Function to handle the crypt operation. Will run while the main thread
     * runs the polling function on the instance doing the op
     */
    void do_crypt(qcc_thread_args *thread_args);

    /*
     * Handle queue with free instances to handle op
     */
    std::queue<int> open_instances;
    int QccGetFreeInstance();
    void QccFreeInstance(int entry);

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
    CpaStatus init_stat, stat;

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
    static void* crypt_thread(void* entry);
    CpaStatus QccCyStartPoll(int entry);
    void poll_instance(int entry);

    pthread_t *cypollthreads;
    static const size_t qcc_sleep_duration = 2;
};
#endif //QCCCRYPTO_H
