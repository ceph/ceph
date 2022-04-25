#include "qcccrypto.h"
#include  <iostream>
#include "string.h"
#include <pthread.h>
#include <condition_variable>
#include "common/debug.h"
#include "include/scope_guard.h"
#include "common/dout.h"
#include "common/errno.h"
#include <atomic>

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "QccCrypto: ";
}
// -----------------------------------------------------------------------------

class CompletionHandle
{
 public:
  CompletionHandle(size_t count):count(count) {};
  CompletionHandle():count(0) {};
  void wait() {
    std::unique_lock lock{mutex};
    cv.wait(lock, [this]{return count == 0;});
  }
  void complete() {
    std::scoped_lock lock{mutex};
    count--;
    if (count == 0) {
      cv.notify_one();
    }
  }

  void add_one() {
    count++;
  }

 private:
  std::condition_variable cv;
  std::mutex mutex;
  std::atomic<std::size_t> count;
};

/*
 * Callback function
 */
static void symDpCallback(CpaCySymDpOpData *pOpData,
                        CpaStatus status,
                        CpaBoolean verifyResult)
{
  if (nullptr != pOpData->pCallbackTag)
  {
    auto complete = static_cast<CompletionHandle*>(pOpData->pCallbackTag);
    complete->complete();
  }
}

static std::mutex qcc_alloc_mutex;
static std::mutex qcc_eng_mutex;
static std::condition_variable alloc_cv;
static std::atomic<bool> init_called = { false };

void QccCrypto::QccFreeInstance(int entry) {
  std::lock_guard<std::mutex> freeinst(qcc_alloc_mutex);
  open_instances.push(entry);
  alloc_cv.notify_one();
}

int QccCrypto::QccGetFreeInstance() {
  int ret = -1;
  std::unique_lock getinst(qcc_alloc_mutex);
  alloc_cv.wait(getinst, [this](){return !open_instances.empty();});
  ret = open_instances.front();
  open_instances.pop();
  return ret;
}

void QccCrypto::cleanup() {
  icp_sal_userStop();
  qaeMemDestroy();
  is_init = false;
  init_called = false;
  derr << "Failure during QAT init sequence. Quitting" << dendl;
}

void QccCrypto::poll_instances(void) {
  while (!thread_stop) {
    for (int iter = 0; iter < qcc_inst->num_instances; iter++) {
      if (qcc_inst->is_polled[iter] == CPA_TRUE) {
        icp_sal_CyPollDpInstance(qcc_inst->cy_inst_handles[iter], 0);
      }
    }
    std::this_thread::yield();
  }
}

/*
 * We initialize QAT instance and everything that is common for all ops
*/
bool QccCrypto::init(const size_t chunk_size) {

  std::lock_guard<std::mutex> l(qcc_eng_mutex);
  CpaStatus stat = CPA_STATUS_SUCCESS;
  this->chunk_size = chunk_size;

  if (init_called) {
    dout(10) << "Init sequence already called. Skipping duplicate call" << dendl;
    return true;
  }

  // First call to init
  dout(15) << "First init for QAT" << dendl;
  init_called = true;

  // Find if the usermode memory driver is available. We need to this to
  // create contiguous memory needed by QAT.
  stat = qaeMemInit();
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to load memory driver" << dendl;
    this->cleanup();
    return false;
  }

  stat = icp_sal_userStart("CEPH");
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to start qat device" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_inst, sizeof(QCCINST));
  if (qcc_inst == NULL) {
    derr << "Unable to alloc mem for instance struct" << dendl;
    this->cleanup();
    return false;
  }

  // Initialize contents of qcc_inst
  qcc_inst->num_instances = 0;
  qcc_inst->cy_inst_handles = NULL;

  stat = cpaCyGetNumInstances(&(qcc_inst->num_instances));
  if ((stat != CPA_STATUS_SUCCESS) || (qcc_inst->num_instances <= 0)) {
    derr << "Unable to find available instances" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_inst->cy_inst_handles,
      ((int)qcc_inst->num_instances * sizeof(CpaInstanceHandle)));
  if (qcc_inst->cy_inst_handles == NULL) {
    derr << "Unable to allocate instances array memory" << dendl;
    this->cleanup();
    return false;
  }

  stat = cpaCyGetInstances(qcc_inst->num_instances, qcc_inst->cy_inst_handles);
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to get instances" << dendl;
    this->cleanup();
    return false;
  }
  dout(1) << "Get instances num: " << qcc_inst->num_instances << dendl;

  int iter = 0;
  //Start Instances
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCyStartInstance(qcc_inst->cy_inst_handles[iter]);
    if (stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to start instance" << dendl;
      this->cleanup();
      return false;
    }
  }

  qcc_os_mem_alloc((void **)&qcc_inst->is_polled,
      ((int)qcc_inst->num_instances * sizeof(CpaBoolean)));
  CpaInstanceInfo2 info;
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    qcc_inst->is_polled[iter] = cpaCyInstanceGetInfo2(qcc_inst->cy_inst_handles[iter],
        &info) == CPA_STATUS_SUCCESS ? info.isPolled : CPA_FALSE;
  }

  // Allocate memory structures for all instances
  qcc_os_mem_alloc((void **)&qcc_sess,
      ((int)qcc_inst->num_instances * sizeof(QCCSESS)));
  if (qcc_sess == NULL) {
    derr << "Unable to allocate memory for session struct" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_op_mem,
      ((int)qcc_inst->num_instances * sizeof(QCCOPMEM)));
  if (qcc_sess == NULL) {
    derr << "Unable to allocate memory for opmem struct" << dendl;
    this->cleanup();
    return false;
  }

  //At this point we are only doing an user-space version.
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCySetAddressTranslation(qcc_inst->cy_inst_handles[iter],
                                       qaeVirtToPhysNUMA);
    if (stat == CPA_STATUS_SUCCESS) {
      open_instances.push(iter);
      qcc_op_mem[iter].is_mem_alloc = false;

      stat = cpaCySymDpRegCbFunc(qcc_inst->cy_inst_handles[iter], symDpCallback);
      if (stat != CPA_STATUS_SUCCESS) {
        dout(1) << "Unable to register callback function for instance " << iter << " with status = " << stat << dendl;
        return false;
      }
    } else {
      dout(1) << "Unable to find address translations of instance " << iter << dendl;
      this->cleanup();
      return false;
    }
  }

  qat_poll_thread = make_named_thread("qat_poll", &QccCrypto::poll_instances, this);
  is_init = true;
  dout(10) << "Init complete" << dendl;
  return true;
}

bool QccCrypto::destroy() {
  if((!is_init) || (!init_called)) {
    dout(15) << "QAT not initialized here. Nothing to do" << dendl;
    return false;
  }

  {
    std::unique_lock getinst(qcc_alloc_mutex);
    // waiting for all instance being free
    alloc_cv.wait(getinst, [this](){return (open_instances.size() == qcc_inst->num_instances);});
  }

  thread_stop = true;
  if (qat_poll_thread.joinable()) {
    qat_poll_thread.join();
  }

  dout(10) << "Destroying QAT crypto & related memory" << dendl;
  int iter = 0;

  // Free up op related memory
  for (iter =0; iter < qcc_inst->num_instances; iter++) {
    for (size_t i = 0; i < MAX_NUM_SYM_REQ_BATCH; i++) {
      qcc_contig_mem_free((void **)&(qcc_op_mem[iter].src_buff[i]));
      qcc_contig_mem_free((void **)&(qcc_op_mem[iter].iv_buff[i]));
      qcc_contig_mem_free((void **)&(qcc_op_mem[iter].sym_op_data[i]));
    }
  }

  // Free up Session memory
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCySymDpRemoveSession(qcc_inst->cy_inst_handles[iter], qcc_sess[iter].sess_ctx);
    qcc_contig_mem_free((void **)&(qcc_sess[iter].sess_ctx));
  }

  // Stop QAT Instances
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCyStopInstance(qcc_inst->cy_inst_handles[iter]);
  }

  // Free up the base structures we use
  qcc_os_mem_free((void **)&qcc_op_mem);
  qcc_os_mem_free((void **)&qcc_sess);
  qcc_os_mem_free((void **)&(qcc_inst->cy_inst_handles));
  qcc_os_mem_free((void **)&(qcc_inst->is_polled));
  qcc_os_mem_free((void **)&qcc_inst);

  //Un-init memory driver and QAT HW
  icp_sal_userStop();
  qaeMemDestroy();
  init_called = false;
  is_init = false;
  return true;
}

bool QccCrypto::perform_op_batch(unsigned char* out, const unsigned char* in, size_t size,
    Cpa8U *iv,
    Cpa8U *key,
    CpaCySymCipherDirection op_type)
{
  if (!init_called) {
    dout(10) << "QAT not intialized yet. Initializing now..." << dendl;
    if (!QccCrypto::init(chunk_size)) {
      derr << "QAT init failed" << dendl;
      return false;
    }
  }

  if (!is_init)
  {
    dout(10) << "QAT not initialized in this instance or init failed" << dendl;
    return is_init;
  }
  CpaStatus status = CPA_STATUS_SUCCESS;
  int avail_inst = -1;
  avail_inst = QccGetFreeInstance();

  dout(10) << "Using dp_batch inst " << avail_inst << dendl;


  auto sg = make_scope_guard([=] {
      //free up the instance irrespective of the op status
      dout(15) << "Completed task under " << avail_inst << dendl;
      qcc_op_mem[avail_inst].op_complete = false;
      QccCrypto::QccFreeInstance(avail_inst);
      });

  /*
   * Allocate buffers for this version of the instance if not already done.
   * Hold onto to most of them until destructor is called.
  */
  if (qcc_op_mem[avail_inst].is_mem_alloc == false) {
    for (size_t i = 0; i < MAX_NUM_SYM_REQ_BATCH; i++) {
      // Allocate IV memory
      status = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].iv_buff[i]), AES_256_IV_LEN, 8);
      if (status != CPA_STATUS_SUCCESS) {
        derr << "Unable to allocate iv_buff memory" << dendl;
        return false;
      }

      // Allocate src memory
      status = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff[i]), chunk_size, 8);
      if (status != CPA_STATUS_SUCCESS) {
        derr << "Unable to allocate src_buff memory" << dendl;
        return false;
      }

      //Setup OpData
      status = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].sym_op_data[i]),
          sizeof(CpaCySymDpOpData), 8);
      if (status != CPA_STATUS_SUCCESS) {
        derr << "Unable to allocate opdata memory" << dendl;
        return false;
      }
    }

    // Set memalloc flag so that we don't go through this exercise again.
    qcc_op_mem[avail_inst].is_mem_alloc = true;

    status = initSession(qcc_inst->cy_inst_handles[avail_inst],
                             &(qcc_sess[avail_inst].sess_ctx),
                             (Cpa8U *)key,
                             op_type);
  } else {
    status = updateSession(qcc_sess[avail_inst].sess_ctx,
                               (Cpa8U *)key,
                               op_type);
  }

  if (unlikely(status != CPA_STATUS_SUCCESS)) {
    derr << "Unable to init session with status =" << status << dendl;
    return false;
  }

  return symPerformOp(avail_inst,
                      qcc_sess[avail_inst].sess_ctx,
                      in,
                      out,
                      size,
                      reinterpret_cast<Cpa8U*>(iv),
                      AES_256_IV_LEN);
}

/*
 * Perform session update
 */
CpaStatus QccCrypto::updateSession(CpaCySymSessionCtx sessionCtx,
                               Cpa8U *pCipherKey,
                               CpaCySymCipherDirection cipherDirection) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  CpaCySymSessionUpdateData sessionUpdateData = {0};

  sessionUpdateData.flags = CPA_CY_SYM_SESUPD_CIPHER_KEY;
  sessionUpdateData.flags |= CPA_CY_SYM_SESUPD_CIPHER_DIR;
  sessionUpdateData.pCipherKey = pCipherKey;
  sessionUpdateData.cipherDirection = cipherDirection;

  do {
    status = cpaCySymUpdateSession(sessionCtx, &sessionUpdateData);
  } while (status == CPA_STATUS_RETRY);

  if (unlikely(status != CPA_STATUS_SUCCESS)) {
    dout(1) << "cpaCySymUpdateSession failed with status = " << status << dendl;
  }

  return status;
}

CpaStatus QccCrypto::initSession(CpaInstanceHandle cyInstHandle,
                             CpaCySymSessionCtx *sessionCtx,
                             Cpa8U *pCipherKey,
                             CpaCySymCipherDirection cipherDirection) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U sessionCtxSize = 0;
  CpaCySymSessionSetupData sessionSetupData;
  memset(&sessionSetupData, 0, sizeof(sessionSetupData));

  sessionSetupData.sessionPriority = CPA_CY_PRIORITY_NORMAL;
  sessionSetupData.symOperation = CPA_CY_SYM_OP_CIPHER;
  sessionSetupData.cipherSetupData.cipherAlgorithm = CPA_CY_SYM_CIPHER_AES_CBC;
  sessionSetupData.cipherSetupData.cipherKeyLenInBytes = AES_256_KEY_SIZE;
  sessionSetupData.cipherSetupData.pCipherKey = pCipherKey;
  sessionSetupData.cipherSetupData.cipherDirection = cipherDirection;

  status = cpaCySymDpSessionCtxGetSize(cyInstHandle, &sessionSetupData, &sessionCtxSize);
  if (likely(CPA_STATUS_SUCCESS == status)) {
    status = qcc_contig_mem_alloc((void **)(sessionCtx), sessionCtxSize);
  } else {
    dout(1) << "cpaCySymDpSessionCtxGetSize failed with status = " << status << dendl;
  }
  if (likely(CPA_STATUS_SUCCESS == status)) {
    status = cpaCySymDpInitSession(cyInstHandle,
                                   &sessionSetupData,
                                   *sessionCtx);
    if (unlikely(status != CPA_STATUS_SUCCESS)) {
      dout(1) << "cpaCySymDpInitSession failed with status = " << status << dendl;
    }
  } else {
    dout(1) << "Session alloc failed with status = " << status << dendl;
  }
  return status;
}

bool QccCrypto::symPerformOp(int avail_inst,
                              CpaCySymSessionCtx sessionCtx,
                              const Cpa8U *pSrc,
                              Cpa8U *pDst,
                              Cpa32U size,
                              Cpa8U *pIv,
                              Cpa32U ivLen) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U one_batch_size = chunk_size * MAX_NUM_SYM_REQ_BATCH;
  Cpa32U iv_index = 0;
  for (Cpa32U off = 0; off < size; off += one_batch_size) {
    CompletionHandle complete;
    for (Cpa32U offset = off, i = 0; offset < size && i < MAX_NUM_SYM_REQ_BATCH; offset += chunk_size, i++) {
      CpaCySymDpOpData *pOpData = qcc_op_mem[avail_inst].sym_op_data[i];
      Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff[i];
      Cpa8U *pIvBuffer = qcc_op_mem[avail_inst].iv_buff[i];
      Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;
      if (CPA_STATUS_SUCCESS == status) {
        // copy source into buffer
        memcpy(pSrcBuffer, pSrc + offset, process_size);
        // copy IV into buffer
        memcpy(pIvBuffer, &pIv[iv_index * ivLen], ivLen);
        iv_index++;

        complete.add_one();

        //pOpData assignment
        pOpData->thisPhys = qaeVirtToPhysNUMA(pOpData);
        pOpData->instanceHandle = qcc_inst->cy_inst_handles[avail_inst];
        pOpData->sessionCtx = sessionCtx;
        pOpData->pCallbackTag = (void *)&complete;
        pOpData->cryptoStartSrcOffsetInBytes = 0;
        pOpData->messageLenToCipherInBytes = process_size;
        pOpData->iv = qaeVirtToPhysNUMA(pIvBuffer);
        pOpData->pIv = pIvBuffer;
        pOpData->ivLenInBytes = ivLen;
        pOpData->srcBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
        pOpData->srcBufferLen = process_size;
        pOpData->dstBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
        pOpData->dstBufferLen = process_size;

        status = cpaCySymDpEnqueueOp(pOpData, CPA_FALSE);
        if (unlikely(CPA_STATUS_SUCCESS != status)) {
          dout(1) << "cpaCySymDpEnqueueOp failed. (status = " << status << ")" << dendl;
          // submit all request, clean the queue
          cpaCySymDpPerformOpNow(qcc_inst->cy_inst_handles[avail_inst]);
          return false;
        }
      }
    }

    if (likely(CPA_STATUS_SUCCESS == status)) {
      status = cpaCySymDpPerformOpNow(qcc_inst->cy_inst_handles[avail_inst]);
      if (unlikely(CPA_STATUS_SUCCESS != status)) {
        dout(1) << "cpaCySymDpPerformOpNow failed. (status = " << status << ")" << dendl;
        return false;
      }
    }

    if (likely(CPA_STATUS_SUCCESS == status)) {
      complete.wait();
      // Copy data back to pDst buffer
      for (Cpa32U offset = off, i = 0; offset < size && i < MAX_NUM_SYM_REQ_BATCH; offset += chunk_size, i++) {
        Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff[i];
        Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;
        memcpy(pDst + offset, pSrcBuffer, process_size);
      }
    }
  }

  Cpa32U max_used_buffer_num = iv_index > MAX_NUM_SYM_REQ_BATCH ? MAX_NUM_SYM_REQ_BATCH : iv_index;
  for (Cpa32U i = 0; i < max_used_buffer_num; i++) {
    memset(qcc_op_mem[avail_inst].src_buff[i], 0, chunk_size);
    memset(qcc_op_mem[avail_inst].iv_buff[i], 0, ivLen);
  }
  return (status == CPA_STATUS_SUCCESS);
}
