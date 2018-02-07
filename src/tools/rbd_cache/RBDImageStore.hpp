#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_cache
#undef dout_prefix
#define dout_prefix *_dout << "rbd::cache::RBDImageStore: " << this << " " \
                           << __func__ << ": "


using librados::Rados;
using librados::IoCtx;

typedef shared_ptr<librados::Rados> RadosRef;
typedef shared_ptr<librados::IoCtx> IoCtxRef;


class RBDImageStore {
  public:
    RBDImageStore(CephContext *cct): m_cct(cct), m_rados(new librados::Rados()){}
    ~RBDImageStore();
    int init();
    int shutdown();
    int open_image(std::string pool_name, std::string volume_name, std::string snap_name);
    int promote_block(std::string pool_name, std::string volume_name, std::string snap_name, uint64_t offset, uint64_t length);

  private:
    uint32_t block_size;

    CephContext *m_cct;
    RadosRef m_rados;
    std::map<std::string, librbd::ImageCtx*> m_image_ctx_map;

    librados::IoCtx m_ioctx;
    librbd::ImageCtx *image_ctx = nullptr;
};

int RBDImageStore::init() {
  int r = m_rados->init_with_context(m_cct);
  if (r < 0) {
    std::cout << "could not initialize rados handle\n";
    return r;
  }

  r = m_rados->connect();
  if (r < 0) {
    //derr << "error connecting to local cluster" << dendl;
    return r;
  }

  //Test
  if (0) {
    r = open_image("rbd", "testimage", "");

    r = promote_block("rbd", "testimage", "", 4096, 4096);
  }
  return r;
}

int RBDImageStore::shutdown() {
  int r = 0;

  for (auto &it: m_image_ctx_map) {
    ((librbd::ImageCtx*)(it.second))->state->close();
    delete ((librbd::ImageCtx*)(it.second));
  }

  m_rados->shutdown();
  return r;
}

int RBDImageStore::open_image(std::string pool_name, std::string volume_name, std::string snap_name) {
  int m_rados_pool_id = 1;
  int r = m_rados->ioctx_create2(m_rados_pool_id, m_ioctx);

  image_ctx = new librbd::ImageCtx(volume_name, "", nullptr, m_ioctx, false);
  r = image_ctx->state->open(false);
  if (r < 0) {
    std::cout << "could not open image\n";
    return r;
  }
  m_image_ctx_map[volume_name] = image_ctx;

  return r;
}

int RBDImageStore::promote_block(std::string pool_name, std::string volume_name, std::string snap_name, uint64_t offset, uint64_t length) {

  librbd::ImageCtx *target_image_ctx = nullptr;
  auto it = m_image_ctx_map.find(volume_name);
  if (it != m_image_ctx_map.end()) {
    target_image_ctx = (librbd::ImageCtx*)it->second;
  }
  assert(target_image_ctx == nullptr);

  char* data = (char*)malloc(length);
  //TODO(): promote cache directly?
  rbd_read(target_image_ctx, offset, length, data);
  //target_image_ctx->state->close();
  free(data);

  return 0;
}
