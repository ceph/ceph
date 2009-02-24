#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include "common/tls.h"

#include "config.h"

#undef dout
#undef derr
#define  dout(l)    if (l<=g_conf.debug_lockdep) *_dout << g_clock.now() << " " << pthread_self() << " tls: "
#define  derr(l)    if (l<=g_conf.debug_lockdep) *_derr << g_clock.now() << " " << pthread_self() << " tls: "


pthread_key_t   _tls_key = 0;

static int _initialized = 0;

static void _tls_destructor(void *value)
{
  free(value);
  pthread_setspecific(_tls_key, NULL);
}

struct TlsData *tls_get_val()
{
  void *val;

  if (!_initialized) {
    return NULL;
  }

  val  = pthread_getspecific(_tls_key);

  if (!val) {
    int ret;
    val = malloc(sizeof(struct TlsData));
    memset(val, 0, sizeof(struct TlsData));
    ret = pthread_setspecific(_tls_key, val);

    if (ret)
      return NULL;
  }

  return (struct TlsData *)val;
}

int tls_init()
{
  int ret = 0;

  if (!_initialized) {
    ret = pthread_key_create(&_tls_key, _tls_destructor);
    _initialized = 1;
  }
 
  return ret; 
}

void tls_finalize()
{
  if (_initialized)
    pthread_key_delete(_tls_key);
}
