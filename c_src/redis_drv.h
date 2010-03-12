#ifndef __REDIS_DRV_H_
#define __REDIS_DRV_H_

#include <erl_driver.h>
#include <ei.h>

#include <credis.h>

#define REDIS_INVALID_COMMAND = 255
#define REDIS_SET             = 0
#define REDIS_GET             = 1
#define REDIS_DEL             = 2
#define REDIS_SADD            = 3
#define REDIS_SREM            = 4
#define REDIS_SMEMBERS        = 5

typedef struct _redis_drv_t {
  ErlDrvPort port;
  
  REDIS *redis;
} redis_drv_t;

static ErlDrvData start(ErlDrvPort port, char* cmd);
static void stop(ErlDrvData handle);
static void outputv(ErlDrvData handle, ErlIOVec *ev);

static void redis_drv_set(redis_drv_t *redis_drv, Reader *const reader);
static void redis_drv_get(redis_drv_t *redis_drv, Reader *const reader);
static void redis_drv_del(redis_drv_t *redis_drv, Reader *const reader);
static void redis_drv_sadd(redis_drv_t *redis_drv, Reader *const reader);
static void redis_drv_srem(redis_drv_t *redis_drv, Reader *const reader);
static void redis_drv_smembers(redis_drv_t *redis_drv, Reader *const reader);
static void redis_drv_unknown(redis_drv_t *redis_drv);

static void return_empty_list(redis_drv_t *redis_drv);
static void return_error(redis_drv_t *redis_drv, int errorno);
static void return_ok(redis_drv_t *redis_drv, int msgno);

#endif