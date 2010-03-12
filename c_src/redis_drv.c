#include "redis_drv.h"

#include <string.h>

//Taken from toke.c
uint8_t redis_invalid_command = REDIS_INVALID_COMMAND;

#define REDIS_DRV_ERROR 0
#define REDIS_DRV_NIL   1
#define REDIS_DRV_UNKNOWN 2

#define REDIS_DRV_OK 0

char ok_msgs[] = {
  "ok"
};

char error_msgs[] = {
  "error",
  "nil",
  "unknown"
};

typedef struct {
  ErlIOVec *ev;
  size_t row;
  size_t column;
  ReaderError last_error;
} Reader;

void make_reader(ErlIOVec *const ev, Reader *const reader) {
  reader->ev = ev;
  reader->row = 1; /* row 0 is reserved for headers */
  reader->column = 0;
  reader->last_error = READER_NO_ERROR;
}

int read_simple_thing(Reader *const reader, const char **const result,
                      const size_t size) {
  size_t row = reader->row;
  size_t column = reader->column;
  const long data_left_in_current_row =
    (reader->ev->binv[row]->orig_size) - column;
  if (data_left_in_current_row == 0) {
    ++row;
    if (row == reader->ev->vsize) {
      reader->last_error = READER_READ_ALL_DATA;
      return FALSE; /* run out of data */
    } else {
      reader->row = row;
      reader->column = 0;
      return read_simple_thing(reader, result, size);
    }
  } else if (data_left_in_current_row < size) {
    reader->last_error = READER_PACKING_ERROR;
    return FALSE; /* packing error! */
  } else {
    *result = (reader->ev->binv[row]->orig_bytes) + column;
    column += size;
    reader->column = column;
    return TRUE;
  }
}

int read_uint8(Reader *const reader, const uint8_t **const result) {
  return read_simple_thing(reader, (const char **const)result, sizeof(uint8_t));
}

int read_int8(Reader *const reader, const int8_t **const result) {
  return read_simple_thing(reader, (const char **const)result, sizeof(int8_t));
}

int read_int32(Reader *const reader, const int32_t **const result) {
  return read_simple_thing(reader, (const char **const)result, sizeof(int32_t));
}

int read_uint64(Reader *const reader, const uint64_t **const result) {
  return
    read_simple_thing(reader, (const char **const)result, sizeof(uint64_t));
}

int read_int64(Reader *const reader, const int64_t **const result) {
  return read_simple_thing(reader, (const char **const)result, sizeof(int64_t));
}

int read_binary(Reader *const reader, const char **const result,
                const uint64_t **const binlen) {
  if (read_simple_thing(reader, (const char **const)binlen, sizeof(uint64_t))) {
    return read_simple_thing(reader, result, **binlen);
  } else {
    return 1;
  }
}

//end of taken from toke.c

static ErlDrvData start(ErlDrvPort port, char* cmd) {
  
  //TODO handle connection errors;
  REDIS redis = credis_connect(NULL, 0, 10000);
  
  redis_drv_t* retval = (redis_drv_t*) driver_alloc(sizeof(redis_drv_t));
  
  retval->port = port;
  retval->redis = redis;
  
  return (ErlDrvData) retval;
}

static void stop(ErlDrvData handle) {
  redis_drv_t* driver_data = (redis_drv_t*) handle;
  
  REDIS redis = driver_data->redis;
  
  credis_close(redis);
  
  driver_free(driver_data);
}

static void outputv(ErlDrvData handle, ErlIOVec *ev) {
  
  Reader reader;
  
  const uint8_t* command = &redis_invalid_command;
  
  redis_drv_t* driver_data = (redis_drv_t*) handle;
  ErlDrvBinary* data = ev->binv[1];
  
  make_reader(ev, &reader);
  
  if (read_uint8(&reader, &command)) {
    switch(command) {
      case REDIS_SET:
        redis_drv_set(driver_data, &reader);
        break;
      case REDIS_GET:
        redis_drv_get(driver_data, &reader);
        break;
      case REDIS_DEL:
        redis_drv_del(driver_data, &reader);
        break;
      case REDIS_SADD:
        redis_drv_sadd(driver_data, &reader);
        break;
      case REDIS_SREM:
        redis_drv_srem(driver_data, &reader);
        break;
      case REDIS_SMEMBERS:
        redis_drv_smembers(driver_data, &reader);
        break;
      default:
        redis_drv_unknown(driver_data);
    }
  } else{
    redis_drv_unknown(driver_data);
  }
}


static void redis_drv_set(redis_drv_t *redis_drv, Reader *const reader)
{
  int rc;
  REDIS redis = redis_drv->redis;
  
  const uint64_t *keysize = NULL;
  const char *key = NULL;
  const uint64_t *valuesize = NULL;
  const char *value = NULL;
  
  if(read_binary(reader, &key, &keysize) && read_binary(reader, &value, &valuesize)){
    rc = credis_set(redis, key, value);
    if(rc > -1){
      return_ok(redis_drv, REDIS_DRV_OK);
    } else {
      return_error(redis_drv, REDIS_DRV_ERROR);
    }
  } else {
    return_error(redis_drv, REDIS_DRV_ERROR);
  }
}

static void redis_drv_get(redis_drv_t *redis_drv, Reader *const reader)
{
  int rc;
  char *val;
  
  REDIS redis = redis_drv->redis;
  
  const uint64_t *keysize = NULL;
  const char *key = NULL;
  
  if(read_binary(reader, &key, &keysize)){
    rc = credis_get(redis, key, &val);
    if(rc > -1){
      ErlDrvTermData spec[] = {
        ERL_DRV_BINARY, val, strlen(val), 0
      };
      driver_output_term(redis_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
    } else{
      return_error(redis_drv, REDIS_DRV_NIL);
    }
  } else{
    return_error(redis_drv, REDIS_DRV_ERROR);
  }
}

static void redis_drv_del(redis_drv_t *redis_drv, Reader *const reader)
{
  int rc;
  
  REDIS redis = redis_drv->redis;
  
  const uint64_t *keysize = NULL;
  const char *key = NULL;
  
  if(read_binary(reader, &key, &keysize)){
    rc = credis_del(redis, key);
    if(rc > -1){
      return_ok(redis_drv, REDIS_DRV_OK);
    }
  }
  
  return_error(redis_drv, REDIS_DRV_ERROR);
}

static void redis_drv_sadd(redis_drv_t *redis_drv, Reader *const reader)
{
  int rc;
  REDIS redis = redis_drv->redis;
  
  const uint64_t *keysize = NULL;
  const char *key = NULL;
  const uint64_t *valuesize = NULL;
  const char *value = NULL;
  
  if(read_binary(reader, &key, &keysize) && read_binary(reader, &value, &valuesize)){
    rc = credis_sadd(redis, key, value);
    if(rc > -1){
      return_ok(redis_drv, REDIS_DRV_OK);
    }
  }
  
  return_error(redis_drv, REDIS_DRV_ERROR);
}

static void redis_drv_srem(redis_drv_t *redis_drv, Reader *const reader)
{
  int rc;
  REDIS redis = redis_drv->redis;
  
  const uint64_t *keysize = NULL;
  const char *key = NULL;
  const uint64_t *valuesize = NULL;
  const char *value = NULL;
  
  if(read_binary(reader, &key, &keysize) && read_binary(reader, &value, &valuesize)){
    rc = credis_srem(redis, key, value);
    if(rc > -1){
      return_ok(redis_drv, REDIS_DRV_OK);
    }
  }
}

static void redis_drv_smembers(redis_drv_t *redis_drv, Reader *const reader)
{
  char **valv;
  int rc;
  
  REDIS redis = redis_drv->redis;
  
  const uint64_t *keysize = NULL;
  const char *key = NULL;
  
  if(read_binary(reader, &key, &keysize)){
    rc = credis_smembers(redis, key, &valv);
    if(rc > 0){
      ErlDrvTermData *p;

      ErlDrvTermData spec[rc*4+3];

      for(p = spec[0]; p <= spec[rc*4]; p++){
        *p = ERL_DRV_BINARY;
        *p++ = valv[spec[rc+2] - p];
        *p++ = strlen(valv[spec[rc+2] - p]);
        *p++ = 0;
      }

      spec[rc+1] = ERL_DRV_NIL;
      spec[rc+2] = ERL_DRV_LIST;
      spec[rc+3] = rc+1;
      
      driver_output_term(redis_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
      
    } else{
      return_empty_list(redis_drv);
    }
  } else{
    return_empty_list(redis_drv);
  }
}

static void redis_drv_unknown(redis_drv_t *redis_drv)
{
  return_error(redis_drv, REDIS_DRV_UNKNOWN);
}

static void return_empty_list(redis_drv_t *redis_drv)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_NIL, ERL_DRV_LIST, 1},
  };
  
  driver_output_term(redis_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static void return_ok(redis_drv_t *redis_drv, int msgno)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom(ok_msgs[msgno]),
  };
  driver_output_term(redis_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static void return_error(redis_drv_t *redis_drv, int errorno)
{
  ErlDrvTermData spec[] = {
      ERL_DRV_ATOM, driver_mk_atom(error_msgs[errorno]),
  };
  driver_output_term(redis_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
}

static ErlDrvEntry redis_drviver_entry = {
    NULL,                             /* init */
    start,                            /* startup */
    stop,                             /* shutdown */
    NULL,                             /* output */
    NULL,                             /* ready_input */
    NULL,                             /* ready_output */
    "redis_drv",                     /* the name of the driver */
    NULL,                             /* finish */
    NULL,                             /* handle */
    NULL,                             /* control */
    NULL,                             /* timeout */
    outputv,                          /* process */
    NULL,                             /* ready_async */
    NULL,                             /* flush */
    NULL,                             /* call */
    NULL,                             /* event */
    ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT(redis_drviver) {
  return &redis_drviver_entry;
}