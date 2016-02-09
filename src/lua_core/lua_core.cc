/*
 * Core Lua Bindings for Generic Interfaces (common to both MDS and OSD)
 */
#include <errno.h>
#include <setjmp.h>
#include <string>
#include <sstream>
#include <lua.hpp>
#include "include/types.h"
#include "json_spirit/json_spirit.h"
#include "lua_core/lua_core.h"
#include "lua_core/lua_core_ops.h"

cls_handle_t h_class;
cls_method_handle_t h_eval_json;
cls_method_handle_t h_eval_bufferlist;
string lua_clslibname;

/*
 * Jump point for recovering from Lua panic.
 */
static jmp_buf cls_lua_panic_jump;

/*
 * Handle Lua panic.
 */
static int cls_lua_atpanic(lua_State *lua)
{
  CLS_ERR("error: Lua panic: %s", lua_tostring(lua, -1));
  longjmp(cls_lua_panic_jump, 1);
  return 0;
}

struct clslua_err {
  bool error;
  int ret;
};

struct clslua_hctx {
  struct clslua_err error;
  InputEncoding in_enc;
  int ret;

  cls_method_context_t *hctx;
  bufferlist *inbl;   // raw cls input
  bufferlist *outbl;  // raw cls output

  string script;      // lua script
  string handler;     // lua handler
  bufferlist input;   // lua handler input
};

/* Lua registry key for method context */
static char clslua_hctx_reg_key;

/*
 * Grabs the full method handler context
 */
clslua_hctx *__clslua_get_hctx(lua_State *L)
{
  /* lookup registry value */
  lua_pushlightuserdata(L, &clslua_hctx_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);

  /* check cls_lua assumptions */
  assert(!lua_isnil(L, -1));
  assert(lua_type(L, -1) == LUA_TLIGHTUSERDATA);

  /* cast and cleanup stack */
  clslua_hctx *hctx = (struct clslua_hctx *)lua_touserdata(L, -1);
  lua_pop(L, 1);

  return hctx;
}

/*
 * Get the method context out of the registry. This is called at the beginning
 * of each clx_cxx_* wrapper, and must be set before there is any chance a Lua
 * script calling a 'cls' module function that requires it.
 */
cls_method_context_t clslua_get_hctx(lua_State *L)
{
  struct clslua_hctx *hctx = __clslua_get_hctx(L);
  return *hctx->hctx;
}

/*
 * Returns a reference to cls_lua error state from registry.
 */
struct clslua_err *clslua_checkerr(lua_State *L)
{
  struct clslua_hctx *hctx = __clslua_get_hctx(L);
  struct clslua_err *err = &hctx->error;
  return err;
}


/* Registry key for real `pcall` function */
static char clslua_pcall_reg_key;

/*
 * Wrap Lua pcall to check for errors thrown by cls_lua (e.g. I/O errors or
 * bufferlist decoding errors). The global error is cleared before returning
 * to the caller.
 */
static int clslua_pcall(lua_State *L)
{
  int nargs = lua_gettop(L);
  lua_pushlightuserdata(L, &clslua_pcall_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);
  lua_insert(L, 1);
  lua_call(L, nargs, LUA_MULTRET);
  struct clslua_err *err = clslua_checkerr(L);
  assert(err);
  if (err->error) {
    err->error = false;
    lua_pushinteger(L, err->ret);
    lua_insert(L, -2);
  }
  return lua_gettop(L);
}


/*
 * cls_log
 */
static int clslua_log(lua_State *L)
{
  int nargs = lua_gettop(L);

  if (!nargs)
    return 0;

  int loglevel = LOG_LEVEL_DEFAULT;
  bool custom_ll = false;

  /* check if first arg can be a log level */
  if (nargs > 1 && lua_isnumber(L, 1)) {
    int ll = (int)lua_tonumber(L, 1);
    if (ll >= 0) {
      loglevel = ll;
      custom_ll = true;
    }
  }

  /* check space for args and seperators (" ") */
  int nelems = ((nargs - (custom_ll ? 1 : 0)) * 2) - 1;
  luaL_checkstack(L, nelems, "rados.log(..)");

  for (int i = custom_ll ? 2 : 1; i <= nargs; i++) {
    const char *part = lua_tostring(L, i);
    if (!part) {
      if (lua_type(L, i) == LUA_TBOOLEAN)
        part = lua_toboolean(L, i) ? "true" : "false";
      else
        part = luaL_typename(L, i);
    }
    lua_pushstring(L, part);
    if ((i+1) <= nargs)
      lua_pushstring(L, " ");
  }

  /* join string parts and send to Ceph/reply log */
  lua_concat(L, nelems);
  CLS_LOG(loglevel, "%s", lua_tostring(L, -1));

  /* concat leaves result at top of stack */
  return 1;
}

static char clslua_registered_handle_reg_key;

/*
 * Register a function to be used as a handler target
 */
static int clslua_register(lua_State *L)
{
  luaL_checktype(L, 1, LUA_TFUNCTION);

  /* get table of registered handlers */
  lua_pushlightuserdata(L, &clslua_registered_handle_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);
  assert(lua_type(L, -1) == LUA_TTABLE);

  /* lookup function argument */
  lua_pushvalue(L, 1);
  lua_gettable(L, -2);

  if (lua_isnil(L, -1)) {
    lua_pushvalue(L, 1);
    lua_pushvalue(L, 1);
    lua_settable(L, -4);
  } else {
    lua_pushstring(L, "Cannot register handler more than once");
    return lua_error(L);
  }

  return 0;
}

/*
 * Check if a function is registered as a handler
 */
static void clslua_check_registered_handler(lua_State *L)
{
  luaL_checktype(L, -1, LUA_TFUNCTION);

  /* get table of registered handlers */
  lua_pushlightuserdata(L, &clslua_registered_handle_reg_key);
  lua_gettable(L, LUA_REGISTRYINDEX);
  assert(lua_type(L, -1) == LUA_TTABLE);

  /* lookup function argument */
  lua_pushvalue(L, -2);
  lua_gettable(L, -2);

  if (!lua_rawequal(L, -1, -3)) {
    lua_pushstring(L, "Handler is not registered");
    lua_error(L);
  }

  lua_pop(L, 2);
}

/*
 * Handle result of a cls_cxx_* call. If @ok is non-zero then we return with
 * the number of Lua return arguments on the stack. Otherwise we save error
 * information in the registry and throw a Lua error.
 */
int clslua_opresult(lua_State *L, int ok, int ret, int nargs,
    bool error_on_stack)
{
  struct clslua_err *err = clslua_checkerr(L);

  assert(err);
  if (err->error) {
    CLS_ERR("error: cls_lua state machine: unexpected error");
    assert(0);
  }

  /* everything is cherry */
  if (ok)
    return nargs;

  /* set error in registry */
  err->error = true;
  err->ret = ret;

  /* push error message */
  if (!error_on_stack)
    lua_pushfstring(L, "%s", strerror(ret));

  return lua_error(L);
}

/*
 * Functions registered in the 'cls' module.
 */
vector<luaL_Reg> clslua_lib;

/*
 * Lua files to add to the environment
 */
vector<string> clslua_env;

/*
 * Set int const in table at top of stack
 */
#define SET_INT_CONST(var) do { \
  lua_pushinteger(L, var); \
  lua_setfield(L, -2, #var); \
} while (0)

/*
 * Add default functions and setup errors
 */
static int luaopen_objclass(lua_State *L)
{
  lua_newtable(L);

  /*
   * Add the functions for the core Lua sandbox wrapper
   */
  clslua_lib.push_back({"register", clslua_register});
  clslua_lib.push_back({"log", clslua_log});
  clslua_lib.push_back({NULL, NULL});

  /*
   * Register cls functions (cls.log, etc...)
   */
  luaL_setfuncs(L, &clslua_lib[0], 0);

  /*
   * Register generic errno values under 'cls'
   */
  SET_INT_CONST(EPERM);
  SET_INT_CONST(ENOENT);
  SET_INT_CONST(ESRCH);
  SET_INT_CONST(EINTR);
  SET_INT_CONST(EIO);
  SET_INT_CONST(ENXIO);
  SET_INT_CONST(E2BIG);
  SET_INT_CONST(ENOEXEC);
  SET_INT_CONST(EBADF);
  SET_INT_CONST(ECHILD);
  SET_INT_CONST(EAGAIN);
  SET_INT_CONST(ENOMEM);
  SET_INT_CONST(EACCES);
  SET_INT_CONST(EFAULT);
  SET_INT_CONST(EBUSY);
  SET_INT_CONST(EEXIST);
  SET_INT_CONST(EXDEV);
  SET_INT_CONST(ENODEV);
  SET_INT_CONST(ENOTDIR);
  SET_INT_CONST(EISDIR);
  SET_INT_CONST(EINVAL);
  SET_INT_CONST(ENFILE);
  SET_INT_CONST(EMFILE);
  SET_INT_CONST(ENOTTY);
  SET_INT_CONST(EFBIG);
  SET_INT_CONST(ENOSPC);
  SET_INT_CONST(ESPIPE);
  SET_INT_CONST(EROFS);
  SET_INT_CONST(EMLINK);
  SET_INT_CONST(EPIPE);
  SET_INT_CONST(EDOM);
  SET_INT_CONST(ERANGE);

  return 1;
}

/*
 * Setup the execution environment. Our sandbox currently is not
 * sophisticated. With a new Lua state per-request we don't need to work about
 * users stepping on each other, but we do rip out access to the local file
 * system. All this will change when/if we decide to use some shared Lua
 * states, most likely for performance reasons.
 */
static void clslua_setup_env(lua_State *L)
{
  luaL_requiref(L, "_G", luaopen_base, 1);
  lua_pop(L, 1);

  /*
   * Wrap `pcall` to intercept errors. First save a reference to the default
   * Lua `pcall` function, and then replace `pcall` with our version.
   */
  lua_pushlightuserdata(L, &clslua_pcall_reg_key);
  lua_getglobal(L, "pcall");
  lua_settable(L, LUA_REGISTRYINDEX);

  lua_pushcfunction(L, clslua_pcall);
  lua_setglobal(L, "pcall");

  /* mask unsafe */
  lua_pushnil(L);
  lua_setglobal(L, "loadfile");

  /* mask unsafe */
  lua_pushnil(L);
  lua_setglobal(L, "dofile");

  /* not integrated into our error handling */
  lua_pushnil(L);
  lua_setglobal(L, "xpcall");

  luaL_requiref(L, LUA_TABLIBNAME, luaopen_table, 1);
  lua_pop(L, 1);

  luaL_requiref(L, LUA_STRLIBNAME, luaopen_string, 1);
  lua_pop(L, 1);

  luaL_requiref(L, LUA_MATHLIBNAME, luaopen_math, 1);
  lua_pop(L, 1);

  luaL_requiref(L, lua_clslibname.c_str(), luaopen_objclass, 1);
  lua_pop(L, 1);

  luaL_requiref(L, "bufferlist", luaopen_bufferlist, 1);
  lua_pop(L, 1);

  // built into liblua convenience library (see: src/lua)
  luaL_requiref(L, "cmsgpack", luaopen_cmsgpack, 1);
  lua_pop(L, 1);
  
  // load the environment helper Lua files
  for(vector<string>::iterator it = clslua_env.begin();
      it != clslua_env.end();
      it++) {
    string fname = "/usr/local/share/ceph/rados-classes/";
    fname.append(*it);
    CLS_LOG(20, "Add to env, fname=%s", fname.c_str());
    int status = luaL_loadfile(L, fname.c_str());
    if (status) {
      CLS_ERR("error: couldn't load the file file: %s", lua_tostring(L, -1));
    } else {
      status = lua_pcall(L, 0, 0, 0);
      if (status)
        CLS_ERR("error: couldn't prime function in loaded file: %s", lua_tostring(L, -1));
    }
  }
}

/*
 * Schema:
 * {
 *   "script": "...",
 *   "handler": "...",
 *   "input": "..." # optional
 * }
 */
static int unpack_json_command(lua_State *L, struct clslua_hctx *ctx,
    std::string& script, std::string& handler, std::string& input,
    size_t *input_len)
{
  std::string json_input(ctx->inbl->c_str());
  json_spirit::mValue value;

  if (!json_spirit::read(json_input, value)) {
    CLS_ERR("error: unparseable JSON");
    ctx->ret = -EINVAL;
    return 1;
  }

  if (value.type() != json_spirit::obj_type) {
    CLS_ERR("error: input not a JSON object");
    ctx->ret = -EINVAL;
    return 1;
  }
  json_spirit::mObject obj = value.get_obj();

  // grab the script
  std::map<std::string, json_spirit::mValue>::const_iterator it = obj.find("script");
  if (it == obj.end()) {
    CLS_ERR("error: 'script' field found in JSON object");
    ctx->ret = -EINVAL;
    return 1;
  }

  if (it->second.type() != json_spirit::str_type) {
    CLS_ERR("error: script is not a string");
    ctx->ret = -EINVAL;
    return 1;
  }
  script = it->second.get_str();

  // grab the target function/handler name
  it = obj.find("handler");
  if (it == obj.end()) {
    CLS_ERR("error: no target handler found in JSON object");
    ctx->ret = -EINVAL;
    return 1;
  }

  if (it->second.type() != json_spirit::str_type) {
    CLS_ERR("error: target handler is not a string");
    ctx->ret = -EINVAL;
    return 1;
  }
  handler = it->second.get_str();

  // grab the input (optional)
  it = obj.find("input");
  if (it != obj.end()) {
    if (it->second.type() != json_spirit::str_type) {
      CLS_ERR("error: handler input is not a string");
      ctx->ret = -EINVAL;
      return 1;
    }
    input = it->second.get_str();
    *input_len = input.size();
  }

  return 0;
}

/*
 * Runs the script, and calls handler.
 */
static int clslua_eval(lua_State *L)
{
  struct clslua_hctx *ctx = __clslua_get_hctx(L);
  ctx->ret = -EIO; /* assume failure */

  /*
   * Load modules, errno value constants, and other environment goodies. Must
   * be done before loading/compiling the chunk.
   */
  clslua_setup_env(L);

  /*
   * Deserialize the input that contains the script, the name of the handler
   * to call, and the handler input.
   */
  switch (ctx->in_enc) {
    case JSON_ENC:
      {
        std::string input_str;
        size_t input_str_len = 0;

        // if there is an error decoding json then ctx->ret will be set and we
        // return normally from this function.
        if (unpack_json_command(L, ctx, ctx->script, ctx->handler, input_str,
              &input_str_len))
          return 0;

        bufferptr bp(input_str.c_str(), input_str_len);
        ctx->input.push_back(bp);
      }
      break;

    case BUFFERLIST_ENC:
      {
        cls_lua_eval_op op;

        try {
          bufferlist::iterator it = ctx->inbl->begin();
          ::decode(op, it);
        } catch (const buffer::error &err) {
          CLS_ERR("error: could not decode ceph encoded input");
          ctx->ret = -EINVAL;
          return 0;
        }

        ctx->script.swap(op.script);
        ctx->handler.swap(op.handler);
        ctx->input = op.input;
      }
      break;

    default:
      CLS_ERR("error: unknown encoding type");
      ctx->ret = -EFAULT;
      assert(0);
      return 0;
  }

  /*
   * Create table to hold registered (valid) handlers.
   *
   * Must be done before running the script for the first time because the
   * script will immediately try to register one or more handlers using
   * cls.register(function), which depends on this table.
   */
  lua_pushlightuserdata(L, &clslua_registered_handle_reg_key);
  lua_newtable(L);
  lua_settable(L, LUA_REGISTRYINDEX);

  /* load and compile chunk */
  if (luaL_loadstring(L, ctx->script.c_str()))
    return lua_error(L);

  /* execute chunk */
  lua_call(L, 0, 0);

  /* no error, but nothing left to do */
  if (!ctx->handler.size()) {
    CLS_LOG(10, "no handler name provided");
    ctx->ret = 0; /* success */
    return 0;
  }

  lua_getglobal(L, ctx->handler.c_str());
  if (lua_type(L, -1) != LUA_TFUNCTION) {
    CLS_ERR("error: unknown handler or not function: %s", ctx->handler.c_str());
    ctx->ret = -EOPNOTSUPP;
    return 0;
  }

  /* throw error if function is not registered */
  clslua_check_registered_handler(L);

  /* setup the input/output bufferlists */
  clslua_pushbufferlist(L, &ctx->input);
  clslua_pushbufferlist(L, ctx->outbl);

  /*
   * Call the target Lua object class handler. If the call is successful then
   * we will examine the return value here and store it in the context. Errors
   * that occur are handled in the top-level eval() function.
   */
  int top = lua_gettop(L);
  lua_call(L, 2, LUA_MULTRET);

  /* store return value in context */
  if (!(lua_gettop(L) + 3 - top))
    lua_pushinteger(L, 0);
  ctx->ret = luaL_checkinteger(L, -1);

  return 0;
}

/*
 * Main handler. Proxies the Lua VM and the Lua-defined handler.
 */
int eval_generic(cls_method_context_t hctx, bufferlist *in, bufferlist *out,
    InputEncoding in_enc)
{
  struct clslua_hctx ctx;
  lua_State *L = NULL;
  int ret = -EIO;

  /* stash context for use in Lua VM */
  ctx.hctx = &hctx;
  ctx.inbl = in;
  ctx.in_enc = in_enc;
  ctx.outbl = out;
  ctx.error.error = false;

  /* build lua vm state */
  L = luaL_newstate();
  if (!L) {
    CLS_ERR("error creating new Lua state");
    goto out;
  }

  /* panic handler for unhandled errors */
  lua_atpanic(L, &cls_lua_atpanic);

  if (setjmp(cls_lua_panic_jump) == 0) {

    /*
     * Stash the handler context in the register. It contains the objclass
     * method context, global error state, and the command and reply structs.
     */
    lua_pushlightuserdata(L, &clslua_hctx_reg_key);
    lua_pushlightuserdata(L, &ctx);
    lua_settable(L, LUA_REGISTRYINDEX);

    /* Process the input and run the script */
    lua_pushcfunction(L, clslua_eval);
    ret = lua_pcall(L, 0, 0, 0);

    /* Encountered an error? */
    if (ret) {
      struct clslua_err *err = clslua_checkerr(L);
      if (!err) {
        CLS_ERR("error: cls_lua state machine: unexpected error");
        assert(0);
      }

      /* Error origin a cls_cxx_* method? */
      if (err->error) {
        ret = err->ret; /* cls_cxx_* return value */

        /* Errors always abort. Fix up ret and log error */
        if (ret >= 0) {
          CLS_ERR("error: unexpected handler return value");
          ret = -EFAULT;
        }

      } else
        ret = -EIO; /* Generic error code */

      CLS_ERR("error: %s", lua_tostring(L, -1));

    } else {
      /*
       * No Lua error encountered while running the script, but the handler
       * may still have returned an error code (e.g. an errno value).
       */
      ret = ctx.ret;
    }

  } else {
    CLS_ERR("error: recovering from Lua panic");
    ret = -EFAULT;
  }

out:
  if (L)
    lua_close(L);
  return ret;
}

static int eval_json(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  return eval_generic(hctx, in, out, JSON_ENC);
}

static int eval_bufferlist(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  return eval_generic(hctx, in, out, BUFFERLIST_ENC);
}

void cls_init(string cls_name, string cls_class, list<luaL_Reg> functions, list<string> env)
{
  CLS_LOG(20, "Loaded lua class!");
  lua_clslibname = cls_class;

  clslua_lib.clear();
  clslua_env.clear();
  clslua_lib.insert(clslua_lib.end(), functions.begin(), functions.end());
  clslua_env.insert(clslua_env.end(), env.begin(), env.end());

  cls_register(cls_name.c_str(), &h_class);

  cls_register_cxx_method(h_class, "eval_json",
      CLS_METHOD_RD | CLS_METHOD_WR, eval_json, &h_eval_json);

  cls_register_cxx_method(h_class, "eval_bufferlist",
      CLS_METHOD_RD | CLS_METHOD_WR, eval_bufferlist, &h_eval_bufferlist);
}
