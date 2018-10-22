#include <errno.h>
#include <lua.hpp>
#include "include/types.h"
#include "include/rados/librados.h"
#include "gtest/gtest.h"
#include "test/librados/test.h"
#include "cls/lua/cls_lua_client.h"
#include "cls/lua/cls_lua.h"

/*
 * JSON script to test JSON I/O protocol with cls_lua
 */
const std::string json_test_script = R"jsonscript(
{
  "script": "function json_echo(input, output) output:append(input:str()); end objclass.register(json_echo)",
  "handler": "json_echo",
  "input": "omg it works",
}
)jsonscript";

/*
 * Lua test script thanks to the magical c++11 string literal syntax
 */
const std::string test_script = R"luascript(
--
-- This Lua script file contains all of the handlers used in the cls_lua unit
-- tests (src/test/cls_lua/test_cls_lua.cc). Each section header corresponds
-- to the ClsLua.XYZ test.
--

--
-- Read
--
function read(input, output)
  size = objclass.stat()
  bl = objclass.read(0, size)
  output:append(bl:str())
end

objclass.register(read)

--
-- Write
--
function write(input, output)
  objclass.write(0, #input, input)
end

objclass.register(write)

--
-- MapGetVal
--
function map_get_val_foo(input, output)
  bl = objclass.map_get_val('foo')
  output:append(bl:str())
end

function map_get_val_dne()
  bl = objclass.map_get_val('dne')
end

objclass.register(map_get_val_foo)
objclass.register(map_get_val_dne)

--
-- Stat
--
function stat_ret(input, output)
  size, mtime = objclass.stat()
  output:append(size .. "," .. mtime)
end

function stat_sdne()
  size, mtime = objclass.stat()
end

function stat_sdne_pcall()
  ok, ret, size, mtime = pcall(objclass.stat, o)
  assert(ok == false)
  assert(ret == -objclass.ENOENT)
  return ret
end

objclass.register(stat_ret)
objclass.register(stat_sdne)
objclass.register(stat_sdne_pcall)

--
-- RetVal
--
function rv_h() end
function rv_h1() return 1; end
function rv_h0() return 0; end
function rv_hn1() return -1; end
function rv_hs1() return '1'; end
function rv_hs0() return '0'; end
function rv_hsn1() return '-1'; end
function rv_hnil() return nil; end
function rv_ht() return {}; end
function rv_hstr() return 'asdf'; end

objclass.register(rv_h)
objclass.register(rv_h1)
objclass.register(rv_h0)
objclass.register(rv_hn1)
objclass.register(rv_hs1)
objclass.register(rv_hs0)
objclass.register(rv_hsn1)
objclass.register(rv_hnil)
objclass.register(rv_ht)
objclass.register(rv_hstr)

--
-- Create
--
function create_c() objclass.create(true); end
function create_cne() objclass.create(false); end

objclass.register(create_c)
objclass.register(create_cne)

--
-- Pcall
--
function pcall_c() objclass.create(true); end

function pcall_pc()
  ok, ret = pcall(objclass.create, true)
  assert(ok == false)
  assert(ret == -objclass.EEXIST)
end

function pcall_pcr()
  ok, ret = pcall(objclass.create, true)
  assert(ok == false)
  assert(ret == -objclass.EEXIST)
  return ret
end

function pcall_pcr2()
  ok, ret = pcall(objclass.create, true)
  assert(ok == false)
  assert(ret == -objclass.EEXIST)
  ok, ret = pcall(objclass.create, true)
  assert(ok == false)
  assert(ret == -objclass.EEXIST)
  return -9999
end

objclass.register(pcall_c)
objclass.register(pcall_pc)
objclass.register(pcall_pcr)
objclass.register(pcall_pcr2)

--
-- Remove
--
function remove_c() objclass.create(true); end
function remove_r() objclass.remove(); end

objclass.register(remove_c)
objclass.register(remove_r)

--
-- MapSetVal
--
function map_set_val(input, output)
  objclass.map_set_val('foo', input)
end

objclass.register(map_set_val)

--
-- MapClear
--
function map_clear()
  objclass.map_clear()
end

objclass.register(map_clear)

--
-- BufferlistEquality
--
function bl_eq_empty_equal(input, output)
  bl1 = bufferlist.new()
  bl2 = bufferlist.new()
  assert(bl1 == bl2)
end

function bl_eq_empty_selfequal()
  bl1 = bufferlist.new()
  assert(bl1 == bl1)
end

function bl_eq_selfequal()
  bl1 = bufferlist.new()
  bl1:append('asdf')
  assert(bl1 == bl1)
end

function bl_eq_equal()
  bl1 = bufferlist.new()
  bl2 = bufferlist.new()
  bl1:append('abc')
  bl2:append('abc')
  assert(bl1 == bl2)
end

function bl_eq_notequal()
  bl1 = bufferlist.new()
  bl2 = bufferlist.new()
  bl1:append('abc')
  bl2:append('abcd')
  assert(bl1 ~= bl2)
end

objclass.register(bl_eq_empty_equal)
objclass.register(bl_eq_empty_selfequal)
objclass.register(bl_eq_selfequal)
objclass.register(bl_eq_equal)
objclass.register(bl_eq_notequal)

--
-- Bufferlist Compare
--
function bl_lt()
  local a = bufferlist.new()
  local b = bufferlist.new()
  a:append('A')
  b:append('B')
  assert(a < b)
end

function bl_le()
  local a = bufferlist.new()
  local b = bufferlist.new()
  a:append('A')
  b:append('B')
  assert(a <= b)
end

objclass.register(bl_lt)
objclass.register(bl_le)

--
-- Bufferlist concat
--
function bl_concat_eq()
  local a = bufferlist.new()
  local b = bufferlist.new()
  local ab = bufferlist.new()
  a:append('A')
  b:append('B')
  ab:append('AB')
  assert(a .. b == ab)
end

function bl_concat_ne()
  local a = bufferlist.new()
  local b = bufferlist.new()
  local ab = bufferlist.new()
  a:append('A')
  b:append('B')
  ab:append('AB')
  assert(b .. a ~= ab)
end

function bl_concat_immut()
  local a = bufferlist.new()
  local b = bufferlist.new()
  local ab = bufferlist.new()
  a:append('A')
  b:append('B')
  ab:append('AB')
  x = a .. b
  assert(x == ab)
  b:append('C')
  assert(x == ab)
  local bc = bufferlist.new()
  bc:append('BC')
  assert(b == bc)
end

objclass.register(bl_concat_eq)
objclass.register(bl_concat_ne)
objclass.register(bl_concat_immut)

--
-- RunError
--
function runerr_a()
  error('WTF')
end

function runerr_b()
  runerr_a()
end

function runerr_c()
  runerr_b()
end

-- only runerr_c is called
objclass.register(runerr_c)

--
-- GetXattr
--
function getxattr(input, output)
  bl = objclass.getxattr("fooz")
  output:append(bl:str())
end

objclass.register(getxattr)

--
-- SetXattr
--
function setxattr(input, output)
  objclass.setxattr("fooz2", input)
end

objclass.register(setxattr)

--
-- WriteFull
--
function write_full(input, output)
    objclass.write_full(input)
end

objclass.register(write_full)

--
-- GetXattrs
--
function getxattrs(input, output)
    -- result
    xattrs = objclass.getxattrs()

    -- sort for determisitic test
    arr = {}
    for n in pairs(xattrs) do
        table.insert(arr, n)
    end
    table.sort(arr)

    output_str = ""
    for i,key in ipairs(arr) do
        output_str = output_str .. key .. "/" .. xattrs[key]:str() .. "/"
    end
    output:append(output_str)
end

objclass.register(getxattrs)

--
-- MapGetKeys
--
function map_get_keys(input, output)
    -- result
    keys = objclass.map_get_keys("", 5)

    -- sort for determisitic test
    arr = {}
    for n in pairs(keys) do
        table.insert(arr, n)
    end
    table.sort(arr)

    output_str = ""
    for i,key in ipairs(arr) do
        output_str = output_str .. key .. "/"
    end

    output:append(output_str)
end

objclass.register(map_get_keys)

--
-- MapGetVals
--
function map_get_vals(input, output)
    -- result
    kvs = objclass.map_get_vals("", "", 10)

    -- sort for determisitic test
    arr = {}
    for n in pairs(kvs) do
        table.insert(arr, n)
    end
    table.sort(arr)

    output_str = ""
    for i,key in ipairs(arr) do
        output_str = output_str .. key .. "/" .. kvs[key]:str() .. "/"
    end
    output:append(output_str)
end

objclass.register(map_get_vals)

--
-- MapHeader (write)
--
function map_write_header(input, output)
    objclass.map_write_header(input)
end

objclass.register(map_write_header)

--
-- MapHeader (read)
--
function map_read_header(input, output)
    hdr = objclass.map_read_header()
    output:append(hdr:str())
end

objclass.register(map_read_header)

--
-- MapSetVals
--
function map_set_vals_empty(input, output)
    record = {}
    objclass.map_set_vals(record)
end

function map_set_vals_one(input, output)
    record = {
        a = "a_val"
    }
    objclass.map_set_vals(record)
end

function map_set_vals_two(input, output)
    record = {
        a = "a_val",
        b = "b_val"
    }
    objclass.map_set_vals(record)
end

function map_set_vals_three(input, output)
    record = {
        a = "a_val",
        b = "b_val",
        c = "c_val"
    }
    objclass.map_set_vals(record)
end

function map_set_vals_array(input, output)
    array = {}
    array[1] = "1_val"
    array[2] = "2_val"
    objclass.map_set_vals(array)
end

function map_set_vals_mixed(input, output)
    record = {
        a = "a_val",
        b = "b_val"
    }
    record[1] = "1_val"
    record[2] = "2_val"
    objclass.map_set_vals(record)
end

function map_set_vals_bad_val(input, output)
    record = {
        a = {}
    }
    objclass.map_set_vals(record)
end

objclass.register(map_set_vals_empty)
objclass.register(map_set_vals_one)
objclass.register(map_set_vals_two)
objclass.register(map_set_vals_three)
objclass.register(map_set_vals_array)
objclass.register(map_set_vals_mixed)
objclass.register(map_set_vals_bad_val)

--
-- MapRemoveKey
--
function map_remove_key(input, output)
    objclass.map_remove_key("a")
end

objclass.register(map_remove_key)

--
-- Version/Subop
--
function current_version(input, output)
    ret = objclass.current_version()
    output:append("" .. ret)
    objclass.log(0, ret)
end

function current_subop_num(input, output)
    ret = objclass.current_subop_num()
    output:append("" .. ret)
    objclass.log(0, ret)
end

function current_subop_version(input, output)
    ret = objclass.current_subop_version()
    output:append("" .. ret)
    objclass.log(0, ret)
end

objclass.register(current_version)
objclass.register(current_subop_num)
objclass.register(current_subop_version)

)luascript";

/*
 * Test harness uses single pool for the entire test case, and generates
 * unique object names for each test.
 */
class ClsLua : public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      pool_name = get_temp_pool_name();
      ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
      ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
    }

    static void TearDownTestCase() {
      ioctx.close();
      ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
    }

    void SetUp() override {
      /* Grab test names to build unique objects */
      const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();

      /* Create unique string using test/testname/pid */
      std::stringstream ss_oid;
      ss_oid << test_info->test_case_name() << "_" <<
        test_info->name() << "_" << getpid();

      /* Unique object for test to use */
      oid = ss_oid.str();
    }

    void TearDown() override {
    }

    /*
     * Helper function. This functionality should eventually make its way into
     * a clslua client library of some sort.
     */
    int __clslua_exec(const string& oid, const string& script,
        librados::bufferlist *input = NULL,  const string& funcname = "")
    {
      bufferlist inbl;
      if (input)
        inbl = *input;

      reply_output.clear();

      return cls_lua_client::exec(ioctx, oid, script, funcname, inbl,
          reply_output);
    }

    int clslua_exec(const string& script, librados::bufferlist *input = NULL,
        const string& funcname = "")
    {
      return __clslua_exec(oid, script, input, funcname);
    }

    static librados::Rados rados;
    static librados::IoCtx ioctx;
    static string pool_name;

    string oid;
    bufferlist reply_output;
};

librados::Rados ClsLua::rados;
librados::IoCtx ClsLua::ioctx;
string ClsLua::pool_name;

TEST_F(ClsLua, Write) {
  /* write some data into object */
  string written = "Hello World";
  bufferlist inbl;
  encode(written, inbl);
  ASSERT_EQ(0, clslua_exec(test_script, &inbl, "write"));

  /* have Lua read out of the object */
  uint64_t size;
  bufferlist outbl;
  ASSERT_EQ(0, ioctx.stat(oid, &size, NULL));
  ASSERT_EQ(size, (uint64_t)ioctx.read(oid, outbl, size, 0) );

  /* compare what Lua read to what we wrote */
  string read;
  decode(read, outbl);
  ASSERT_EQ(read, written);
}

TEST_F(ClsLua, SyntaxError) {
  ASSERT_EQ(-EIO, clslua_exec("-"));
}

TEST_F(ClsLua, EmptyScript) {
  ASSERT_EQ(0, clslua_exec(""));
}

TEST_F(ClsLua, RetVal) {
  /* handlers can return numeric values */
  ASSERT_EQ(1, clslua_exec(test_script, NULL, "rv_h1"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "rv_h0"));
  ASSERT_EQ(-1, clslua_exec(test_script, NULL, "rv_hn1"));
  ASSERT_EQ(1, clslua_exec(test_script, NULL, "rv_hs1"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "rv_hs0"));
  ASSERT_EQ(-1, clslua_exec(test_script, NULL, "rv_hsn1"));

  /* no return value is success */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "rv_h"));

  /* non-numeric return values are errors */
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "rv_hnil"));
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "rv_ht"));
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "rv_hstr"));
}

TEST_F(ClsLua, Create) {
  /* create works */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "create_c"));

  /* exclusive works */
  ASSERT_EQ(-EEXIST, clslua_exec(test_script, NULL, "create_c"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "create_cne"));
}

TEST_F(ClsLua, Pcall) {
  /* create and error works */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "pcall_c"));
  ASSERT_EQ(-EEXIST, clslua_exec(test_script, NULL, "pcall_c"));

  /* pcall masks the error */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "pcall_pc"));

  /* pcall lets us get the failed return value */
  ASSERT_EQ(-EEXIST, clslua_exec(test_script, NULL, "pcall_pcr"));

  /*
   * the first call in pcr2 will fail (check ret != 0), and the second pcall
   * should also fail (we check with a bogus return value to mask real
   * errors). This is also an important check for our error handling because
   * we need a case where two functions in the same handler fail to exercise
   * our internal error book keeping.
   */
  ASSERT_EQ(-9999, clslua_exec(test_script, NULL, "pcall_pcr2"));
}

TEST_F(ClsLua, Remove) {
  /* object doesn't exist */
  ASSERT_EQ(-ENOENT, clslua_exec(test_script, NULL, "remove_r"));

  /* can remove */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "remove_c"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "remove_r"));
  ASSERT_EQ(-ENOENT, clslua_exec(test_script, NULL, "remove_r"));
}

TEST_F(ClsLua, Stat) {
  /* build object and stat */
  char buf[1024];
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write_full(oid, bl));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat(oid, &size, &mtime));

  /* test stat success */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "stat_ret"));

  // size,mtime from ioctx call
  std::stringstream s1;
  s1 << size << "," << mtime;

  // lua constructed size,mtime string
  std::string s2(reply_output.c_str(), reply_output.length());

  ASSERT_EQ(s1.str(), s2);

  /* test object dne */
  ASSERT_EQ(-ENOENT, __clslua_exec("dne", test_script, NULL, "stat_sdne"));

  /* can capture error with pcall */
  ASSERT_EQ(-ENOENT, __clslua_exec("dne", test_script, NULL, "stat_sdne_pcall"));
}

TEST_F(ClsLua, MapClear) {
  /* write some data into a key */
  string msg = "This is a test message";
  bufferlist val;
  val.append(msg.c_str(), msg.size());
  map<string, bufferlist> map;
  map["foo"] = val;
  ASSERT_EQ(0, ioctx.omap_set(oid, map));

  /* test we can get it back out */
  set<string> keys;
  keys.insert("foo");
  map.clear();
  ASSERT_EQ(0, (int)map.count("foo"));
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, keys, &map));
  ASSERT_EQ(1, (int)map.count("foo"));

  /* now clear it */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_clear"));

  /* test that the map we get back is empty now */
  map.clear();
  ASSERT_EQ(0, (int)map.count("foo"));
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, keys, &map));
  ASSERT_EQ(0, (int)map.count("foo"));
}

TEST_F(ClsLua, MapSetVal) {
  /* build some input value */
  bufferlist orig_val;
  encode("this is the original value yay", orig_val);

  /* have the lua script stuff the data into a map value */
  ASSERT_EQ(0, clslua_exec(test_script, &orig_val, "map_set_val"));

  /* grap the key now and compare to orig */
  map<string, bufferlist> out_map;
  set<string> out_keys;
  out_keys.insert("foo");
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, out_keys, &out_map));
  bufferlist out_bl = out_map["foo"];
  string out_val;
  decode(out_val, out_bl);
  ASSERT_EQ(out_val, "this is the original value yay");
}

TEST_F(ClsLua, MapGetVal) {
  /* write some data into a key */
  string msg = "This is a test message";
  bufferlist orig_val;
  orig_val.append(msg.c_str(), msg.size());
  map<string, bufferlist> orig_map;
  orig_map["foo"] = orig_val;
  ASSERT_EQ(0, ioctx.omap_set(oid, orig_map));

  /* now compare to what we put it */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_val_foo"));

  /* check return */
  string ret_val;
  ret_val.assign(reply_output.c_str(), reply_output.length());
  ASSERT_EQ(ret_val, msg);

  /* error case */
  ASSERT_EQ(-ENOENT, clslua_exec(test_script, NULL, "map_get_val_dne"));
}

TEST_F(ClsLua, Read) {
  /* put data into object */
  string msg = "This is a test message";
  bufferlist bl;
  bl.append(msg.c_str(), msg.size());
  ASSERT_EQ(0, ioctx.write_full(oid, bl));

  /* get lua to read it and send it back */
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "read"));

  /* check return */
  string ret_val;
  ret_val.assign(reply_output.c_str(), reply_output.length());
  ASSERT_EQ(ret_val, msg);
}

TEST_F(ClsLua, Log) {
  ASSERT_EQ(0, clslua_exec("objclass.log()"));
  ASSERT_EQ(0, clslua_exec("s = objclass.log(); objclass.log(s);"));
  ASSERT_EQ(0, clslua_exec("objclass.log(1)"));
  ASSERT_EQ(0, clslua_exec("objclass.log(-1)"));
  ASSERT_EQ(0, clslua_exec("objclass.log('x')"));
  ASSERT_EQ(0, clslua_exec("objclass.log(0, 0)"));
  ASSERT_EQ(0, clslua_exec("objclass.log(1, 1)"));
  ASSERT_EQ(0, clslua_exec("objclass.log(-10, -10)"));
  ASSERT_EQ(0, clslua_exec("objclass.log('x', 'y')"));
  ASSERT_EQ(0, clslua_exec("objclass.log(1, 'one')"));
  ASSERT_EQ(0, clslua_exec("objclass.log(1, 'one', 'two')"));
  ASSERT_EQ(0, clslua_exec("objclass.log('one', 'two', 'three')"));
  ASSERT_EQ(0, clslua_exec("s = objclass.log('one', 'two', 'three'); objclass.log(s);"));
}

TEST_F(ClsLua, BufferlistEquality) {
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_empty_equal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_empty_selfequal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_selfequal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_equal"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_eq_notequal"));
}

TEST_F(ClsLua, RunError) {
  ASSERT_EQ(-EIO, clslua_exec(test_script, NULL, "runerr_c"));
}

TEST_F(ClsLua, HandleNotFunc) {
  string script = "x = 1;";
  ASSERT_EQ(-EOPNOTSUPP, clslua_exec(script, NULL, "x"));
}

TEST_F(ClsLua, Register) {
  /* normal cases: register and maybe call the handler */
  string script = "function h() end; objclass.register(h);";
  ASSERT_EQ(0, clslua_exec(script, NULL, ""));
  ASSERT_EQ(0, clslua_exec(script, NULL, "h"));

  /* can register and call multiple handlers */
  script = "function h1() end; function h2() end;"
    "objclass.register(h1); objclass.register(h2);";
  ASSERT_EQ(0, clslua_exec(script, NULL, ""));
  ASSERT_EQ(0, clslua_exec(script, NULL, "h1"));
  ASSERT_EQ(0, clslua_exec(script, NULL, "h2"));

  /* normal cases: register before function is defined */
  script = "objclass.register(h); function h() end;";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, "h"));

  /* cannot call handler that isn't registered */
  script = "function h() end;";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, "h"));

  /* handler doesn't exist */
  script = "objclass.register(lalala);";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));

  /* handler isn't a function */
  script = "objclass.register('some string');";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));

  /* cannot register handler multiple times */
  script = "function h() end; objclass.register(h); objclass.register(h);";
  ASSERT_EQ(-EIO, clslua_exec(script, NULL, ""));
}

TEST_F(ClsLua, BufferlistCompare) {
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_lt"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_le"));
}

TEST_F(ClsLua, BufferlistConcat) {
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_concat_eq"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_concat_ne"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "bl_concat_immut"));
}

TEST_F(ClsLua, GetXattr) {
  bufferlist bl;
  bl.append("blahblahblahblahblah");
  ASSERT_EQ(0, ioctx.setxattr(oid, "fooz", bl));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "getxattr"));
  ASSERT_TRUE(reply_output == bl);
}

TEST_F(ClsLua, SetXattr) {
  bufferlist inbl;
  inbl.append("blahblahblahblahblah");
  ASSERT_EQ(0, clslua_exec(test_script, &inbl, "setxattr"));
  bufferlist outbl;
  ASSERT_EQ((int)inbl.length(), ioctx.getxattr(oid, "fooz2", outbl));
  ASSERT_TRUE(outbl == inbl);
}

TEST_F(ClsLua, WriteFull) {
  // write some data
  char buf[1024];
  bufferlist blin;
  blin.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write(oid, blin, blin.length(), 0));
  bufferlist blout;
  ASSERT_EQ((int)blin.length(), ioctx.read(oid, blout, 0, 0));
  ASSERT_EQ(blin, blout);

  // execute write_full from lua
  blin.clear();
  char buf2[200];
  sprintf(buf2, "%s", "data replacing content");
  blin.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, clslua_exec(test_script, &blin, "write_full"));

  // read it back
  blout.clear();
  ASSERT_EQ((int)blin.length(), ioctx.read(oid, blout, 0, 0));
  ASSERT_EQ(blin, blout);
}

TEST_F(ClsLua, GetXattrs) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "getxattrs"));
  ASSERT_EQ(0, (int)reply_output.length());

  string key1str("key1str");
  bufferlist key1bl;
  key1bl.append(key1str);
  ASSERT_EQ(0, ioctx.setxattr(oid, "key1", key1bl));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "getxattrs"));
  string out1(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out1.c_str(), "key1/key1str/");

  string key2str("key2str");
  bufferlist key2bl;
  key2bl.append(key2str);
  ASSERT_EQ(0, ioctx.setxattr(oid, "key2", key2bl));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "getxattrs"));
  string out2(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out2.c_str(), "key1/key1str/key2/key2str/");

  string key3str("key3str");
  bufferlist key3bl;
  key3bl.append(key3str);
  ASSERT_EQ(0, ioctx.setxattr(oid, "key3", key3bl));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "getxattrs"));
  string out3(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out3.c_str(), "key1/key1str/key2/key2str/key3/key3str/");
}

TEST_F(ClsLua, MapGetKeys) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_keys"));
  ASSERT_EQ(0, (int)reply_output.length());

  map<string, bufferlist> kvpairs;

  kvpairs["k1"] = bufferlist();
  ASSERT_EQ(0, ioctx.omap_set(oid, kvpairs));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_keys"));
  string out1(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out1.c_str(), "k1/");

  kvpairs["k2"] = bufferlist();
  ASSERT_EQ(0, ioctx.omap_set(oid, kvpairs));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_keys"));
  string out2(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out2.c_str(), "k1/k2/");

  kvpairs["xxx"] = bufferlist();
  ASSERT_EQ(0, ioctx.omap_set(oid, kvpairs));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_keys"));
  string out3(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out3.c_str(), "k1/k2/xxx/");
}

TEST_F(ClsLua, MapGetVals) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_vals"));
  ASSERT_EQ(0, (int)reply_output.length());

  map<string, bufferlist> kvpairs;

  kvpairs["key1"] = bufferlist();
  kvpairs["key1"].append(string("key1str"));
  ASSERT_EQ(0, ioctx.omap_set(oid, kvpairs));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_vals"));
  string out1(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out1.c_str(), "key1/key1str/");

  kvpairs["key2"] = bufferlist();
  kvpairs["key2"].append(string("key2str"));
  ASSERT_EQ(0, ioctx.omap_set(oid, kvpairs));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_vals"));
  string out2(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out2.c_str(), "key1/key1str/key2/key2str/");

  kvpairs["key3"] = bufferlist();
  kvpairs["key3"].append(string("key3str"));
  ASSERT_EQ(0, ioctx.omap_set(oid, kvpairs));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_get_vals"));
  string out3(reply_output.c_str(), reply_output.length()); // add the trailing \0
  ASSERT_STREQ(out3.c_str(), "key1/key1str/key2/key2str/key3/key3str/");
}

TEST_F(ClsLua, MapHeader) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  bufferlist bl_out;
  ASSERT_EQ(0, ioctx.omap_get_header(oid, &bl_out));
  ASSERT_EQ(0, (int)bl_out.length());

  std::string val("this is a value");
  bufferlist hdr;
  hdr.append(val);

  ASSERT_EQ(0, clslua_exec(test_script, &hdr, "map_write_header"));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_read_header"));

  ASSERT_EQ(reply_output, hdr);
}

TEST_F(ClsLua, MapSetVals) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_empty"));

  std::map<string, bufferlist> out_vals;
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_one"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(1, (int)out_vals.size());
  ASSERT_STREQ("a_val", std::string(out_vals["a"].c_str(), out_vals["a"].length()).c_str());

  out_vals.clear();
  ASSERT_EQ(0, ioctx.omap_clear(oid));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_two"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(2, (int)out_vals.size());
  ASSERT_STREQ("a_val", std::string(out_vals["a"].c_str(), out_vals["a"].length()).c_str());
  ASSERT_STREQ("b_val", std::string(out_vals["b"].c_str(), out_vals["b"].length()).c_str());

  out_vals.clear();
  ASSERT_EQ(0, ioctx.omap_clear(oid));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_three"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(3, (int)out_vals.size());
  ASSERT_STREQ("a_val", std::string(out_vals["a"].c_str(), out_vals["a"].length()).c_str());
  ASSERT_STREQ("b_val", std::string(out_vals["b"].c_str(), out_vals["b"].length()).c_str());
  ASSERT_STREQ("c_val", std::string(out_vals["c"].c_str(), out_vals["c"].length()).c_str());

  out_vals.clear();
  ASSERT_EQ(0, ioctx.omap_clear(oid));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_array"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(2, (int)out_vals.size());
  ASSERT_STREQ("1_val", std::string(out_vals["1"].c_str(), out_vals["1"].length()).c_str());
  ASSERT_STREQ("2_val", std::string(out_vals["2"].c_str(), out_vals["2"].length()).c_str());

  out_vals.clear();
  ASSERT_EQ(0, ioctx.omap_clear(oid));
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_mixed"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(4, (int)out_vals.size());
  ASSERT_STREQ("a_val", std::string(out_vals["a"].c_str(), out_vals["a"].length()).c_str());
  ASSERT_STREQ("b_val", std::string(out_vals["b"].c_str(), out_vals["b"].length()).c_str());
  ASSERT_STREQ("1_val", std::string(out_vals["1"].c_str(), out_vals["1"].length()).c_str());
  ASSERT_STREQ("2_val", std::string(out_vals["2"].c_str(), out_vals["2"].length()).c_str());

  ASSERT_EQ(-EINVAL, clslua_exec(test_script, NULL, "map_set_vals_bad_val"));
}

TEST_F(ClsLua, MapRemoveKey) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  std::map<string, bufferlist> out_vals;
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_set_vals_two"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(2, (int)out_vals.size());
  ASSERT_STREQ("a_val", std::string(out_vals["a"].c_str(), out_vals["a"].length()).c_str());
  ASSERT_STREQ("b_val", std::string(out_vals["b"].c_str(), out_vals["b"].length()).c_str());

  out_vals.clear();
  ASSERT_EQ(0, clslua_exec(test_script, NULL, "map_remove_key"));
  ASSERT_EQ(0, ioctx.omap_get_vals(oid, "", 100, &out_vals));
  ASSERT_EQ(1, (int)out_vals.size());
  ASSERT_STREQ("b_val", std::string(out_vals["b"].c_str(), out_vals["b"].length()).c_str());
}

TEST_F(ClsLua, VersionSubop) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "current_version"));
  ASSERT_GT((int)reply_output.length(), 0);

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "current_subop_num"));
  ASSERT_GT((int)reply_output.length(), 0);

  ASSERT_EQ(0, clslua_exec(test_script, NULL, "current_subop_version"));
  ASSERT_GT((int)reply_output.length(), 0);
}

TEST_F(ClsLua, Json) {
  ASSERT_EQ(0, ioctx.create(oid, false));

  bufferlist inbl, outbl;

  inbl.append(json_test_script);

  int ret = ioctx.exec(oid, "lua", "eval_json", inbl, outbl);
  ASSERT_EQ(ret, 0);

  std::string out(outbl.c_str(), outbl.length());
  ASSERT_STREQ(out.c_str(), "omg it works");
}
