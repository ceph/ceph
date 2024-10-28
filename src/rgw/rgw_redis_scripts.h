/*
 * ========================================================================
 * THIS FILE IS AUTO-GENERATED. DO NOT EDIT!
 *
 * This file is generated from "rgw_redis_scripts.lua". Update the redis
 * functions in "rgw_redis_scripts.lua" and run cmake to regenerate this.
 * ========================================================================
 */

#pragma once

#include <string>

const std::string RGW_LUA_SCRIPT = R"(#!lua name=rgwlib

--- Linux Error codes
local lerrorCodes = {
    EPERM = 1,
    ENOENT = 2,
    EBUSY = 16,
    EEXIST = 17,
    ENOMEM = 12,
    ENOSPC = 28
}

---
--- Reply for all functions need to be of the format
--- {"errorCode": <error code>, "errorMessage": <error message>, "data": <data>}
---
local function format_response(errorCode, errorMessage, data)
    return {
        map = {
            errorCode = errorCode,
            errorMessage = errorMessage,
            data = data
        }
    }
end

--- 
--- Lock functions
---

--- Assert if the lock is held by the owner of the cookie
--- @param keys table A single element list - lock name
--- @param args table A single-element list - cookie 
--- @return number 0 if the lock is held by the owner of the cookie,
--- -lerrorCodes.EBUSY if the lock is held by another owner, 
--- -lerrorCodes.ENOENT if the lock does not exist
local function assert_lock(keys, args)
    local name = keys[1]
    local cookie = args[1]
    if redis.call('EXISTS', name) == 1 then
        local existing_cookie = redis.call('GET', name)
        if existing_cookie == cookie then
            return format_response(0, "", "")
        else
            return format_response(-lerrorCodes.EBUSY, "Lock is held by another process", "")
        end
    end
    return format_response(-lerrorCodes.ENOENT, "Lock does not exist", "")
end

--- Acquire a lock on a resource.
--- It sets a key with a cookie value if the key does not exist.
--- If the key exists and the value is same as cookie, it extends the lock.
--- If the key exists and the value is different from cookie, it fails.  
---@param keys table A single element list - lock name
---@param args table A two-element list - cookie and timeout
---@return number 0 if the lock is acquired or extended
local function lock(keys, args)
    local name = keys[1]
    local cookie = args[1]
    local timeout = args[2]
    local lock_status = assert_lock(keys, args)
    if lock_status.map.errorCode == 0 then
        redis.call('PEXPIRE', name, timeout)
        return format_response(0, "", "")
    elseif lock_status.map.errorCode == -lerrorCodes.ENOENT then
        redis.call('SET', name, cookie, 'PX', timeout)
        return format_response(0, "", "")
    end
    return lock_status
end

--- Release the lock on a resource.
--- It deletes the key if the value matches the cookie.
---@param keys table A single element list - lock name
---@param args table A single-element list - cookie
local function unlock(keys, args)
    local name = keys[1]
    local cookie = args[1]
    local lock_status = assert_lock(keys, args)
    if lock_status.map.errorCode == 0 then
        redis.call('DEL', name)
        return format_response(0, "", "")
    end
    return lock_status
end

--- Register the functions.
redis.register_function('lock', lock)
redis.register_function('unlock', unlock)
redis.register_function('assert_lock', assert_lock)

-----
--- Queue functions
-----

-- Refer to 'Execution under low memory conditions'
-- in https://redis.io/docs/latest/develop/interact/programmability/eval-intro/
---

local MAPNAME = "2pc:queues"

-- Function to generate a string longer than n by concatenating the charset
local function generateString(n)
    local charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
    local result = {}
    local charset_length = #charset

    local repeats = math.ceil(n / charset_length) + 1
    for i = 1, repeats do
        table.insert(result, charset)
    end
    return table.concat(result)
end

--- Initialize a queue with a given size
--- @param keys table A  single element list - queue set name
--- @param args table A single-element list - size
--- @return number 0 if the queue is initialized, -lerrorCodes.EEXIST if the queue already exists
local function init_queue(keys, args)
    local queue_name = keys[1]
    local size = tonumber(args[1])

    local ret = redis.call('HSETNX', MAPNAME, queue_name, size)
    if ret == 0 then
        return format_response(-lerrorCodes.EEXIST, "Queue already exists", "")
    end
    return format_response(0, "", "")
end

--- Remove a queue
--- @param keys table A two element list - queue set name and queue name
--- @return number 0 if the queue is removed, -lerrorCodes.ENOENT if the queue does not exist
local function remove_queue(keys)
    local queue_name = keys[1]

    local ret = redis.call('HDEL', MAPNAME, queue_name)
    if ret == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    --- Remove all the keys associated with the queue
    redis.call('DEL', "queue:" .. queue_name)
    redis.call('DEL', "reserve:" .. queue_name)
    redis.call('DEL', "lock:" .. queue_name)

    return format_response(0, "", "")
end

--- Check if a queue exists
--- @param keys table A single element list - queue name
--- @return number 1 if the queue exists, 0 if the queue does not exist
local function check_queue_exists(keys)
    local queue_name = keys[1]

    return redis.call('HEXISTS', MAPNAME, queue_name)
end

--- Add an item to the reserve queue for n bytes
--- @param keys table A single element list - queue name
--- @param args table A single-element list - item size
--- @return number 0 if the item is added to the queue, -lerrorCodes.ENOSPC if the queue is full
local function reserve(keys, args)
    local name = "reserve:" .. keys[1]
    local item_size = tonumber(args[1])

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local randomString = generateString(item_size)
    --- generate a json of the format {"timestamp": <current time>, "data": <value>}
    local time = redis.call("TIME")[0]
    local reservation_id = redis.call('INCR', name .. ":id")
    local value = '{"timestamp":' .. redis.call("TIME")[1] .. ',"data":"' .. randomString .. '"}'
    if not redis.call('LPUSH', name, value) then
        return format_response(-lerrorCodes.ENOSPC, "Not enough memory", "")
    end
    local data = {
        reservationID = reservation_id
    }
    local jsondata = cjson.encode(data)
    return format_response(0, "", jsondata)
end

--- Remove an item from the reserve queue
--- @param keys table A single element list - queue name
--- @param args table A single-element list - reservation id
--- @return number 0 if the item is removed from the queue, -lerrorCodes.ENOENT if the queue is empty
local function unreserve(keys, args)
    local name = "reserve:" .. keys[1]
    local reservation_id = tonumber(args[1])

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local value = redis.call('RPOP', name)
    if value == nil then
        return format_response(-lerrorCodes.ENOENT, "Queue is empty", "")
    end
    return format_response(0, "", "")
end

--- Commit message to the queue
--- @param keys table A single element list - queue name
--- @param args table A two element list - message and reservation id
--- @return number 0 if the message is committed to the queue
local function commit(keys, args)
    local name = "queue:" .. keys[1]
    local message = args[1]
    local reservation_id = tonumber(args[2])

    local res = unreserve(keys, {reservation_id})
    if res.map.errorCode ~= 0 then
        return res
    end

    if not redis.call('LPUSH', name, message) then
        return format_response(-lerrorCodes.ENOSPC, "Not enough memory", "")
    end
    return format_response(0, "", "")
end

--- Abort the message reservation
local function abort(keys, args)
    return unreserve(keys, args)
end

--- Read a message from the queue
--- @param keys table A single element list - queue name
--- @return string message if the message is read from the queue, nil if the queue is empty
--- This does not remove the message from the queue
local function read(keys)
    local name = "queue:" .. keys[1]

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local value = redis.call('LRANGE', name, -1, -1)[1]
    return format_response(0, "", value)
end

--- Read single message from the queue
--- @param keys table A single element list - queue name
--- @param args table A single element list - cookie
--- @return pair of number (error code) and string (message)
local function locked_read(keys, args)
    local name = "queue:" .. keys[1]
    local cookie = args[1]

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local assert_lock_keys = {"lock:" .. keys[1]}
    local assert_lock_args = {cookie}

    local lock_status = assert_lock(assert_lock_keys, assert_lock_args)
    if lock_status.map.errorCode == 0 then
        local value = redis.call('LRANGE', name, -1, -1)[1]
        return format_response(0, "", value)
    end
    return lock_status
end

--- Read multiple messages from the queue
--- @param keys table A single element list - queue name
--- @param args table A two element list - cookie, count
--- @return pair of number (error code) and string (message)
local function locked_read_multi(keys, args)
    local name = "queue:" .. keys[1]
    local cookie = args[1]
    local count = tonumber(args[2])

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local assert_lock_keys = {"lock:" .. keys[1]}
    local assert_lock_args = {cookie}

    local lock_status = assert_lock(assert_lock_keys, assert_lock_args)
    if lock_status.map.errorCode == 0 then
        local values = redis.call('LRANGE', name, -count, -1)
        local queueLen = redis.call('LLEN', name)
        local isTruncated = queueLen > count
        local data = {
            values = values,
            isTruncated = isTruncated
        }
        local jsondata = cjson.encode(data)
        return format_response(0, "", jsondata)
    end
    return lock_status
end

local function ack(keys)
    local name = "queue:" .. keys[1]

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    redis.call('RPOP', name)
    return format_response(0, "", "")
end

--- Acknowledge the Read
--- @param keys table A single element list - queue name
--- @param args table A single element list - cookie
--- @return number 0 if the message is acknowledged
local function locked_ack(keys, args)
    local name = "queue:" .. keys[1]
    local cookie = args[1]

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local assert_lock_keys = {"lock:" .. keys[1]}
    local assert_lock_args = {cookie}

    local lock_status = assert_lock(assert_lock_keys, assert_lock_args)
    if lock_status.map.errorCode == 0 then
        redis.call('RPOP', name)
        return format_response(0, "", "")
    end
    return lock_status
end

--- Acknowledge the Read
--- @param keys table A single element list - queue name
--- @param args table A two element list - cookie and count
--- @return number 0 if the message is acknowledged
local function locked_ack_multi(keys, args)
    local name = "queue:" .. keys[1]
    local cookie = args[1]
    local count = args[2]

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local assert_lock_keys = {"lock:" .. keys[1]}
    local assert_lock_args = {cookie}

    local lock_status = assert_lock(assert_lock_keys, assert_lock_args)
    if lock_status.map.errorCode == 0 then
        redis.call('RPOP', name, count)
        return format_response(0, "", "")
    end
    return lock_status
end

--- Stale queue cleanup
--- @param keys table A single element list - queue name
--- @param args table A single element - timeout
--- @return number 0 if the cleanup is successful
local function cleanup(keys, args)
    local name = "reserve:" .. keys[1]
    local timeout = args[1]

    if check_queue_exists({keys[1]}) == 0 then
        return format_response(-lerrorCodes.ENOENT, "Queue does not exist", "")
    end

    local values = redis.call('LRANGE', name, 0, -1)
    local index = -1
    for i, value in ipairs(values) do
        local message = cjson.decode(value)
        if message.timestamp + timeout < tonumber(redis.call("TIME")[1]) then
            index = i - 1
            break
        end
    end
    if index == 0 then
        redis.call('DEL', name)
    else
        redis.call('LTRIM', name, 0, index)
    end
    return format_response(0, "", index)
end

--- Register the functions.
redis.register_function('init_queue', init_queue)
redis.register_function('remove_queue', remove_queue)
redis.register_function('reserve', reserve)
redis.register_function('commit', commit)
redis.register_function('abort', abort)
redis.register_function('read', read)
redis.register_function('locked_read', locked_read)
redis.register_function('locked_read_multi', locked_read_multi)
redis.register_function('ack', ack)
redis.register_function('locked_ack', locked_ack)
redis.register_function('locked_ack_multi', locked_ack_multi)
redis.register_function('cleanup', cleanup)
)";
