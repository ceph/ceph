#pragma once

#include <string>

const std::string RGW_LUA_SCRIPT = R"(#!lua name=rgwlib

--- Linux Error codes
local lerrorCodes = {
    EPERM = 1,
    ENOENT = 2,
    EBUSY = 16,
    EEXIST = 17,
    ENOMEM = 12
}

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
            return 0 -- success
        else
            return -lerrorCodes.EBUSY
        end
    end
    return -lerrorCodes.ENOENT
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
    if lock_status == 0 then
        redis.call('PEXPIRE', name, timeout)
        return 0
    elseif lock_status == -lerrorCodes.ENOENT then
        redis.call('SET', name, cookie, 'PX', timeout)
        return 0
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
    if lock_status == 0 then
        redis.call('DEL', name)
        return 0
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

--- Add an item to the reserve queue for n bytes
--- @param keys table A single element list - queue name
--- @param args table A single-element list - item size
--- @return number 0 if the item is added to the queue, -lerrorCodes.ENOMEM if the queue is full
local function reserve(keys, args)
    local name = "reserve:" .. keys[1]
    local item_size = tonumber(args[1])

    local randomString = generateString(item_size)
    --- generate a json of the format {"timestamp": <current time>, "data": <value>}
    local value = '{"timestamp":' .. redis.call("TIME")[0] .. ',"data":"' .. randomString .. '"}'
    if not redis.call('LPUSH', name, value) then
        return -lerrorCodes.ENOMEM
    end
    return 0
end

--- Remove an item from the reserve queue
--- @param keys table A single element list - queue name
--- @return number 0 if the item is removed from the queue, -lerrorCodes.ENOENT if the queue is empty
local function unreserve(keys)
    local name = "reserve:" .. keys[1]

    local value = redis.call('RPOP', name)
    if not value then
        return -lerrorCodes.ENOENT
    end
    return 0
end

--- Commit message to the queue
--- @param keys table A single element list - queue name
--- @param args table A single-element list - message
--- @return number 0 if the message is committed to the queue
local function commit(keys, args)
    local name = "queue:" .. keys[1]
    local message = args[1]

    unreserve(keys)
    if not redis.call('LPUSH', name, message) then
        return -lerrorCodes.ENOMEM
    end
    return 0
end

--- Read a message from the queue
--- @param keys table A single element list - queue name
--- @return string message if the message is read from the queue, nil if the queue is empty
--- @return This does not remove the message from the queue
local function read(keys)
    local name = "queue:" .. keys[1]
    return redis.call('LRANGE', name, -1, -1)[1]
end

--- Get the length of the queue and reserve queue
--- @param keys table A single element list - queue name
--- @param args table Empty
--- @return pair of number (queue length) and number (reserve length)
local function queue_status(keys, args)
    local name = "queue:" .. keys[1]
    local reserve_name = "reserve:" .. keys[1]
    local queue_length = redis.call('LLEN', name)
    local reserve_length = redis.call('LLEN', reserve_name)
    return {queue_length, reserve_length}
end

--- Option one
--- Have a separate read if lock is held
--- @param keys table A single element list - queue name
--- @param args table A single element list - cookie
--- @return pair of number (error code) and string (message)
local function locked_read(keys, args)
    local name = "queue:" .. keys[1]
    local cookie = args[1]

    local lock_status = assert_lock(keys, args)
    if lock_status == 0 then
        return 0, redis.call('LRANGE', name, -1, -1)[1]
    end
    return lock_status, ""
end

--- Stale queue cleanup
--- @param keys table A single element list - queue name
--- @param args table A single element - timeout
--- @return number 0 if the cleanup is successful
local function cleanup(keys, args)
    local name = "reserve:" .. keys[1]
    local timeout = args[1]
    return 0
end

--- Register the functions.
redis.register_function('reserve', reserve)
redis.register_function('unreserve', unreserve)
redis.register_function('commit', commit)
redis.register_function('read', read)
redis.register_function('locked_read', locked_read)
redis.register_function('queue_status', queue_status)
redis.register_function('cleanup', cleanup)
)";
