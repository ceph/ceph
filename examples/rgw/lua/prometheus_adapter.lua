local http = require("socket.http")
local ltn12 = require("ltn12")

local respbody = {}
local op = "rgw_other_request_content_length"
if (Request.RGWOp == "put_obj") then
  op = "rgw_put_request_content_length"
elseif (Request.RGWOp == "get_obj") then
  op = "rgw_get_request_content_length"
end
local field = op .. " " .. tostring(Request.ContentLength) .. "\n"

local body, code, headers, status = http.request{
  url = "http://127.0.0.1:9091/metrics/job/rgw", 
  method = "POST",
  headers = {
    ["Content-Type"] = "application/x-www-form-urlencoded",
    ["Content-Length"] = string.len(field)
  },
  source = ltn12.source.string(field), 
  sink = ltn12.sink.table(respbody),
}

