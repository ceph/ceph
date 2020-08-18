local check = ngx.req.get_headers()["AUTHORIZATION"]
local uri =  ngx.var.request_uri
local ngx_re = require "ngx.re"
local hdrs = ngx.req.get_headers()
--Take all signedheaders names, this for creating the X-Amz-Cache which is necessary to override range header to be able to readahead an object
local res, err = ngx_re.split(check,"SignedHeaders=")
local res2, err2 = ngx_re.split(res[2],",")
local res3, err3 = ngx_re.split(res2[1],";")
local t = {}
local concathdrs = string.char(0x00)
for i = 1, #res3, 1 do
    if hdrs[res3[i]] ~= nil then
--0xB1 is the separator between header name and value 
        t[i] = res3[i] .. string.char(0xB1) ..  hdrs[res3[i]]
--0xB2 is the separator between headers
        concathdrs = concathdrs .. string.char(0xB2) .. t[i]
    end
end
-- check if the authorization header is not empty
if check ~= nil then
    local xamzcache = concathdrs:sub(2)
    xamzcache = xamzcache .. string.char(0xB2) .. "Authorization" .. string.char(0xB1) .. check
        if xamzcache:find("aws4_request") ~= nil and uri ~= "/" and uri:find("?") == nil then
            ngx.var.authvar = xamzcache
        end
end
