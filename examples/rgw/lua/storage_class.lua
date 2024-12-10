local function isempty(input)
  return input == nil or input == ''
end

if Request.RGWOp == 'put_obj' then
  RGWDebugLog("Put_Obj with StorageClass: " .. Request.HTTP.StorageClass )
  if (isempty(Request.HTTP.StorageClass)) then
    if (Request.ContentLength >= 65536) then
      RGWDebugLog("No StorageClass for Object and size >= threshold: " .. Request.Object.Name .. " adding QLC StorageClass")
      Request.HTTP.StorageClass = "QLC_CLASS"
    else
      RGWDebugLog("No StorageClass for Object and size < threshold: " .. Request.Object.Name .. " adding STANDARD StorageClass")
      Request.HTTP.StorageClass = "STANDARD"
    end
  else
    RGWDebugLog("Storage Class Header Present on Object: " .. Request.Object.Name .. " with StorageClass: " .. Request.HTTP.StorageClass)
  end
end

