  local json = require ("lunajson")
  local nats = require ("nats")

  function nats_connect(nats_host, nats_port)
      local nats_params = {
          host = nats_host,
          port = nats_port,
      }
      client = nats.connect(nats_params)
      client:connect()
  end

  function toJson(request, eventName, opaqueData, configure)
      supported_event = true
  	local notification = {
                      ["Records"] = {
                          ["eventVersion"] = "2.1",
                          ["eventSource"] = "ceph:s3",
                          ["awsRegion"] = request.ZoneGroup.Name,
                          ["eventTime"] = request.Time,
                          ["eventName"] = eventName,
                          ["userIdentity"] = {
                              ["principalId"] = request.User.Id
                          },
                          ["requestParameters"] = {
                              ["sourceIPAddress"] = ""
                          },
                          ["responseElements"] = {
                              ["x-amz-request-id"] =  request.Id,
                              ["x-amz-id-2"] = request.RGWId
                          },
                          ["s3"] = {
                              ["s3SchemaVersion"] = "1.0",
                              ["configurationId"] = configure,
                              ["bucket"] = {
                                  ["name"] = request.Bucket.Name,
                                  ["ownerIdentity"] = {
                                      ["principalId"] = request.Bucket.User.Id
                                  },
                                  ["arn"] = "arn:aws:s3:" .. request.ZoneGroup.Name .. "::" .. request.Bucket.Name,
                                  ["id"] = request.Bucket.Id
                              },
                              ["object"] = {
                                  ["key"] = request.Object.Name,
                                  ["size"] = request.Object.Size,
                                  ["eTag"] = "", -- eTag is not supported yet
                                  ["versionId"] = request.Object.Instance,
                                  ["sequencer"] = string.format("%x", os.time()), 
                                  ["metadata"] = {
                                      json.encode(request.HTTP.Metadata)
                                  },
                                  ["tags"] = {
                                      json.encode(request.Tags)
                                  }
                              }
                          },
                          ["eventId"] = "",
                          ["opaqueData"] = opaqueData
                      }
      }
      return notification
  end
  
  supported_event = false
  configure = "mynotif1"
  opaqueData = "me@example.com"
  topic = "Bucket_Notification"
  bucket_name = "mybucket"
  nats_host = '0.0.0.0'
  nats_port = 4222
  
  if bucket_name == Request.Bucket.Name then
    --Object Created
    if Request.RGWOp == "put_obj" then
        notification = toJson(Request ,'ObjectCreated:Put', opaqueData, configure)
    elseif Request.RGWOp == "post_obj" then
        notification = toJson(Request ,'ObjectCreated:Post', opaqueData, configure)
    
    elseif Request.RGWOp == "copy_obj" then
        notification = toJson(Request ,'ObjectCreated:Copy', opaqueData, configure)
    
    --Object Removed
    elseif Request.RGWOp == "delete_obj" then
        notification = toJson(Request ,'ObjectRemoved:Delete', opaqueData, configure)
    end
    
    if supported_event == true then
        nats_connect()
        local payload = json.encode(notification)
        client:publish(topic, payload) 
        RGWDebugLog("bucket notification sent to nats://" .. nats_host .. ":" .. nats_port .. "/" .. topic)
    end
  end
