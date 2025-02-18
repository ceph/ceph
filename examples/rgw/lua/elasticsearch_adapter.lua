local elasticsearch = require ("elasticsearch")
local json = require ("lunajson")

local client = elasticsearch.client{
  hosts = {
    {
      host = "localhost",
      port = "9200"
    }
  }
}

local copyfrom = {}
if (Request.CopyFrom ~= nil) then
  copyfrom = {
    Tenant = Request.CopyFrom.Tenant,
    Bucket = Request.CopyFrom.Bucket,
    Object = {
      Name = Request.CopyFrom.Object.Name,
      Instance = Request.CopyFrom.Object.Instance,
      Id = Request.CopyFrom.Object.Id,
      Size = Request.CopyFrom.Object.Size,
      MTime = Request.CopyFrom.Object.MTime
    }
  }
end

local res, status = client:index{
  index = "rgw",
  type = "Request",
  id = Request.Id,
  body = 
  {
    RGWOp = Request.RGWOp,
    DecodedURI = Request.DecodedURI,
    ContentLength = Request.ContentLength,
    GenericAttributes = json.encode(Request.GenericAttributes),
    Response = {
      HTTPStatusCode = Request.Response.HTTPStatusCode,
      HTTPStatus = Request.Response.HTTPStatus,
      RGWCode = Request.Response.RGWCode,
      Message = Request.Response.Message
    },
    SwiftAccountName = Request.SwiftAccountName,
    Bucket = {
      Tenant = Request.Bucket.Tenant,
      Name = Request.Bucket.Name,
      Marker = Request.Bucket.Marker,
      Id = Request.Bucket.Id,
      Count = Request.Bucket.Count,
      Size = Request.Bucket.Size,
      ZoneGroupId = Request.Bucket.ZoneGroupId,
      CreationTime = Request.Bucket.CreationTime,
      MTime = Request.Bucket.MTime,
      Quota = {
        MaxSize = Request.Bucket.Quota.MaxSize,
        MaxObjects = Request.Bucket.Quota.MaxObjects,
        Enabled = Request.Bucket.Quota.Enabled,
        Rounded = Request.Bucket.Quota.Rounded
      },
      PlacementRule = {
        Name = Request.Bucket.PlacementRule.Name,
        StorageClass = Request.Bucket.PlacementRule.StorageClass
      },
      User = {
        Tenant = Request.Bucket.User.Tenant,
        Id = Request.Bucket.User.Id
      }
    },
    Object = {
      Name = Request.Object.Name,
      Instance = Request.Object.Instance,
      Id = Request.Object.Id,
      Size = Request.Object.Size,
      MTime = Request.Object.MTime
    },
    CopyFrom = copyfrom,
    ObjectOwner = {
      DisplayName = Request.ObjectOwner.DisplayName,
      User = {
        Tenant = Request.ObjectOwner.User.Tenant,
        Id = Request.ObjectOwner.User.Id
      }
    },
    ZoneGroup = {
      Name = Request.ZoneGroup.Name,
      Endpoint = Request.ZoneGroup.Endpoint
    },
    Environment = json.encode(Request.Environment),
    Policy = json.encode(Request.Policy),
    UserPolicies = json.encode(Request.UserPolicies),
    RGWId = Request.RGWId,
    HTTP = {
      Parameters = json.encode(Request.HTTP.Parameters),
      Resources = json.encode(Request.HTTP.Resources),
      Metadata = json.encode(Request.HTTP.Metadata),
      Host = Request.HTTP.Host,
      Method = Request.HTTP.Method,
      URI = Request.HTTP.URI,
      QueryString = Request.HTTP.QueryString,
      Domain = Request.HTTP.Domain
    },
    Time = Request.Time,
    Dialect = Request.Dialect,
    Id = Request.Id,
    TransactionId = Request.TransactionId,
    Tags = json.encode(Request.Tags),
    User = {
      Tenant = Request.User.Tenant,
      Id = Request.User.Id
    }
  } 
}

