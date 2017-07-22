local metrics = {"auth.meta_load", "all.meta_load", "req_rate", "queue_len", "cpu_load_avg"}

-- Metric for balancing is the workload; also dumps metrics
local function mds_load()
  for rank, mds in pairs(mds) do
    local s = "MDS"..rank..": < "
    for _, metric in ipairs(metrics) do
      s = s..metric.."="..mds[metric].." "
    end
    mds.load = mds["all.meta_load"]
    BAL_LOG(5, s.."> load="..mds.load)
  end
end

-- Shed load when you have load and your neighbor doesn't
local function when()
  if not mds[whoami+1] then
    -- i'm the last rank
    BAL_LOG(5, "when: not migrating! I am the last rank, nothing to spill to.");
    return false
  end
  my_load = mds[whoami]["load"]
  his_load = mds[whoami+1]["load"]
  if my_load > 0.01 and his_load < 0.01 then
    BAL_LOG(5, "when: migrating! my_load="..my_load.." hisload="..his_load)
    return true
  end
  BAL_LOG(5, "when: not migrating! my_load="..my_load.." hisload="..his_load)
  return false
end

-- Shed half your load to your neighbor
-- neighbor=whoami+2 because Lua tables are indexed starting at 1
local function where(targets)
  targets[whoami+1] = mds[whoami]["load"]/2
  return targets
end

local targets = {}
for rank in pairs(mds) do
  targets[rank] = 0
end

mds_load()
if when() then
  where(targets)
end

return targets
