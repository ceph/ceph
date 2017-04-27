metrics = {"auth.meta_load", "all.meta_load", "req_rate", "queue_len", "cpu_load_avg"}

-- Metric for balancing is the workload; also dumps metrics
function mds_load()
  for i=0, #mds do
    s = "MDS"..i..": < "
    for j=1, #metrics do
      s = s..metrics[j].."="..mds[i][metrics[j]].." "
    end
    mds[i]["load"] = mds[i]["all.meta_load"]
    BAL_LOG(0, s.."> load="..mds[i]["load"])
  end
end

-- Shed load when you have load and your neighbor doesn't
function when()
  my_load = mds[whoami]["load"]
  his_load = mds[whoami+1]["load"]
  if my_load > 0.01 and his_load < 0.01 then
    BAL_LOG(2, "when: migrating! my_load="..my_load.." hisload="..his_load)
    return true
  end
  BAL_LOG(2, "when: not migrating! my_load="..my_load.." hisload="..his_load)
  return false
end

-- Shed half your load to your neighbor
-- neighbor=whoami+2 because Lua tables are indexed starting at 1
function where()
  targets = {}
  for i=1, #mds+1 do
    targets[i] = 0
  end

  targets[whoami+2] = mds[whoami]["load"]/2
  return targets
end

mds_load()
if when() then
  return where()
end

targets = {}
for i=1, #mds+1 do
  targets[i] = 0
end
return targets
