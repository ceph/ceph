-- global varaibles
FUDGE = 0.001
METRICS = {"auth", "all", "req", "q", "cpu", "mem"}
module(..., package.seeall)

-- Print out the metric measurements for each MDS.
-- @arg0:  log file
-- @arg1:  dictionary of MDS loads
function print_metrics(debug, MDSs)
  f = io.open(log, "a")
  io.output(f)
  for i=1, #MDSs do
    io.write(string.format("  [Lua5.2] MDS%d: load=%f <", i - 1, MDSs[i]["load"]))
    for j=1, #METRICS do
      io.write(string.format(" %f", MDSs[i][METRICS[j]]))
    end
    io.write(" >\n")
  end
  io.close(f)
end

-- Parse the arguments passed from C++ Ceph code. For this version of the
-- API, the order of the arguments is
--     debug log name, me, 6-tuples for each MDS
--     (metadata load on authority, metadata on all other subtrees, 
--     request rate, queue length, CPU load, memory load)
-- arg0:   array of arguments to parse
-- return: array mapping MDSs to load maps
function parse_args(arg)
  log = arg[1]
  f = io.open(log, "a")
  io.output(f)

  whoami = arg[2]
  authmetaload = arg[3]
  nfiles = arg[4]
  allmetaload = arg[5]
  metrics = {}
  if (#arg - 5) % #METRICS ~= 0 then
    io.write("  [Lua5.2] Didn't receive all load metrics for all MDSs\n")
    io.close(f)
    return -1
  end
  i = 1
  for k,v in ipairs(arg) do 
    if k > 5 then  
      metrics[i] = v 
      i = i + 1
    end 
  end
  MDSs = {}
  nmds = 0
  for i=0, #metrics-1 do
    if i % #METRICS == 0 then 
      nmds = nmds + 1
      MDSs[nmds] = {}
      MDSs[nmds]["load"] = 0
    end
    MDSs[nmds][METRICS[(i % #METRICS) + 1]] = metrics[i+1]
    i = i + 1
  end
  targets = {}
  for i=1,#MDSs do 
    targets[i] = 0
  end

  io.close(f)
  return log, whoami, MDSs, authmetaload, nfiles, allmetaload, targets
end

-- Save the state for future migration decisions
-- @arg0:  string to save
function wr_state(x)
  stateF = io.open("/tmp/balancer_state", "w")
  stateF:write(x)
  io.close(stateF)
end

-- Read the state saved by a previous migration decision
-- return: string of the saved value
function rd_state()
  stateF = io.open("/tmp/balancer_state", "r")
  state = stateF:read("*all")
  io.close(stateF)
  return state
end

-- Basic min/max functions 
-- @arg0:  first number
-- @arg1:  second number
-- return: the smaller/larger number
function max(x, y) if x > y then return x else return y end end
function min(x, y) if x < y then return x else return y end end

-- Get the total load, which is used to determine if the current MDS 
-- is overloaded and for designating importer and exporter MDSs.
-- @arg0:  dictionary of MDS loads
-- return: the sum of all the MDS loads
function get_total(MDSs)
  total = 0
  for i=1,#MDSs do
    total = total + MDSs[i]["load"]
  end
  return total
end

-- Return an empty array of targets, used when the ``when''
-- condition fails.
-- @arg0:  dictionary of MDS loads
-- return: array of 0s
function get_empty_targets(log, MDSs)
  f = io.open(log, "a")
  io.output(f)  

  io.write("  [Lua5.2] not migrating\n")
  ret = ""
  for i=1,#targets do 
    ret = ret..targets[i].." " 
  end

  io.close(f)
  return ret
end

-- Convert the array of targets into a string; zero out targets
-- if assignment is illegal
-- @arg0:  log file
-- @arg1:  array of loads 
-- return: string of targets
function convert_targets(log, targets)
  ret = ""
  if targets[whoami] ~= 0 then
    for i=1,#targets do 
      targets[i] = 0 
    end
  end
  for i=1,#targets do 
    ret = ret..targets[i].." " 
  end
  return ret
end

-- Sanity check the targets filled in by the custom balancer and return
-- the targetes array "regardless of it erred out" as a string.
-- @arg0:  log file
-- @arg1:  array of how much to send to each MDS
function check_targets(log, targets)
  f = io.open(log, "a")
  io.output(f)  

  ret = ""
  if targets[whoami] ~= 0 then
    io.write("  [Lua5.2] Uh oh... trying to send load to myself, zeroing out targets array.\n")
    for i=1,#targets do targets[i] = 0 end
  else
    for i=1,#targets do ret = ret..targets[i].." " end
  end

  io.close(f)
  return ret
end

-- Designate MDSs as importers (those who have the capacity for load)
-- and exporters (thsoe that want to shed laod). This is modeled off
-- of Sage's original work sharing design.
-- input:  MDSs dictionary
-- return: an importer and exporter array
function get_exporters_importers(log, MDSs)
  f = io.open(log, "a")
  io.output(f)  

  total = get_total(MDSs)
  io.write(string.format("  [Lua5.2] migrating! (total load=%f)\n", total))

  E = {}; I = {}
  for i=1,#MDSs do
    metaload = MDSs[i]["load"]
    if metaload > total / #MDSs then 
      E[i] = metaload
    else 
      I[i] = metaload 
    end
  end

  io.close(f)
  return E, I
end
