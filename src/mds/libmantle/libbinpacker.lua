module(..., package.seeall)

-- Add to this map everytime you add a binpacker
strategy_map = {}

-- Send off the fragments with the most load, first
function send_until_target(debug, small_to_large, target, dirfrags)
  sum = 0
  send_dfs = {}
  nextmin = {-1, 2147483648}
  if small_to_large then
    start = 1
    finish = #dirfrags
    inc = 1
  else
    start = #dirfrags
    finish = 1
    inc = -1
  end

  for i=start,finish,inc do
    sum = sum + dirfrags[i][2]
    if sum <= target then table.insert(send_dfs, dirfrags[i])
    else 
      sum = sum - dirfrags[i][2] 
      if dirfrags[i][2] < nextmin[2] then nextmin = dirfrags[i] end
    end
  end
  return send_dfs, nextmin
end

local function small_first(debug, target, dirfrags)
  s, n = send_until_target(debug, true, target, dirfrags)
  return s
end
strategy_map["small_first"] = small_first

function small_first_plus1(debug, target, dirfrags)
  s, n = send_until_target(debug, true, target, dirfrags)
  table.insert(s, n)
  return s
end
strategy_map["small_first_plus1"] = small_first_plus1

function big_first(debug, target, dirfrags)
  s, n = send_until_target(debug, true, target, dirfrags)
  return s
end
strategy_map["big_first"] = big_first

function big_first_plus1(debug, target, dirfrags)
  -- want to add the next smallest one, since we are up against the barrier
  -- if we hadn't added it already, then we didn't go all the way to the threshold
  s, n = send_until_target(debug, false, target, dirfrags)
  table.insert(s, n)
  return s
end
strategy_map["big_first_plus1"] = big_first_plus1

function half(debug, target, dirfrags)
  send_dfs = {}
  for i=1,math.ceil(#dirfrags/2) do
    table.insert(send_dfs, dirfrags[i])
  end
  return send_dfs
end
strategy_map["half"] = half

function big_half(debug, target, dirfrags)
  send_dfs = {}
  for i=#dirfrags,(#dirfrags/2)+1,-1 do
    table.insert(send_dfs, dirfrags[i])
  end
  return send_dfs
end
strategy_map["big_half"] = big_half

function small_half(debug, target, dirfrags)
  send_dfs = {}
  for i=1,#dirfrags/2 do
    table.insert(send_dfs, dirfrags[i])
  end
  return send_dfs
end
strategy_map["small_half"] = small_half



-- input:  @target:   STRING how much load we went to offload
--         @dirfrags: STRING the loads for the directory fragments
function pack(strategy_names, arg)
  debug = arg[1]
  target = arg[2]
  f = io.open(debug, "a")
  io.output(f)
  io.write(string.format("  [Lua5.2] using %d binpacking strategies, trying to send load=%f.\n", 
          #strategy_names, target))

  dirfrags = {}
  io.write("  [Lua5.2] print dirfrags: ")
  for i=3,#arg do 
    dirfrags[i-2] = {i-2, arg[i]}
    io.write(string.format(" df[%d]=%f", dirfrags[i-2][1], dirfrags[i-2][2]))
  end
  io.write("\n")

  strategies = {}
  for i=1,#strategy_names do table.insert(strategies, strategy_map[strategy_names[i]]) end

  distance = 2147483648
  send = {}
  --table.sort(dirfrags, function(a,b) return a[2] < b[2] end )
  function dofunction(f) return f(debug, target, dirfrags) end
  for i=1,#strategies do 
    s = dofunction(strategies[i]) 

    sum = 0
    for i=1,#s do sum = sum + s[i][2] end
    d = math.abs(sum-target)
    io.write(string.format("  [Lua5.2]  distance=%f, export_dirfrags=", d))
    for k,v in ipairs(s) do io.write(string.format(" df[%d]", v[1])) end
    io.write(string.format(" (%s)\n", strategy_names[i]))
    if d < distance then
      distance = d
      send = s
    end
  end

  ret = ""
  for k,v in ipairs(send) do ret = ret.." "..v[1] end
  if ret == "" then ret = "-1" end
  io.write(string.format("  [Lua5.2] returning -> %s\n", ret))

  io.close(f)
  return ret
end
