#!/usr/bin/env python

class DataPoint:  
  def __init__(self):                
    self.nserver = 0;
    self.nclient = 0;
    self.heap_type = 0;  
    self.total_time_to_add_req = 0;
    self.total_time_to_complete_req = 0;
    self.config = ''

  def set_name(self, config, heap_type):
    self.config = config;
    self.heap_type = heap_type

  def get_conig(self):
    import re
    return re.split(r"/|\.", self.config)[1]

  def __str__(self):
    return "s:%d, c:%d,h:%d,config:%s"%(self.nserver, self.nclient, self.heap_type, self.config);
# end DataPoint


def isFloat(elem):        
 try:
  float(elem)
  return True
 except ValueError:
  return False
#end isFloat


def parse_config_params(fname):
  nclient = 0;
  nserver = 0;
  # read config file property 
  with open(fname, 'r') as f:
    for line in f:
      line = line.strip('\n \t')
      if not line: continue;
      if line.startswith("client_count"):
        nclient += int(line.split('=')[-1]);
      if line.startswith("server_count"): 
        nserver += int(line.split('=')[-1]);
  # end of file
  return [nserver, nclient];
# parse_config_params

def make_aggregate_data_point(dps, config, heap_type): 
    # create new aggregate point
    dp = DataPoint();
    # set set and k_way_heap property
    dp.set_name(config, heap_type); 
    
    num_run = 0
    for _dp in dps:
      if _dp.config == config and _dp.heap_type == heap_type:
        # print _dp, config, heap_type
        dp.nserver =_dp.nserver
        dp.nclient = _dp.nclient
        num_run                       += 1
        dp.total_time_to_add_req      += _dp.total_time_to_add_req
        dp.total_time_to_complete_req += _dp.total_time_to_complete_req 
        
    # average
    dp.total_time_to_add_req      /= num_run;
    dp.total_time_to_complete_req /= num_run
    #print dp
    return dp;

def parse_data_points(filename):
  dps = []; #data-points
  dp = None;
  state = 0;
  configs = {}
  k_ways  = {}
  
  with open(filename, 'r') as f:
    for line in f:
      line = line.strip('\n \t')
      if not line: continue;
      
      # file_name:1:configs/dmc_sim_8_6.conf
      if line.startswith("file_name"):      
        if dp:
          dps.append(dp);
          state = 0;
         
        # new data-point 
        dp = DataPoint();
        parts = line.split(':')
        fname = parts[-1];        
        dp.heap_type = int(parts[1]);
        if dp.heap_type not in k_ways:
          k_ways[dp.heap_type] = 1;
        
        # add to the dictionary
        configs[fname] = 1;
        
        dp.config = fname;
        params = parse_config_params(fname)      
        dp.nserver = params[0];
        dp.nclient = params[-1];
         
      elif line.startswith("average"):	# take last 2 averages
        r = [float(s) for s in line.split(' ') if isFloat(s)]
        state +=1;
        #print r, dp #if isFloat(s)
        if state == 3:
          dp.total_time_to_add_req = r[0]
        elif state == 4:
          dp.total_time_to_complete_req = r[0]
        else: pass

      else: 
        pass;    
  # final entry
  dps.append(dp) 
  
  # compute average of multiple runs
  dps_avg = []
  for config in configs:
    data_per_config = []
    for k in k_ways:
      aggr_dp = make_aggregate_data_point(dps, config , k);
      data_per_config.append(aggr_dp);
    dps_avg.append(data_per_config);
  # end for
  return dps_avg;
# end parse_data_points


def create_header(num_cols):
  fields = ['nserver_nclient(config_file)','add_req', 'complete_req'];
  header = fields[0]
  #write add_req_{1, ...}
  for i in range(num_cols):
    header = '%s %s_%i'%(header, fields[1], i+2)
  #write complete_req_{1, ...}
  for i in range(num_cols):
    header = '%s %s_%i'%(header, fields[2], i+2)
  # new-line
  header = '%s\n'%(header)
  return header
# end create_header


def create_data_line(aggr_dp):
  # get common info
  dp = aggr_dp[0]
  data_line = "s:%d_c:%d "%(dp.nserver, dp.nclient);
  # get the point-count
  num_cols = len(aggr_dp);
  # write add_req_{1, ...}
  for i in range(num_cols):
    data_line = '%s %f'%(data_line, aggr_dp[i].total_time_to_add_req)
  # write complete_req_{1, ...}
  for i in range(num_cols):
    data_line = '%s %f'%(data_line, aggr_dp[i].total_time_to_complete_req)
  # new-line
  data_line = '%s\n'%(data_line)
  return data_line
# end create_data_line

    
def make_data(filename):
  # write the aggregated point in space separated file  
  dps = parse_data_points(filename);
  if not len(dps) : return
  print "total points: ", len(dps)
  # open file
  with open('%s.dat'%(filename), 'w+') as f:
    # write header
    f.write(create_header(len(dps[0])));
    # write data-line
    for aggr_dp in dps:
    	f.write(create_data_line(aggr_dp));


def main(output_file):
  print output_file
  make_data(output_file);

import sys
if __name__ == "__main__":
  file_name="result"
  if len(sys.argv) > 1:
    file_name=sys.argv[1].strip()
  main(file_name)

