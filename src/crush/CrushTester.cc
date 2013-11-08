
#include "CrushTester.h"

#include <algorithm>
#include <stdlib.h>


void CrushTester::set_device_weight(int dev, float f)
{
  int w = (int)(f * 0x10000);
  if (w < 0)
    w = 0;
  if (w > 0x10000)
    w = 0x10000;
  device_weight[dev] = w;
}

int CrushTester::get_maximum_affected_by_rule(int ruleno)
{
  // get the number of steps in RULENO
  int rule_size = crush.get_rule_len(ruleno);
  vector<int> affected_types;
  map<int,int> replications_by_type;

  for (int i = 0; i < rule_size; i++){
    // get what operation is done by the current step
    int rule_operation = crush.get_rule_op(ruleno, i);

    // if the operation specifies choosing a device type, store it
    if (rule_operation >= 2 && rule_operation != 4){
      int desired_replication = crush.get_rule_arg1(ruleno,i);
      int affected_type = crush.get_rule_arg2(ruleno,i);
      affected_types.push_back(affected_type);
      replications_by_type[affected_type] = desired_replication;
    }
  }

  /*
   * now for each of the affected bucket types, see what is the
   * maximum we are (a) requesting or (b) have
   */

  map<int,int> max_devices_of_type;

  // loop through the vector of affected types
  for (vector<int>::iterator it = affected_types.begin(); it != affected_types.end(); ++it){
    // loop through the number of buckets looking for affected types
    for (map<int,string>::iterator p = crush.name_map.begin(); p != crush.name_map.end(); ++p){
      int bucket_type = crush.get_bucket_type(p->first);
      if ( bucket_type == *it)
        max_devices_of_type[*it]++;
    }
  }

  for(std::vector<int>::iterator it = affected_types.begin(); it != affected_types.end(); ++it){
    if ( replications_by_type[*it] > 0 && replications_by_type[*it] < max_devices_of_type[*it] )
      max_devices_of_type[*it] = replications_by_type[*it];
  }

  /*
   * get the smallest number of buckets available of any type as this is our upper bound on
   * the number of replicas we can place
  */
  int max_affected = max( crush.get_max_buckets(), crush.get_max_devices() );

  for(std::vector<int>::iterator it = affected_types.begin(); it != affected_types.end(); ++it){
    if (max_devices_of_type[*it] > 0 && max_devices_of_type[*it] < max_affected )
      max_affected = max_devices_of_type[*it];
  }

  return max_affected;
}


map<int,int> CrushTester::get_collapsed_mapping()
{
  int num_to_check = crush.get_max_devices();
  int next_id = 0;
  map<int, int> collapse_mask;

  for (int i = 0; i < num_to_check; i++){
    if (crush.check_item_present(i)){
      collapse_mask[i] = next_id;
      next_id++;
    }
  }
  
  return collapse_mask;
}

void CrushTester::adjust_weights(vector<__u32>& weight)
{

  if (mark_down_device_ratio > 0) {
    // active buckets
    vector<int> bucket_ids;
    for (int i = 0; i < crush.get_max_buckets(); i++) {
      int id = -1 - i;
      if (crush.get_bucket_weight(id) > 0) {
        bucket_ids.push_back(id);
      }
    }

    // get buckets that are one level above a device
    vector<int> buckets_above_devices;
    for (unsigned i = 0; i < bucket_ids.size(); i++) {
      // grab the first child object of a bucket and check if it's ID is less than 0
      int id = bucket_ids[i];
      if (crush.get_bucket_size(id) == 0)
        continue;
      int first_child = crush.get_bucket_item(id, 0); // returns the ID of the bucket or device
      if (first_child >= 0) {
        buckets_above_devices.push_back(id);
      }
    }

    // permute bucket list
    for (unsigned i = 0; i < buckets_above_devices.size(); i++) {
      unsigned j = lrand48() % (buckets_above_devices.size() - 1);
      std::swap(buckets_above_devices[i], buckets_above_devices[j]);
    }

    // calculate how many buckets and devices we need to reap...
    int num_buckets_to_visit = (int) (mark_down_bucket_ratio * buckets_above_devices.size());

    for (int i = 0; i < num_buckets_to_visit; i++) {
      int id = buckets_above_devices[i];
      int size = crush.get_bucket_size(id);
      vector<int> items;
      for (int o = 0; o < size; o++)
        items.push_back(crush.get_bucket_item(id, o));

      // permute items
      for (int o = 0; o < size; o++) {
        int j = lrand48() % (crush.get_bucket_size(id) - 1);
        std::swap(items[o], items[j]);
      }

      int local_devices_to_visit = (int) (mark_down_device_ratio*size);
      for (int o = 0; o < local_devices_to_visit; o++){
        int item = crush.get_bucket_item(id, o);
        weight[item] = 0;
      }
    }
  }
}

bool CrushTester::check_valid_placement(int ruleno, vector<int> in, const vector<__u32>& weight)
{

  bool valid_placement = true;
  vector<int> included_devices;
  map<string,string> seen_devices;

  // first do the easy check that all devices are "up"
  for (vector<int>::iterator it = in.begin(); it != in.end(); ++it) {
    if (weight[(*it)] == 0) {
      valid_placement = false;
      break;
    } else if (weight[(*it)] > 0) {
      included_devices.push_back( (*it) );
    }
  }

  /*
   * now do the harder test of checking that the CRUSH rule r is not violated
   * we could test that none of the devices mentioned in out are unique,
   * but this is a special case of this test
   */

  // get the number of steps in RULENO
  int rule_size = crush.get_rule_len(ruleno);
  vector<string> affected_types;

  // get the smallest type id, and name
  int min_map_type = crush.get_num_type_names();
  for (map<int,string>::iterator it = crush.type_map.begin(); it != crush.type_map.end(); ++it ) {
    if ( (*it).first < min_map_type ) {
      min_map_type = (*it).first;
    }
  }

  string min_map_type_name = crush.type_map[min_map_type];

  // get the types of devices affected by RULENO
  for (int i = 0; i < rule_size; i++) {
    // get what operation is done by the current step
    int rule_operation = crush.get_rule_op(ruleno, i);

    // if the operation specifies choosing a device type, store it
    if (rule_operation >= 2 && rule_operation != 4) {
      int affected_type = crush.get_rule_arg2(ruleno,i);
      affected_types.push_back( crush.get_type_name(affected_type));
    }
  }

  // find in if we are only dealing with osd's
  bool only_osd_affected = false;
  if (affected_types.size() == 1) {
    if ((affected_types.back() == min_map_type_name) && (min_map_type_name == "osd")) {
      only_osd_affected = true;
    }
  }

  // check that we don't have any duplicate id's
  for (vector<int>::iterator it = included_devices.begin(); it != included_devices.end(); ++it) {
    int num_copies = std::count(included_devices.begin(), included_devices.end(), (*it) );
    if (num_copies > 1) {
      valid_placement = false;
    }
  }

  // if we have more than just osd's affected we need to do a lot more work
  if (!only_osd_affected) {
    // loop through the devices that are "in/up"
    for (vector<int>::iterator it = included_devices.begin(); it != included_devices.end(); ++it) {
      if (valid_placement == false)
        break;

      // create a temporary map of the form (device type, device name in map)
      map<string,string> device_location_hierarchy = crush.get_full_location(*it);

      // loop over the types affected by RULENO looking for duplicate bucket assignments
      for (vector<string>::iterator t = affected_types.begin(); t != affected_types.end(); ++t) {
        if (seen_devices.count( device_location_hierarchy[*t])) {
          valid_placement = false;
          break;
        } else {
          // store the devices we have seen in the form of (device name, device type)
          seen_devices[ device_location_hierarchy[*t] ] = *t;
        }
      }
    }
  }

  return valid_placement;
}

int CrushTester::random_placement(int ruleno, vector<int>& out, int maxout, vector<__u32>& weight)
{
  // get the total weight of the system
  int total_weight = 0;
  for (unsigned i = 0; i < weight.size(); i++)
    total_weight += weight[i];

  if (total_weight == 0 ||
      crush.get_max_devices() == 0)
    return -EINVAL;

  // compute each device's proportional weight
  vector<float> proportional_weights( weight.size() );
  for (unsigned i = 0; i < weight.size(); i++) {
    proportional_weights[i] = (float) weight[i] / (float) total_weight;
  }

  // determine the real maximum number of devices to return
  int devices_requested = min(maxout, get_maximum_affected_by_rule(ruleno));
  bool accept_placement = false;

  vector<int> trial_placement(devices_requested);
  int attempted_tries = 0;
  int max_tries = 100;
  do {
    // create a vector to hold our trial mappings
    int temp_array[devices_requested];
    for (int i = 0; i < devices_requested; i++){
      temp_array[i] = lrand48() % (crush.get_max_devices());
    }

    trial_placement.assign(temp_array, temp_array + devices_requested);
    accept_placement = check_valid_placement(ruleno, trial_placement, weight);
    attempted_tries++;
  } while (accept_placement == false && attempted_tries < max_tries);

  // save our random placement to the out vector
  if (accept_placement)
    out.assign(trial_placement.begin(), trial_placement.end());

  // or don't....
  else if (attempted_tries == max_tries)
    return -EINVAL;

  return 0;
}

void CrushTester::write_integer_indexed_vector_data_string(vector<string> &dst, int index, vector<int> vector_data)
{
  stringstream data_buffer (stringstream::in | stringstream::out);
  unsigned input_size = vector_data.size();

  // pass the indexing variable to the data buffer
  data_buffer << index;

  // pass the rest of the input data to the buffer
  for (unsigned i = 0; i < input_size; i++) {
    data_buffer << ',' << vector_data[i];
  }

  data_buffer << std::endl;

  // write the data buffer to the destination
  dst.push_back( data_buffer.str() );
}

void CrushTester::write_integer_indexed_vector_data_string(vector<string> &dst, int index, vector<float> vector_data)
{
  stringstream data_buffer (stringstream::in | stringstream::out);
  unsigned input_size = vector_data.size();

  // pass the indexing variable to the data buffer
  data_buffer << index;

  // pass the rest of the input data to the buffer
  for (unsigned i = 0; i < input_size; i++) {
    data_buffer << ',' << vector_data[i];
  }

  data_buffer << std::endl;

  // write the data buffer to the destination
  dst.push_back( data_buffer.str() );
}

void CrushTester::write_integer_indexed_scalar_data_string(vector<string> &dst, int index, int scalar_data)
{
  stringstream data_buffer (stringstream::in | stringstream::out);

  // pass the indexing variable to the data buffer
  data_buffer << index;

  // pass the input data to the buffer
  data_buffer << ',' << scalar_data;
  data_buffer << std::endl;

  // write the data buffer to the destination
  dst.push_back( data_buffer.str() );
}
void CrushTester::write_integer_indexed_scalar_data_string(vector<string> &dst, int index, float scalar_data)
{
  stringstream data_buffer (stringstream::in | stringstream::out);

  // pass the indexing variable to the data buffer
  data_buffer << index;

  // pass the input data to the buffer
  data_buffer << ',' << scalar_data;
  data_buffer << std::endl;

  // write the data buffer to the destination
  dst.push_back( data_buffer.str() );
}

int CrushTester::test()
{
  if (min_rule < 0 || max_rule < 0) {
    min_rule = 0;
    max_rule = crush.get_max_rules() - 1;
  }
  if (min_x < 0 || max_x < 0) {
    min_x = 0;
    max_x = 1023;
  }

  // initial osd weights
  vector<__u32> weight;

  /*
   * note device weight is set by crushtool
   * (likely due to a given a command line option)
   */
  for (int o = 0; o < crush.get_max_devices(); o++) {
    if (device_weight.count(o)) {
      weight.push_back(device_weight[o]);
    } else if (crush.check_item_present(o)) {
      weight.push_back(0x10000);
    } else {
      weight.push_back(0);
    }
  }

  if (output_utilization_all)
    err << "devices weights (hex): " << hex << weight << dec << std::endl;

  // make adjustments
  adjust_weights(weight);


  int num_devices_active = 0;
  for (vector<__u32>::iterator p = weight.begin(); p != weight.end(); ++p)
    if (*p > 0)
      num_devices_active++;

  if (output_choose_tries)
    crush.start_choose_profile();
  
  for (int r = min_rule; r < crush.get_max_rules() && r <= max_rule; r++) {
    if (!crush.rule_exists(r)) {
      if (output_statistics)
        err << "rule " << r << " dne" << std::endl;
      continue;
    }
    int minr = min_rep, maxr = max_rep;
    if (min_rep < 0 || max_rep < 0) {
      minr = crush.get_rule_mask_min_size(r);
      maxr = crush.get_rule_mask_max_size(r);
    }
    
    if (output_statistics)
      err << "rule " << r << " (" << crush.get_rule_name(r)
      << "), x = " << min_x << ".." << max_x
      << ", numrep = " << minr << ".." << maxr
      << std::endl;

    for (int nr = minr; nr <= maxr; nr++) {
      vector<int> per(crush.get_max_devices());
      map<int,int> sizes;

      int num_objects = ((max_x - min_x) + 1);
      float num_devices = (float) per.size(); // get the total number of devices, better to cast as a float here 

      // create a structure to hold data for post-processing
      tester_data_set tester_data;
      vector<int> vector_data_buffer;
      vector<float> vector_data_buffer_f;

      // create a map to hold batch-level placement information
      map<int, vector<int> > batch_per;
      int objects_per_batch = num_objects / num_batches;
      int batch_min = min_x;
      int batch_max = min_x + objects_per_batch - 1;

      // get the total weight of the system
      int total_weight = 0;
      for (unsigned i = 0; i < per.size(); i++)
        total_weight += weight[i];

      if (total_weight == 0)
	continue;

      // compute the expected number of objects stored per device in the absence of weighting
      float expected_objects = min(nr, get_maximum_affected_by_rule(r)) * num_objects;

      // compute each device's proportional weight
      vector<float> proportional_weights( per.size() );

      for (unsigned i = 0; i < per.size(); i++)
        proportional_weights[i] = (float) weight[i] / (float) total_weight;

      if (output_data_file) {
        // stage the absolute weight information for post-processing
        for (unsigned i = 0; i < per.size(); i++) {
          tester_data.absolute_weights[i] = (float) weight[i] / (float)0x10000;
        }

        // stage the proportional weight information for post-processing
        for (unsigned i = 0; i < per.size(); i++) {
          if (proportional_weights[i] > 0 )
            tester_data.proportional_weights[i] = proportional_weights[i];

          tester_data.proportional_weights_all[i] = proportional_weights[i];
        }

      }
      // compute the expected number of objects stored per device when a device's weight is considered
      vector<float> num_objects_expected(num_devices);

      for (unsigned i = 0; i < num_devices; i++)
        num_objects_expected[i] = (proportional_weights[i]*expected_objects);

      for (int current_batch = 0; current_batch < num_batches; current_batch++) {
        if (current_batch == (num_batches - 1)) {
          batch_max = max_x;
          objects_per_batch = (batch_max - batch_min + 1);
        }

        float batch_expected_objects = min(nr, get_maximum_affected_by_rule(r)) * objects_per_batch;
        vector<float> batch_num_objects_expected( per.size() );

        for (unsigned i = 0; i < per.size() ; i++)
          batch_num_objects_expected[i] = (proportional_weights[i]*batch_expected_objects);

        // create a vector to hold placement results temporarily 
        vector<int> temporary_per ( per.size() );

        for (int x = batch_min; x <= batch_max; x++) {
          // create a vector to hold the results of a CRUSH placement or RNG simulation
          vector<int> out;

          if (use_crush) {
            if (output_statistics)
              err << "CRUSH"; // prepend CRUSH to placement output
            crush.do_rule(r, x, out, nr, weight);
          } else {
            if (output_statistics)
              err << "RNG"; // prepend RNG to placement output to denote simulation
            // test our new monte carlo placement generator
            random_placement(r, out, nr, weight);
          }

          if (output_statistics)
            err << " rule " << r << " x " << x << " " << out << std::endl;

          if (output_data_file)
            write_integer_indexed_vector_data_string(tester_data.placement_information, x, out);

          for (unsigned i = 0; i < out.size(); i++) {
            per[out[i]]++;
            temporary_per[out[i]]++;
          }

          batch_per[current_batch] = temporary_per;
          sizes[out.size()]++;
          if (output_bad_mappings && out.size() != (unsigned)nr) {
            cout << "bad mapping rule " << r << " x " << x << " num_rep " << nr << " result " << out << std::endl;
          }
        }

        batch_min = batch_max + 1;
        batch_max = batch_min + objects_per_batch - 1;
      }

      for (unsigned i = 0; i < per.size(); i++)
        if (output_utilization && !output_statistics)
          err << "  device " << i
          << ":\t" << per[i] << std::endl;

      for (map<int,int>::iterator p = sizes.begin(); p != sizes.end(); ++p)
        if (output_statistics)
          err << "rule " << r << " (" << crush.get_rule_name(r) << ") num_rep " << nr
          << " result size == " << p->first << ":\t"
          << p->second << "/" << (max_x-min_x+1) << std::endl;

      if (output_statistics)
        for (unsigned i = 0; i < per.size(); i++) {
          if (output_utilization && num_batches > 1){
            if (num_objects_expected[i] > 0 && per[i] > 0) {
              err << "  device " << i << ":\t"
                  << "\t" << " stored " << ": " << per[i]
                  << "\t" << " expected " << ": " << num_objects_expected[i]
                  << std::endl;
            }
          } else if (output_utilization_all && num_batches > 1) {
            err << "  device " << i << ":\t"
                << "\t" << " stored " << ": " << per[i]
                << "\t" << " expected " << ": " << num_objects_expected[i]
                << std::endl;
          }
        }

      if (output_data_file)
        for (unsigned i = 0; i < per.size(); i++) {
          vector_data_buffer_f.clear();
          vector_data_buffer_f.push_back( (float) per[i]);
          vector_data_buffer_f.push_back( (float) num_objects_expected[i]);

          write_integer_indexed_vector_data_string(tester_data.device_utilization_all, i, vector_data_buffer_f);

          if (num_objects_expected[i] > 0 && per[i] > 0)
            write_integer_indexed_vector_data_string(tester_data.device_utilization, i, vector_data_buffer_f);
        }

      if (output_data_file && num_batches > 1) {
        // stage batch utilization information for post-processing
        for (int i = 0; i < num_batches; i++) {
          write_integer_indexed_vector_data_string(tester_data.batch_device_utilization_all, i, batch_per[i]);
          write_integer_indexed_vector_data_string(tester_data.batch_device_expected_utilization_all, i, batch_per[i]);
        }
      }

      string rule_tag = crush.get_rule_name(r);

      if (output_csv)
        write_data_set_to_csv(output_data_file_name+rule_tag,tester_data);
    }
  }

  if (output_choose_tries) {
    __u32 *v = 0;
    int n = crush.get_choose_profile(&v);
    for (int i=0; i<n; i++) {
      cout.setf(std::ios::right);
      cout << std::setw(2)
      << i << ": " << std::setw(9) << v[i];
      cout.unsetf(std::ios::right);
      cout << std::endl;
    }

    crush.stop_choose_profile();
  }

  return 0;
}
