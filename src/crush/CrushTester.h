// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_TESTER_H
#define CEPH_CRUSH_TESTER_H

#include "crush/CrushWrapper.h"

#include <fstream>
#include <sstream>

class CrushTester {
  CrushWrapper& crush;
  ostream& err;

  map<int, int> device_weight;
  int min_rule, max_rule;
  int min_x, max_x;
  int min_rep, max_rep;

  int num_batches;
  bool use_crush;

  float mark_down_device_ratio;
  float mark_down_bucket_ratio;

  bool output_utilization;
  bool output_utilization_all;
  bool output_statistics;
  bool output_mappings;
  bool output_bad_mappings;
  bool output_choose_tries;

  bool output_data_file;
  bool output_csv;

  string output_data_file_name;

/*
 * mark a ratio of devices down, can be used to simulate placement distributions
 * under degrated cluster conditions
 */
  void adjust_weights(vector<__u32>& weight);

  /*
   * Get the maximum number of devices that could be selected to satisfy ruleno.
   */
  int get_maximum_affected_by_rule(int ruleno);

  /*
   * for maps where in devices have non-sequential id numbers, return a mapping of device id
   * to a sequential id number. For example, if we have devices with id's 0 1 4 5 6 return a map
   * where:
   *     0 = 0
   *     1 = 1
   *     4 = 2
   *     5 = 3
   *     6 = 4
   *
   * which can help make post-processing easier
   */
  map<int,int> get_collapsed_mapping();

  /*
   * Essentially a re-implementation of CRUSH. Given a vector of devices
   * check that the vector represents a valid placement for a given ruleno.
   */
  bool check_valid_placement(int ruleno, vector<int> in, const vector<__u32>& weight);

  /*
   * Generate a random selection of devices which satisfies ruleno. Essentially a
   * monte-carlo simulator for CRUSH placements which can be used to compare the
   * statistical distribution of the CRUSH algorithm to a random number generator
   */
  int random_placement(int ruleno, vector<int>& out, int maxout, vector<__u32>& weight);

  // scaffolding to store data for off-line processing
   struct tester_data_set {
     vector <string> device_utilization;
     vector <string> device_utilization_all;
     vector <string> placement_information;
     vector <string> batch_device_utilization_all;
     vector <string> batch_device_expected_utilization_all;
     map<int, float> proportional_weights;
     map<int, float> proportional_weights_all;
     map<int, float> absolute_weights;
   } ;

   void write_to_csv(ofstream& csv_file, vector<string>& payload)
   {
     if (csv_file.good())
       for (vector<string>::iterator it = payload.begin(); it != payload.end(); ++it)
         csv_file << (*it);
   }

   void write_to_csv(ofstream& csv_file, map<int, float>& payload)
   {
     if (csv_file.good())
       for (map<int, float>::iterator it = payload.begin(); it != payload.end(); ++it)
         csv_file << (*it).first << ',' << (*it).second << std::endl;
   }

   void write_data_set_to_csv(string user_tag, tester_data_set& tester_data)
   {

     ofstream device_utilization_file ((user_tag + (string)"-device_utilization.csv").c_str());
     ofstream device_utilization_all_file ((user_tag + (string)"-device_utilization_all.csv").c_str());
     ofstream placement_information_file ((user_tag + (string)"-placement_information.csv").c_str());
     ofstream proportional_weights_file ((user_tag + (string)"-proportional_weights.csv").c_str());
     ofstream proportional_weights_all_file ((user_tag + (string)"-proportional_weights_all.csv").c_str());
     ofstream absolute_weights_file ((user_tag + (string)"-absolute_weights.csv").c_str());

     // write the headers
     device_utilization_file << "Device ID, Number of Objects Stored, Number of Objects Expected" << std::endl;
     device_utilization_all_file << "Device ID, Number of Objects Stored, Number of Objects Expected" << std::endl;
     proportional_weights_file << "Device ID, Proportional Weight" << std::endl;
     proportional_weights_all_file << "Device ID, Proportional Weight" << std::endl;
     absolute_weights_file << "Device ID, Absolute Weight" << std::endl;

     placement_information_file << "Input";
     for (int i = 0; i < max_rep; i++) {
       placement_information_file << ", OSD" << i;
     }
     placement_information_file << std::endl;

     write_to_csv(device_utilization_file, tester_data.device_utilization);
     write_to_csv(device_utilization_all_file, tester_data.device_utilization_all);
     write_to_csv(placement_information_file, tester_data.placement_information);
     write_to_csv(proportional_weights_file, tester_data.proportional_weights);
     write_to_csv(proportional_weights_all_file, tester_data.proportional_weights_all);
     write_to_csv(absolute_weights_file, tester_data.absolute_weights);

     device_utilization_file.close();
     device_utilization_all_file.close();
     placement_information_file.close();
     proportional_weights_file.close();
     absolute_weights_file.close();

     if (num_batches > 1) {
       ofstream batch_device_utilization_all_file ((user_tag + (string)"-batch_device_utilization_all.csv").c_str());
       ofstream batch_device_expected_utilization_all_file ((user_tag + (string)"-batch_device_expected_utilization_all.csv").c_str());

       batch_device_utilization_all_file << "Batch Round";
       for (unsigned i = 0; i < tester_data.device_utilization.size(); i++) {
         batch_device_utilization_all_file << ", Objects Stored on OSD" << i;
       }
       batch_device_utilization_all_file << std::endl;

       batch_device_expected_utilization_all_file << "Batch Round";
       for (unsigned i = 0; i < tester_data.device_utilization.size(); i++) {
         batch_device_expected_utilization_all_file << ", Objects Expected on OSD" << i;
       }
       batch_device_expected_utilization_all_file << std::endl;

       write_to_csv(batch_device_utilization_all_file, tester_data.batch_device_utilization_all);
       write_to_csv(batch_device_expected_utilization_all_file, tester_data.batch_device_expected_utilization_all);
       batch_device_expected_utilization_all_file.close();
       batch_device_utilization_all_file.close();
     }
   }

   void write_integer_indexed_vector_data_string(vector<string> &dst, int index, vector<int> vector_data);
   void write_integer_indexed_vector_data_string(vector<string> &dst, int index, vector<float> vector_data);
   void write_integer_indexed_scalar_data_string(vector<string> &dst, int index, int scalar_data);
   void write_integer_indexed_scalar_data_string(vector<string> &dst, int index, float scalar_data);

public:
  CrushTester(CrushWrapper& c, ostream& eo)
    : crush(c), err(eo),
      min_rule(-1), max_rule(-1),
      min_x(-1), max_x(-1),
      min_rep(-1), max_rep(-1),
      num_batches(1),
      use_crush(true),
      mark_down_device_ratio(0.0),
      mark_down_bucket_ratio(1.0),
      output_utilization(false),
      output_utilization_all(false),
      output_statistics(false),
      output_mappings(false),
      output_bad_mappings(false),
      output_choose_tries(false),
      output_data_file(false),
      output_csv(false),
      output_data_file_name("")

  { }

  void set_output_data_file_name(string name) {
    output_data_file_name = name;
  }
  string get_output_data_file_name() const {
    return output_data_file_name;
  }

  void set_output_data_file(bool b) {
     output_data_file = b;
  }
  bool get_output_data_file() const {
    return output_data_file;
  }

  void set_output_csv(bool b) {
     output_csv = b;
  }
  bool get_output_csv() const {
    return output_csv;
  }

  void set_output_utilization(bool b) {
    output_utilization = b;
  }
  bool get_output_utilization() const {
    return output_utilization;
  }

  void set_output_utilization_all(bool b) {
    output_utilization_all = b;
  }
  bool get_output_utilization_all() const {
    return output_utilization_all;
  }

  void set_output_statistics(bool b) {
    output_statistics = b;
  }
  bool get_output_statistics() const {
    return output_statistics;
  }

  void set_output_mappings(bool b) {
    output_mappings = b;
  }
  bool get_output_mappings() const {
    return output_mappings;
  }

  void set_output_bad_mappings(bool b) {
    output_bad_mappings = b;
  }
  bool get_output_bad_mappings() const {
    return output_bad_mappings;
  }

  void set_output_choose_tries(bool b) {
    output_choose_tries = b;
  }
  bool get_output_choose_tries() const {
    return output_choose_tries;
  }

  void set_batches(int b) {
    num_batches = b;
  }
  int get_batches() const {
    return num_batches;
  }

  void set_random_placement() {
    use_crush = false;
  }
  bool get_random_placement() const {
    return use_crush == false;
  }

  void set_bucket_down_ratio(float bucket_ratio) {
    mark_down_bucket_ratio = bucket_ratio;
  }
  float get_bucket_down_ratio() const {
    return mark_down_bucket_ratio;
  }

  void set_device_down_ratio(float device_ratio) {
    mark_down_device_ratio = device_ratio;
  }
  float set_device_down_ratio() const {
    return mark_down_device_ratio;
  }

  void set_device_weight(int dev, float f);

  void set_min_rep(int r) {
    min_rep = r;
  }
  int get_min_rep() const {
    return min_rep;
  }

  void set_max_rep(int r) {
    max_rep = r;
  }
  int get_max_rep() const {
    return max_rep;
  }

  void set_num_rep(int r) {
    min_rep = max_rep = r;
  }
  
  void set_min_x(int x) {
    min_x = x;
  }
  int get_min_x() const {
    return min_x;
  }

  void set_max_x(int x) {
    max_x = x;
  }
  int get_max_x() const {
    return max_x;
  }

  void set_x(int x) {
    min_x = max_x = x;
  }

  void set_min_rule(int rule) {
    min_rule = rule;
  }
  int get_min_rule() const {
    return min_rule;
  }

  void set_max_rule(int rule) {
    max_rule = rule;
  }
  int get_max_rule() const {
    return max_rule;
  }

  void set_rule(int rule) {
    min_rule = max_rule = rule;
  }

  /**
   * check if any bucket/nodes is referencing an unknown name or type
   * @return false if an dangling name/type is referenced, true otherwise
   */
  bool check_name_maps() const;
  int test();
  int test_with_crushtool(const string& crushtool,
			  int timeout);
};

#endif
