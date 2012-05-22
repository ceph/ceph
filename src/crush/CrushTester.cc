
#include "CrushTester.h"

void CrushTester::set_device_weight(int dev, float f)
{
  int w = (int)(f * 0x10000);
  if (w < 0)
    w = 0;
  if (w > 0x10000)
    w = 0x10000;
  device_weight[dev] = w;
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

  // all osds in
  vector<__u32> weight;
  for (int o = 0; o < crush.get_max_devices(); o++)
    if (device_weight.count(o))
      weight.push_back(device_weight[o]);
    else
      weight.push_back(0x10000);
  if (verbose>1)
    err << "devices weights (hex): " << hex << weight << dec << std::endl;

  for (int r = min_rule; r < crush.get_max_rules() && r <= max_rule; r++) {
    if (!crush.rule_exists(r)) {
      if (verbose>0)
	err << "rule " << r << " dne" << std::endl;
      continue;
    }
    int minr = min_rep, maxr = max_rep;
    if (min_rep < 0 || max_rep < 0) {
      minr = crush.get_rule_mask_min_size(r);
      maxr = crush.get_rule_mask_max_size(r);
    }

    if (verbose>0)
      err << "rule " << r << " (" << crush.get_rule_name(r)
	  << "), x = " << min_x << ".." << max_x
	  << ", numrep = " << minr << ".." << maxr
	  << std::endl;
    for (int nr = minr; nr <= maxr; nr++) {
      vector<int> per(crush.get_max_devices());
      map<int,int> sizes;
      for (int x = min_x; x <= max_x; x++) {
	vector<int> out;
	crush.do_rule(r, x, out, nr, weight);
	if (verbose)
	  if (verbose>1)
	    err << " rule " << r << " x " << x << " " << out << std::endl;
	for (unsigned i = 0; i < out.size(); i++)
	  per[out[i]]++;
	sizes[out.size()]++;
      }
      for (unsigned i = 0; i < per.size(); i++)
	if (verbose>1)
	  err << "  device " << i << ":\t" << per[i] << std::endl;
      for (map<int,int>::iterator p = sizes.begin(); p != sizes.end(); p++)
	if (verbose>0 || p->first != nr)
	  err << "rule " << r << " (" << crush.get_rule_name(r) << ") num_rep " << nr
	      << " result size == " << p->first << ":\t"
	      << p->second << "/" << (max_x-min_x+1) << std::endl;
    }
  }
  return 0;
}
