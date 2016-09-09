/*
 * centile.cc
 *
 *  Created on: 25-Aug-2016
 *      Author: prashant.kr
 */
#include "centile.h"

namespace centile {
  Centile::Centile(unsigned int start, unsigned int end, unsigned int inc) {
    this->start = start;
    this->end = end;
    this->inc = inc;

    numBuckets = (end-start)/inc + 1;
    buckets = new atomic64_t[numBuckets];
    firstBucket = start/inc;
    lastBucket = end/inc;
  }

  Centile::~Centile() {
    delete buckets;
  }

  atomic64_t& Centile::get_sample_count() {
    return sample_count;
  }

  void Centile::insert(unsigned int value) {
    unsigned int index = int(value/inc);
    if(index >= firstBucket && index <= lastBucket) {
      buckets[index - firstBucket].inc();
      sample_count.inc();
    }
  }

  void Centile::reset() {
    sample_count.set(0);
    for(int index = 0; index < numBuckets; index++) {
      buckets[index].set(0);
    }
  }
  unsigned int Centile::get_percentile(double quantile) {
    unsigned int percentile_value=start, sum=0;
    unsigned int position = sample_count.read() * quantile;

    for(unsigned int index = 0 ; index < numBuckets; index++) {
      sum += buckets[index].read();
      percentile_value += inc;
      if(sum >= position) {
        break;
      }
    }
    return percentile_value;
  }

  CentileCollection::CentileCollection(unsigned int start, unsigned int end, unsigned int inc, vector<unsigned int> object_sizes) {
    this->object_sizes = object_sizes;
    for(vector<unsigned int>::iterator object_size_it = object_sizes.begin(); object_size_it != object_sizes.end(); object_size_it++) {
      centile_buckets.push_back(new centile::Centile(start, end, inc));
    }
  }

  void CentileCollection::reset() {
   for(vector<Centile*>::iterator it = centile_buckets.begin(); it != centile_buckets.end(); it++) {
      (*it)->reset();
    } 
  }
  void CentileCollection::insert(unsigned int object_size, unsigned int value) {
    vector<unsigned int>::iterator object_size_it;
    int index = 0;
    for(object_size_it = object_sizes.begin(); object_size_it != object_sizes.end(); object_size_it++) {
      if(*object_size_it >= object_size) {
        break;
      }
      index++;
    }
    if(object_size_it == object_sizes.end()) {
      index--;
    }
    centile_buckets[index]->insert(value);
  }

  unsigned int CentileCollection::get_percentile(CephContext *cct, unsigned int object_size, double quantile) {
    vector<unsigned int>::iterator object_size_it;
    int index = 0;
    for(object_size_it = object_sizes.begin(); object_size_it != object_sizes.end(); object_size_it++) {
      if(*object_size_it >= object_size) {
        break;
      }
      index++;
    }
    if(object_size_it == object_sizes.end()) {
      index--;
    }
    return centile_buckets[index]->get_percentile(quantile);
  }

  CentileCollection::~CentileCollection() {
    for(vector<Centile*>::iterator it = centile_buckets.begin(); it != centile_buckets.end(); it++) {
      delete *it;
    }
  }
}


