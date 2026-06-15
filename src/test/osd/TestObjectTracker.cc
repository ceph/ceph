// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "test/osd/ObjectTracker.h"

/**
 * Basic tests for ObjectTracker functionality
 */
class TestObjectTracker : public ::testing::Test {
protected:
  ObjectTracker tracker;
};

TEST_F(TestObjectTracker, BasicCreate) {
  eversion_t v1(1, 1);
  
  tracker.record_create("obj1", "hello", v1);
  
  ASSERT_TRUE(tracker.object_exists("obj1"));
  ASSERT_EQ(tracker.get_expected_size("obj1"), 5);
  ASSERT_EQ(tracker.get_object_version("obj1"), v1);
  ASSERT_EQ(tracker.get_expected_data("obj1", 0, 5), "hello");
}

TEST_F(TestObjectTracker, DataWrite) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "hello", v1);
  tracker.record_data_write("obj1", 0, "HELLO", v2);
  
  ASSERT_TRUE(tracker.object_exists("obj1"));
  ASSERT_EQ(tracker.get_expected_size("obj1"), 5);
  ASSERT_EQ(tracker.get_object_version("obj1"), v2);
  ASSERT_EQ(tracker.get_expected_data("obj1", 0, 5), "HELLO");
}

TEST_F(TestObjectTracker, PartialWrite) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "hello world", v1);
  tracker.record_data_write("obj1", 6, "WORLD", v2);
  
  ASSERT_TRUE(tracker.object_exists("obj1"));
  ASSERT_EQ(tracker.get_expected_size("obj1"), 11);
  ASSERT_EQ(tracker.get_expected_data("obj1", 0, 11), "hello WORLD");
}

TEST_F(TestObjectTracker, ExtendingWrite) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "hello", v1);
  tracker.record_data_write("obj1", 5, " world", v2);
  
  ASSERT_TRUE(tracker.object_exists("obj1"));
  ASSERT_EQ(tracker.get_expected_size("obj1"), 11);
  ASSERT_EQ(tracker.get_expected_data("obj1", 0, 11), "hello world");
}

TEST_F(TestObjectTracker, AttributeWrite) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "data", v1);
  tracker.record_attribute_write("obj1", "attr1", "value1", v2);
  
  auto attr = tracker.get_expected_attribute("obj1", "attr1");
  ASSERT_TRUE(attr.has_value());
  ASSERT_EQ(*attr, "value1");
}

TEST_F(TestObjectTracker, AttributeUpdate) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  eversion_t v3(1, 3);
  
  tracker.record_create("obj1", "data", v1);
  tracker.record_attribute_write("obj1", "attr1", "value1", v2);
  tracker.record_attribute_write("obj1", "attr1", "value2", v3);
  
  auto attr = tracker.get_expected_attribute("obj1", "attr1");
  ASSERT_TRUE(attr.has_value());
  ASSERT_EQ(*attr, "value2");
}

TEST_F(TestObjectTracker, OMapWrite) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "data", v1);
  tracker.record_omap_write("obj1", "key1", "omap_value1", v2);
  
  auto omap = tracker.get_expected_omap("obj1", "key1");
  ASSERT_TRUE(omap.has_value());
  ASSERT_EQ(*omap, "omap_value1");
}

TEST_F(TestObjectTracker, Delete) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "data", v1);
  ASSERT_TRUE(tracker.object_exists("obj1"));
  
  tracker.record_delete("obj1", v2);
  ASSERT_FALSE(tracker.object_exists("obj1"));
  ASSERT_EQ(tracker.get_expected_size("obj1"), 0);
}

TEST_F(TestObjectTracker, MultipleObjects) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  
  tracker.record_create("obj1", "data1", v1);
  tracker.record_create("obj2", "data2", v2);
  
  ASSERT_TRUE(tracker.object_exists("obj1"));
  ASSERT_TRUE(tracker.object_exists("obj2"));
  
  auto objects = tracker.get_tracked_objects();
  ASSERT_EQ(objects.size(), 2);
}

TEST_F(TestObjectTracker, Clear) {
  eversion_t v1(1, 1);
  
  tracker.record_create("obj1", "data", v1);
  ASSERT_TRUE(tracker.object_exists("obj1"));
  
  tracker.clear();
  ASSERT_FALSE(tracker.object_exists("obj1"));
  
  auto objects = tracker.get_tracked_objects();
  ASSERT_EQ(objects.size(), 0);
}

TEST_F(TestObjectTracker, Stats) {
  eversion_t v1(1, 1);
  eversion_t v2(1, 2);
  eversion_t v3(1, 3);
  
  tracker.record_create("obj1", "data", v1);
  tracker.record_attribute_write("obj1", "attr1", "value1", v2);
  tracker.record_omap_write("obj1", "key1", "omap_value1", v3);
  
  std::string stats = tracker.get_stats();
  ASSERT_FALSE(stats.empty());
  // Stats should contain information about tracked objects
  ASSERT_NE(stats.find("Objects tracked: 1"), std::string::npos);
}

// Made with Bob
