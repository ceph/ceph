// -*- mode:Java; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*- 

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Unit tests for the CephFileSystem API implementation.
 */

package org.apache.hadoop.fs.ceph;


import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TestCeph extends FileSystemContractBaseTest {

  @Override
  protected void setUp() throws IOException {
    Configuration conf = new Configuration();
    CephFaker cephfaker = new CephFaker(conf, FileSystem.LOG);
    CephFileSystem cephfs = new CephFileSystem(cephfaker, "ceph://null");

    cephfs.initialize(URI.create("ceph://null"), conf);
    fs = cephfs;
    cephfs.setWorkingDirectory(new Path(getDefaultWorkingDirectory()));
  }
}
