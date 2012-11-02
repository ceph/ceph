/*
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package com.ceph.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import org.junit.*;
import org.junit.rules.ExternalResource;
import org.junit.runners.Suite;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;


@RunWith( Suite.class )
@Suite.SuiteClasses( { 
  CephDoubleMountTest.class,
  CephMountCreateTest.class,
  CephMountTest.class,
  CephUnmountedTest.class,
})

/**
 * Every Java test class must be added to this list in order to be executed with 'ant test'
 */
public class CephAllTests{

  @Rule
  public static ExternalResource testRule = new ExternalResource(){
    @Override
    protected void before() throws Throwable{
      // Add debugging messages or setup code here
    };

    @Override
    protected void after(){
      // Add debugging messages or cleanup code here
    };
  };
}
