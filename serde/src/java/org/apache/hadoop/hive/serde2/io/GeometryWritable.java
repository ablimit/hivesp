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
 */

/**
 * This file is back-ported from hadoop-0.19, to make sure hive can run 
 * with hadoop-0.17.
 */
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Writable for Geometry values.
 */
public class GeometryWritable implements WritableComparable {

  private String value = "";

  public GeometryWritable() {

  }

  public GeometryWritable(String value) {
    set(value);
  }

  public void set(String value) {
    this.value = value;
  }

  public String get() {
    return value;
  }

  public void readFields(DataInput in) throws IOException {
    final byte[] temp = new byte[9];
    in.readFully(temp);
    value = String.valueOf(temp);
  }

  public void write(DataOutput out) throws IOException {
    final byte[] temp = value.getBytes();
    out.write(temp);
  }

  public int compareTo(Object o) {
    GeometryWritable other = (GeometryWritable) o;
    return this.value.compareTo(other.value);
  }

  //static { // register this comparator
  //  WritableComparator.define(GeometryWritable.class, new Comparator());
  //}
}
