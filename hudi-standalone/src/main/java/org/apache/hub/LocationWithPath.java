/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hub;

import com.google.common.base.Objects;
import org.apache.hudi.common.model.HoodieRecordLocation;

public class LocationWithPath {
  String path;
  HoodieRecordLocation recordLocation;

  public LocationWithPath(String path, HoodieRecordLocation recordLocation) {
    this.path = path;
    this.recordLocation = recordLocation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LocationWithPath otherLoc = (LocationWithPath) o;
    if (recordLocation == null) {
      return Objects.equal(path, otherLoc.path);
    } else {
      return Objects.equal(path, otherLoc.path) &&
          Objects.equal(recordLocation, otherLoc.recordLocation);
    }
  }

  @Override
  public int hashCode() {
    if (recordLocation == null) {
      return Objects.hashCode(path);
    } else {
      return Objects.hashCode(path, recordLocation);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("LocationWithPath {");
    sb.append("path=").append(path).append(", ");
    if (recordLocation == null) {
      sb.append("recordLocation=null");
    } else {
      sb.append("recordLocation=").append(recordLocation);
    }
    sb.append('}');
    return sb.toString();
  }

  public String getPath() {
    return path;
  }

  public HoodieRecordLocation getRecordLocation() {
    return recordLocation;
  }
}
