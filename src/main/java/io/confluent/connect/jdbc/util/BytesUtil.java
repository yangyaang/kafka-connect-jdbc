/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

public class BytesUtil {

  private static final char[] hexCode = "0123456789ABCDEF".toCharArray();

  public static String toHex(byte[] data) {
    StringBuilder r = new StringBuilder(data.length * 2);
    for (byte b : data) {
      r.append(hexCode[(b >> 4) & 0xF]);
      r.append(hexCode[(b & 0xF)]);
    }
    return r.toString();
  }

  public static String toBinary(byte[] data, int scale) {
    StringBuilder r = new StringBuilder(data.length * 8);
    for (byte b : data) {
      r.append((b >> 7) & 0x1);
      r.append((b >> 6) & 0x1);
      r.append((b >> 5) & 0x1);
      r.append((b >> 4) & 0x1);
      r.append((b >> 3) & 0x1);
      r.append((b >> 2) & 0x1);
      r.append((b >> 1) & 0x1);
      r.append(b & 0x1);
    }
    if (scale < r.length()) {
      return r.substring(r.length() - scale);
    } else if (scale > r.length()) {
      for (int i = r.length(); i < scale; i++) {
        r.insert(0, "0");
      }
    } else {
      return r.toString();
    }
    return r.toString();
  }

}
