/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.weld;

import static org.apache.spark.weld.WeldStruct.struct;
import static org.apache.spark.weld.VecType.vecOfi64;

public class WeldUtil {

    static WeldModule module;

    public static long runUsingWeld(Long a, Long b) {
        initialize();
        final WeldValue value = struct(a, b).toValue();
        final WeldValue output = module.run(value);
        return output.result(vecOfi64()).getVec(0).getLong(0);
    }

    private static void initialize() {
        if(module == null) {
            String code1 = "|x:i64, y:i64| [x + y]";
            module = WeldModule.compile(code1);
        }
    }
}
