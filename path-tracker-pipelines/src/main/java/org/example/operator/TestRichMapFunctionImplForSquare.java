/*
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

package org.example.operator;

import org.example.Configuration;
import org.example.datasource.DecorateRecord;

public class TestRichMapFunctionImplForSquare extends DecorateRichMapFunction {
    @Override
    public DecorateRecord map(DecorateRecord record) throws Exception {
        record.addAndSetPathInfo(instanceID);
        Thread.sleep(Configuration.OPERATOR_SLEEP_MILLIS);
        // todo: either insert real workload or reduce all operators to dummy workload with induced sleep
        return record;
    }
}
