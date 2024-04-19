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

package org.example.utils;

import com.google.common.primitives.Longs;
import org.example.Configuration;
import org.example.datasource.DecorateRecord;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class RecordSerdes {


    // if is watermark, do not include payload bytes
    public static byte[] toBytes(DecorateRecord record){
        byte[] seqNumBytes = Longs.toByteArray(record.getSeqNum());
        byte[] pathInfoBytes = Strings.toByteArray(record.getPathInfo());
        int size;
        byte[] result;

        size = 9 + pathInfoBytes.length + (record.isDummyWatermark()?0:Configuration.RECORD_PAYLOAD_BYTE_SIZE);
        result = new byte[size];

        result[0] = record.isDummyWatermark()? (byte) 1: (byte) 0;
        System.arraycopy(seqNumBytes, 0, result, 1, seqNumBytes.length);

        if (!record.isDummyWatermark()){
            System.arraycopy(record.getPayload(), 0, result, 9, record.getPayload().length);
        }

        System.arraycopy(pathInfoBytes, 0, result,
                9+(record.isDummyWatermark()?0:record.getPayload().length), pathInfoBytes.length);

        return result;

    }



    public static DecorateRecord fromBytes(byte[] payload){
        boolean isWatermark = payload[0] == (byte) 1;
        // avoid copying
        long seqNum = Longs.fromBytes(payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],payload[8]);
        byte[] recordPayload;
        String pathInfo;
        if (isWatermark){
            recordPayload = new byte[0];
            pathInfo = Strings.fromByteArray(Arrays.copyOfRange(payload, 9, payload.length));
        }
        else {
            recordPayload = Arrays.copyOfRange(payload, 9, 9+Configuration.RECORD_PAYLOAD_BYTE_SIZE);
            pathInfo = Strings.fromByteArray(
                    Arrays.copyOfRange(payload, 9+Configuration.RECORD_PAYLOAD_BYTE_SIZE, payload.length)
            );

        }
        return new DecorateRecord(isWatermark, seqNum, recordPayload, pathInfo);
    }

    // quick serdes test
    public static void main(String[] args) {
        byte[] payload = new byte[Configuration.RECORD_PAYLOAD_BYTE_SIZE];
        for (int i=0; i<payload.length; i++){
            payload[i] = (byte) i;
        }
        DecorateRecord watermark = new DecorateRecord(
                true, 666L, payload, "test-path-info"
        );

        DecorateRecord record = new DecorateRecord(
                false, 777L, payload, "test-path-info2"
        );

        System.out.println(RecordSerdes.fromBytes(RecordSerdes.toBytes(watermark)));
        System.out.println(RecordSerdes.fromBytes(RecordSerdes.toBytes(record)));
    }
}
