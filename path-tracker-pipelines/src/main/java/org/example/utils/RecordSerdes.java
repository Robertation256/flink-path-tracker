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
        byte[] createTime = Longs.toByteArray(record.getCreateTime());
        byte[] sinkTime = Longs.toByteArray(record.getSinkTime());
        byte[] consumeTime = Longs.toByteArray(record.getConsumeTime());
        byte[] heapPushTime = Longs.toByteArray(record.getHeapPushTime());
        byte[] emitTime = Longs.toByteArray(record.getEmitTime());
        byte[] pathInfoBytes = Strings.toByteArray(record.getPathInfo());
        int size;
        byte[] result;

        size = 9 + 5 * Long.BYTES + pathInfoBytes.length + (record.isDummyWatermark()?0:Configuration.RECORD_PAYLOAD_BYTE_SIZE);
        result = new byte[size];

        int offset = 0;
        result[0] = record.isDummyWatermark()? (byte) 1: (byte) 0;
        offset += 1;
        System.arraycopy(seqNumBytes, 0, result, offset, Long.BYTES);
        offset += Long.BYTES;
        System.arraycopy(createTime, 0, result, offset, Long.BYTES);
        offset += Long.BYTES;
        System.arraycopy(sinkTime, 0, result, offset, Long.BYTES);
        offset += Long.BYTES;
        System.arraycopy(consumeTime, 0, result, offset, Long.BYTES);
        offset += Long.BYTES;
        System.arraycopy(heapPushTime, 0, result, offset, Long.BYTES);
        offset += Long.BYTES;
        System.arraycopy(emitTime, 0, result, offset, Long.BYTES);
        offset += Long.BYTES;


        if (!record.isDummyWatermark()){
            System.arraycopy(record.getPayload(), 0, result, offset, record.getPayload().length);
            offset += record.getPayload().length;
        }


        System.arraycopy(pathInfoBytes, 0, result, offset, pathInfoBytes.length);

        return result;
    }



    public static DecorateRecord fromBytes(byte[] payload){
        boolean isWatermark = payload[0] == (byte) 1;
        // avoid copying
        int offset = 1;
        long seqNum = readLong(payload, offset);
        offset += Long.BYTES;
        long createTime = readLong(payload,offset);
        offset += Long.BYTES;
        long sinkTime = readLong(payload,offset);
        offset += Long.BYTES;
        long consumeTime = readLong(payload,offset);
        offset += Long.BYTES;
        long heapPushTime = readLong(payload,offset);
        offset += Long.BYTES;
        long emitTime = readLong(payload,offset);
        offset += Long.BYTES;


        byte[] recordPayload;
        String pathInfo;
        if (isWatermark){
            recordPayload = new byte[0];
            pathInfo = Strings.fromByteArray(Arrays.copyOfRange(payload, offset, payload.length));
        }
        else {
            recordPayload = Arrays.copyOfRange(payload, offset, offset+Configuration.RECORD_PAYLOAD_BYTE_SIZE);
            pathInfo = Strings.fromByteArray(
                    Arrays.copyOfRange(payload, offset+Configuration.RECORD_PAYLOAD_BYTE_SIZE, payload.length)
            );

        }
        DecorateRecord record = new DecorateRecord(isWatermark, seqNum, recordPayload, pathInfo);
        record.setCreateTime(createTime);
        record.setSinkTime(sinkTime);
        record.setConsumeTime(consumeTime);
        record.setHeapPushTime(heapPushTime);
        record.setEmitTime(emitTime);
        return record;
    }

    private static long readLong(byte[] payload, int start){
        return Longs.fromBytes(payload[start], payload[start+1], payload[start+2], payload[start+3], payload[start+4], payload[start+5], payload[start+6],payload[start+7]);
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
        watermark.setCreateTime(11);
        watermark.setSinkTime(22);
        watermark.setConsumeTime(33);
        watermark.setHeapPushTime(44);
        watermark.setEmitTime(55);

        DecorateRecord record = new DecorateRecord(
                false, 777L, payload, "test-path-info2"
        );
        record.setCreateTime(11);
        record.setSinkTime(22);
        record.setConsumeTime(33);
        record.setHeapPushTime(44);
        record.setEmitTime(55);

        System.out.println(RecordSerdes.fromBytes(RecordSerdes.toBytes(watermark)));
        System.out.println(RecordSerdes.fromBytes(RecordSerdes.toBytes(record)));
    }
}
