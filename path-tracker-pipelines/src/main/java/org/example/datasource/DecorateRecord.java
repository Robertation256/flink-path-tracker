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

package org.example.datasource;

import org.example.Configuration;
import java.util.Arrays;

public class DecorateRecord {
    private final boolean isDummyWatermark;
    private long seqNum;
    private byte[] payload;
    private String pathInfo;

    private int queueId = -1;

    private long createTime;
    private long processCompletionTime;
    private long sinkTime;
    private long consumeTime;
    private long heapPushTime;
    private long emitTime;

    public long getProcessCompletionTime() {
        return processCompletionTime;
    }

    public void setProcessCompletionTime(long processCompletionTime) {
        this.processCompletionTime = processCompletionTime;
    }


    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getSinkTime() {
        return sinkTime;
    }

    public void setSinkTime(long sinkTime) {
        this.sinkTime = sinkTime;
    }

    public long getConsumeTime() {
        return consumeTime;
    }

    public void setConsumeTime(long consumeTime) {
        this.consumeTime = consumeTime;
    }

    public long getHeapPushTime() {
        return heapPushTime;
    }

    public void setHeapPushTime(long heapPushTime) {
        this.heapPushTime = heapPushTime;
    }

    public long getEmitTime() {
        return emitTime;
    }

    public void setEmitTime(long emitTime) {
        this.emitTime = emitTime;
    }




    public DecorateRecord(long seqNum, String pathInfo) {
        this.seqNum = seqNum;
        this.pathInfo = pathInfo;
        this.payload = new byte[Configuration.RECORD_PAYLOAD_BYTE_SIZE];
        this.isDummyWatermark = false;
    }


    public DecorateRecord(boolean isDummyWatermark, long seqNum, byte[] payload, String pathInfo) {
        this.seqNum = seqNum;
        this.pathInfo = pathInfo;
        this.payload = payload;
        this.isDummyWatermark = isDummyWatermark;
    }

    public DecorateRecord(long SeqNum, boolean isDummyWatermark) {
        this.seqNum = SeqNum;
        this.pathInfo = "";
        this.isDummyWatermark = isDummyWatermark;
    }

    public void setSeqNum(long seqNum) {
        this.seqNum = seqNum;
    }

    public long getSeqNum() {
        return seqNum;
    }

    public void setQueueId(int queueId) {this.queueId = queueId;}
    public int getQueueId(){return queueId;}

    // TODO: use xor to compress path information?
    public void addAndSetPathInfo(String vertexID) {
        this.pathInfo = String.format("%s-%s", this.pathInfo, vertexID);
    }

    public String addPathInfo(String vertexID) {
        return String.format("%s-%s", this.pathInfo, vertexID);
    }

    public void setPathInfo(String pathInfo) {
        this.pathInfo = pathInfo;
    }

    public String getPathInfo() {
        return this.pathInfo;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }

    public boolean isDummyWatermark() {
        return this.isDummyWatermark;
    }

    @Override
    public String toString() {
        return String.format(
                "{IsWatermark=%b SeqNumber=%d, PathInfo=(%s), CreateTime=%d, ProcessCompletionTime=%d, SinkTime=%d, ConsumeTime=%d, HeapPushTime=%d, EmitTime=%d, Payload=%s}",
                this.isDummyWatermark,
                this.getSeqNum(),
                this.getPathInfo(),
                this.getCreateTime(),
                this.getProcessCompletionTime(),
                this.getSinkTime(),
                this.getConsumeTime(),
                this.getHeapPushTime(),
                this.getEmitTime(),
                Arrays.toString(this.payload));
    }
}
