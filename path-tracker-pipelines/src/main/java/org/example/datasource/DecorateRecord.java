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
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

import java.util.Arrays;

public class DecorateRecord {
    private final boolean isDummyWatermark;
    private Long seqNum;
    private byte[] payload;
    private String pathInfo;

    private int queueId = -1;

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
                "{IsWatermark=%b SeqNumber=%d, PathInfo=(%s), Payload=%s}",
                this.isDummyWatermark,
                this.getSeqNum(),
                this.getPathInfo(),
                Arrays.toString(this.payload));
    }
}
