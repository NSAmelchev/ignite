/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;

/**
 * IO test message flow:
 * <p>
 *     <b>Send request</b>
 * </p>
 * <p>{@link #reqCreateTs} Request create timestamp (JVM time).
 * <p>
 *     (queue time)
 * </p>
 * <p>{@link #reqSndTs} Request send timestamp (JVM time).
 * <p>{@link #reqSndTsMillis} Request send timestamp (System time).
 * <p>
 *     (network latency, sending to remote node)
 * <p>
 * <p>{@link #reqRcvTsMillis} Request received timestamp (System time).
 * <p>{@link #reqRcvTs} Request receive timestamp (JVM time).
 * <p>
 *     (remote node system pool queue time)
 * </p>
 * <p>
 *     <b>Send response</b>
 * </p>
 * <p>{@link #reqProcTs} Request process started timestamp (JVM time).
 * <p>
 *     (queue time)
 * </p>
 * <p>{@link #resSndTs} Response send timestamp (JVM time).
 * <p>{@link #resSndTsMillis} Response send timestamp (System time).
 * <p>
 *     (network latency, sending back)
 * <p>
 * <p>{@link #resRcvTsMillis} Response received timestamp (System time).
 * <p>{@link #resRcvTs} Response receive timestamp (JVM time).
 * <p>
 *     (queue time)
 * </p>
 * <p>{@link #resProcTs} Response processed timestamp (JVM time).
 */
public class IgniteIoTestMessage implements MarshallableMessage {
    /** */
    @Order(0)
    long id;

    /** */
    @Order(1)
    boolean procFromNioThread;

    /** */
    @Order(2)
    boolean req;

    /** */
    @Order(3)
    byte[] payload;

    /** */
    @Order(4)
    long reqCreateTs;

    /** */
    @Order(5)
    long reqSndTs;

    /** */
    @Order(6)
    long reqSndTsMillis;

    /** */
    @Order(7)
    long reqRcvTs;

    /** */
    @Order(8)
    long reqRcvTsMillis;

    /** */
    @Order(9)
    long reqProcTs;

    /** */
    @Order(10)
    long resSndTs;

    /** */
    @Order(11)
    long resSndTsMillis;

    /** */
    @Order(12)
    long resRcvTs;

    /** */
    @Order(13)
    long resRcvTsMillis;

    /** */
    @Order(14)
    long resProcTs;

    /** */
    public IgniteIoTestMessage() {
        // No-op.
    }

    /**
     * Request constructor.
     *
     * @param id Test ID.
     * @param payload Payload.
     */
    public IgniteIoTestMessage(long id, byte[] payload, boolean procFromNioThread) {
        this.id = id;
        this.payload = payload;
        this.procFromNioThread = procFromNioThread;

        req = true;
        reqCreateTs = System.nanoTime();
    }

    /** Response constructor. */
    public IgniteIoTestMessage(IgniteIoTestMessage req) {
        id = req.id;

        reqCreateTs = req.reqCreateTs;

        reqSndTs = req.reqSndTs;
        reqSndTsMillis = req.reqSndTsMillis;

        reqRcvTs = req.reqRcvTs;
        reqRcvTsMillis = req.reqRcvTsMillis;
    }

    /**
     * @return {@code True} if message should be processed from NIO thread
     * (otherwise message is submitted to system pool).
     */
    public boolean processFromNioThread() {
        return procFromNioThread;
    }

    /** @return {@code true} if this is request. */
    public boolean request() {
        return req;
    }

    /** @return Test ID. */
    public long testId() {
        return id;
    }

    /** @return Request create timestamp. */
    public long requestCreateTs() {
        return reqCreateTs;
    }

    /** @return Request send timestamp. */
    public long requestSendTs() {
        return reqSndTs;
    }

    /** @return Request receive timestamp. */
    public long requestReceiveTs() {
        return reqRcvTs;
    }

    /** @return Request process started timestamp. */
    public long requestProcessTs() {
        return reqProcTs;
    }

    /** @return Response send timestamp. */
    public long responseSendTs() {
        return resSndTs;
    }

    /** Response send timestamp (millis) */
    public long responseSendTsMillis() {
        return resSndTsMillis;
    }

    /** @return Response receive timestamp. */
    public long responseReceiveTs() {
        return resRcvTs;
    }

    /** @return Request send timestamp (millis). */
    public long requestSendTsMillis() {
        return reqSndTsMillis;
    }

    /** @return Request received timestamp (millis). */
    public long requestReceivedTsMillis() {
        return reqRcvTsMillis;
    }

    /** @return Response received timestamp (millis). */
    public long responseReceivedTsMillis() {
        return resRcvTsMillis;
    }

    /**
     * This method is called to initialize tracing variables.
     * TODO: introduce direct message lifecycle API?
     */
    public void onAfterRead() {
        if (req && reqRcvTs == 0) {
            reqRcvTs = System.nanoTime();

            reqRcvTsMillis = System.currentTimeMillis();
        }

        if (!req && resRcvTs == 0) {
            resRcvTs = System.nanoTime();

            resRcvTsMillis = System.currentTimeMillis();
        }
    }

    /**
     * This method is called to initialize tracing variables.
     * TODO: introduce direct message lifecycle API?
     */
    public void onBeforeWrite() {
        if (req && reqSndTs == 0) {
            reqSndTs = System.nanoTime();

            reqSndTsMillis = System.currentTimeMillis();
        }

        if (!req && resSndTs == 0) {
            resSndTs = System.nanoTime();

            resSndTsMillis = System.currentTimeMillis();
        }
    }

    /** */
    public void onRequestProcessed() {
        reqProcTs = System.nanoTime();
    }

    /** */
    public void onResponseProcessed() {
        resProcTs = System.nanoTime();
    }

    /** @return Response processed timestamp. */
    public long responseProcessedTs() {
        return resProcTs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        onBeforeWrite();
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        onAfterRead();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteIoTestMessage.class, this);
    }
}
