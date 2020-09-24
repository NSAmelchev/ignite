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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.Files.walkFileTree;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader.State.DESERIALIZED;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader.State.FORWARD_READ;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader.State.NOT_ENOUGH_DATA;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.JOB;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.QUERY_READS;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TASK;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.TX_COMMIT;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheOperation;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.cacheRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.jobRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryReadsRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.queryRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.taskRecordSize;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.transactionOperation;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.transactionRecordSize;

/**
 * Walker over the performance statistics file.
 *
 * @see FilePerformanceStatisticsWriter
 */
public class FilePerformanceStatisticsReader {
    /** File read buffer size. */
    private static final int READ_BUFFER_SIZE = (int)(8 * U.MB);

    /** Uuid as string pattern. */
    private static final String UUID_STR_PATTERN =
        "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

    /** File name pattern. */
    private static final Pattern FILE_PATTERN = Pattern.compile("^node-(" + UUID_STR_PATTERN + ")(-\\d+)?.prf$");

    /** No-op handler. */
    private static final PerformanceStatisticsHandler[] NOOP_HANDLER = {};

    /** IO factory. */
    private final RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Current file I/O. */
    private FileIO fileIo;

    /** Current record position. */
    private long curRecPos;

    /** Handlers to process deserialized operations. */
    private final PerformanceStatisticsHandler[] handlers;

    /** Cached strings by hashcodes. */
    private final Map<Integer, String> knownStrs = new HashMap<>();

    /** Current handlers. */
    private PerformanceStatisticsHandler[] currHandlers;

    /** Forward read mode. */
    private ForwardRead forwardRead;

    /** @param handlers Handlers to process deserialized operations. */
    public FilePerformanceStatisticsReader(PerformanceStatisticsHandler... handlers) {
        A.notEmpty(handlers, "At least one handler expected.");

        this.handlers = handlers;
        currHandlers = handlers;
    }

    /**
     * Walks over performance statistics files.
     *
     * @param filesOrDirs Files or directories.
     * @throws IOException If read failed.
     */
    public void read(List<File> filesOrDirs) throws IOException {
        List<File> files = resolveFiles(filesOrDirs);

        if (files.isEmpty())
            return;

        ByteBuffer buf = allocateDirect(READ_BUFFER_SIZE).order(nativeOrder());

        for (File file : files) {
            buf.clear();

            UUID nodeId = nodeId(file);

            try (FileIO io = ioFactory.create(file)) {
                fileIo = io;

                while (true) {
                    if (io.read(buf) <= 0) {
                        if (forwardRead != null && !forwardRead.skip) {
                            forwardRead.skip = true;

                            io.position(curRecPos);

                            buf.clear();

                            continue;
                        }

                        break;
                    }

                    buf.flip();

                    while (true) {
                        int pos = buf.position();

                        State state = deserialize(buf, nodeId, currHandlers);

                        if (state == DESERIALIZED) {
                            if (forwardRead == null)
                                curRecPos += buf.position() - pos;

                            if (forwardRead != null && forwardRead.found) {
                                if (forwardRead.resetBuf) {
                                    buf.limit(0);

                                    fileIo.position(curRecPos);
                                }
                                else
                                    buf.position(forwardRead.bufPos);

                                forwardRead = null;

                                currHandlers = handlers;
                            }

                            continue;
                        }
                        else if (state == NOT_ENOUGH_DATA) {
                            buf.position(pos);

                            if (forwardRead != null)
                                forwardRead.resetBuf = true;

                            break;
                        }

                        if (forwardRead.bufPos == -1)
                            forwardRead.bufPos = pos;

                        if (forwardRead.skip) {
                            forwardRead = null;

                            currHandlers = handlers;

                            curRecPos += buf.position() - pos;
                        }
                    }

                    buf.compact();
                }
            }

            knownStrs.clear();
            curRecPos = 0;
            forwardRead = null;
        }
    }

    /**
     * @param buf Buffer.
     * @param nodeId Node id.
     * @param handlers Handlers.
     * @return {@code True} if operation deserialized. {@code False} if not enough bytes.
     */
    private State deserialize(ByteBuffer buf, UUID nodeId, PerformanceStatisticsHandler... handlers) {
        if (buf.remaining() < 1)
            return NOT_ENOUGH_DATA;

        byte opTypeByte = buf.get();

        OperationType opType = OperationType.of(opTypeByte);

        if (cacheOperation(opType)) {
            if (buf.remaining() < cacheRecordSize())
                return NOT_ENOUGH_DATA;

            int cacheId = buf.getInt();
            long startTime = buf.getLong();
            long duration = buf.getLong();

            for (PerformanceStatisticsHandler handler : handlers)
                handler.cacheOperation(nodeId, opType, cacheId, startTime, duration);

            return DESERIALIZED;
        }
        else if (transactionOperation(opType)) {
            if (buf.remaining() < 4)
                return NOT_ENOUGH_DATA;

            int cacheIdsCnt = buf.getInt();

            if (buf.remaining() < transactionRecordSize(cacheIdsCnt) - 4)
                return NOT_ENOUGH_DATA;

            GridIntList cacheIds = new GridIntList(cacheIdsCnt);

            for (int i = 0; i < cacheIdsCnt; i++)
                cacheIds.add(buf.getInt());

            long startTime = buf.getLong();
            long duration = buf.getLong();

            for (PerformanceStatisticsHandler handler : handlers)
                handler.transaction(nodeId, cacheIds, startTime, duration, opType == TX_COMMIT);

            return DESERIALIZED;
        }
        else if (opType == QUERY) {
            if (buf.remaining() < 1)
                return NOT_ENOUGH_DATA;

            boolean cached = buf.get() != 0;

            String text;

            if (cached) {
                if (buf.remaining() < 4)
                    return NOT_ENOUGH_DATA;

                text = readCachedString(buf);

                if (buf.remaining() < queryRecordSize(0, true) - 1 - 4)
                    return NOT_ENOUGH_DATA;
            }
            else {
                if (buf.remaining() < 4)
                    return NOT_ENOUGH_DATA;

                int textLen = buf.getInt();

                if (buf.remaining() < queryRecordSize(textLen, false) - 1 - 4)
                    return NOT_ENOUGH_DATA;

                text = readString(buf, textLen);
            }

            GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());
            long id = buf.getLong();
            long startTime = buf.getLong();
            long duration = buf.getLong();
            boolean success = buf.get() != 0;

            if (text == null)
                return FORWARD_READ;

            for (PerformanceStatisticsHandler handler : handlers)
                handler.query(nodeId, queryType, text, id, startTime, duration, success);

            return DESERIALIZED;
        }
        else if (opType == QUERY_READS) {
            if (buf.remaining() < queryReadsRecordSize())
                return NOT_ENOUGH_DATA;

            GridCacheQueryType queryType = GridCacheQueryType.fromOrdinal(buf.get());
            UUID uuid = readUuid(buf);
            long id = buf.getLong();
            long logicalReads = buf.getLong();
            long physicalReads = buf.getLong();

            for (PerformanceStatisticsHandler handler : handlers)
                handler.queryReads(nodeId, queryType, uuid, id, logicalReads, physicalReads);

            return DESERIALIZED;
        }
        else if (opType == TASK) {
            if (buf.remaining() < 1)
                return NOT_ENOUGH_DATA;

            boolean cached = buf.get() != 0;

            String taskName;

            if (cached) {
                if (buf.remaining() < 4)
                    return NOT_ENOUGH_DATA;

                taskName = readCachedString(buf);

                if (buf.remaining() < taskRecordSize(0, true) - 1 - 4)
                    return NOT_ENOUGH_DATA;
            }
            else {
                if (buf.remaining() < 4)
                    return NOT_ENOUGH_DATA;

                int nameLen = buf.getInt();

                if (buf.remaining() < taskRecordSize(nameLen, false) - 1 - 4)
                    return NOT_ENOUGH_DATA;

                taskName = readString(buf, nameLen);
            }

            IgniteUuid sesId = readIgniteUuid(buf);
            long startTime = buf.getLong();
            long duration = buf.getLong();
            int affPartId = buf.getInt();

            if (taskName == null)
                return FORWARD_READ;

            for (PerformanceStatisticsHandler handler : handlers)
                handler.task(nodeId, sesId, taskName, startTime, duration, affPartId);

            return DESERIALIZED;
        }
        else if (opType == JOB) {
            if (buf.remaining() < jobRecordSize())
                return NOT_ENOUGH_DATA;

            IgniteUuid sesId = readIgniteUuid(buf);
            long queuedTime = buf.getLong();
            long startTime = buf.getLong();
            long duration = buf.getLong();
            boolean timedOut = buf.get() != 0;

            for (PerformanceStatisticsHandler handler : handlers)
                handler.job(nodeId, sesId, queuedTime, startTime, duration, timedOut);

            return DESERIALIZED;
        }
        else
            throw new IgniteException("Unknown operation type id [typeId=" + opTypeByte + ']');
    }

    /** Resolves performance statistics files. */
    static List<File> resolveFiles(List<File> filesOrDirs) throws IOException {
        if (filesOrDirs == null || filesOrDirs.isEmpty())
            return Collections.emptyList();

        List<File> files = new LinkedList<>();

        for (File file : filesOrDirs) {
            if (file.isDirectory()) {
                walkFileTree(file.toPath(), EnumSet.noneOf(FileVisitOption.class), 1,
                    new SimpleFileVisitor<Path>() {
                        @Override public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
                            if (nodeId(path.toFile()) != null)
                                files.add(path.toFile());

                            return FileVisitResult.CONTINUE;
                        }
                    });

                continue;
            }

            if (nodeId(file) != null)
                files.add(file);
        }

        return files;
    }

    /** @return UUID node of file. {@code Null} if this is not a statistics file. */
    @Nullable private static UUID nodeId(File file) {
        Matcher matcher = FILE_PATTERN.matcher(file.getName());

        if (matcher.matches())
            return UUID.fromString(matcher.group(1));

        return null;
    }

    /** Reads cached string from byte buffer. */
    private String readCachedString(ByteBuffer buf) {
        int hash = buf.getInt();

        String str = knownStrs.get(hash);

        if (str == null && forwardRead == null) {
            forwardRead = new ForwardRead(hash);

            currHandlers = NOOP_HANDLER;
        }

        return str;
    }

    /** Reads string from byte buffer. */
    private String readString(ByteBuffer buf, int size) {
        byte[] bytes = new byte[size];

        buf.get(bytes);

        String str = new String(bytes);

        knownStrs.putIfAbsent(str.hashCode(), str);

        if (forwardRead != null && forwardRead.hash == str.hashCode())
            forwardRead.found = true;

        return str;
    }

    /** Reads {@link UUID} from buffer. */
    private static UUID readUuid(ByteBuffer buf) {
        return new UUID(buf.getLong(), buf.getLong());
    }

    /** Reads {@link IgniteUuid} from buffer. */
    private static IgniteUuid readIgniteUuid(ByteBuffer buf) {
        UUID globalId = new UUID(buf.getLong(), buf.getLong());

        return new IgniteUuid(globalId, buf.getLong());
    }

    /** Forward read mode info. */
    private static class ForwardRead {
        /** Hashcode. */
        final int hash;

        /** String found flag. */
        boolean found;

        /** Skip record if string was not found flag. */
        boolean skip;

        /** Buffer position. */
        int bufPos = -1;

        /** {@code True} if the data in the buffer was overwritten during the search. */
        boolean resetBuf;

        /** */
        private ForwardRead(int hash) {
            this.hash = hash;
        }
    }

    /** Record deserialization state. */
    enum State {
        /** Not enough data to deserialize a record. */
        NOT_ENOUGH_DATA,

        /** Record deserialized. */
        DESERIALIZED,

        /** A record contains an unknown string. Forward read mode should be used to find it. */
        FORWARD_READ;
    }
}
