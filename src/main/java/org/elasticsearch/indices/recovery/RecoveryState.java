/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Keeps track of state related to shard recovery.
 */
public class RecoveryState implements ToXContent, Streamable {

    public static enum Stage {
        INIT((byte) 0),

        /**
         * recovery of lucene files, either reusing local ones are copying new ones
         */
        INDEX((byte) 1),

        /** starting up the engine, potentially running checks */
        START((byte) 2),

        /** replaying the translog */
        TRANSLOG((byte) 3),

        /**
         * performing final task after all translog ops have been done
         */
        FINALIZE((byte) 4),

        DONE((byte) 5);

        private static final Stage[] STAGES = new Stage[Stage.values().length];

        static {
            for (Stage stage : Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Stage fromId(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= STAGES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    public static enum Type {
        GATEWAY((byte) 0), SNAPSHOT((byte) 1), REPLICA((byte) 2), RELOCATION((byte) 3);

        private static final Type[] TYPES = new Type[Type.values().length];

        static {
            for (Type type : Type.values()) {
                assert type.id() < TYPES.length && type.id() >= 0;
                TYPES[type.id] = type;
            }
        }

        private final byte id;

        Type(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Type fromId(byte id) throws ElasticsearchIllegalArgumentException {
            if (id < 0 || id >= TYPES.length) {
                throw new ElasticsearchIllegalArgumentException("No mapping for id [" + id + "]");
            }
            return TYPES[id];
        }
    }

    private Stage stage;

    private final Index index = new Index();
    private final Translog translog = new Translog();
    private final Start start = new Start();
    private final Timer timer = new Timer();

    private Type type;
    private ShardId shardId;
    private RestoreSource restoreSource;
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;
    private boolean primary = false;

    private RecoveryState() {
    }

    public RecoveryState(ShardId shardId, boolean primary, Type type, DiscoveryNode sourceNode, DiscoveryNode targetNode) {
        this(shardId, primary, type, sourceNode, null, targetNode);
    }

    public RecoveryState(ShardId shardId, boolean primary, Type type, RestoreSource restoreSource, DiscoveryNode targetNode) {
        this(shardId, primary, type, null, restoreSource, targetNode);
    }

    private RecoveryState(ShardId shardId, boolean primary, Type type, @Nullable DiscoveryNode sourceNode, @Nullable RestoreSource restoreSource, DiscoveryNode targetNode) {
        this.shardId = shardId;
        this.primary = primary;
        this.type = type;
        this.sourceNode = sourceNode;
        this.restoreSource = restoreSource;
        this.targetNode = targetNode;
        stage = Stage.INIT;
        timer.start();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public synchronized Stage getStage() {
        return this.stage;
    }

    private void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            throw new ElasticsearchIllegalStateException("can't move recovery to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized RecoveryState setStage(Stage stage) {
        switch (stage) {
            case INIT:
                // reinitializing stop remove all state except for start time
                this.stage = Stage.INIT;
                getIndex().reset();
                getStart().reset();
                getTranslog().reset();
                break;
            case INDEX:
                validateAndSetStage(Stage.INIT, stage);
                getIndex().start();
                break;
            case START:
                validateAndSetStage(Stage.INDEX, stage);
                getIndex().stop();
                getStart().start();
                break;
            case TRANSLOG:
                validateAndSetStage(Stage.START, stage);
                getStart().stop();
                getTranslog().start();
                break;
            case FINALIZE:
                validateAndSetStage(Stage.TRANSLOG, stage);
                getTranslog().stop();
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE, stage);
                getTimer().stop();
                break;
            default:
                throw new ElasticsearchIllegalArgumentException("unknown RecoveryState.Stage [" + stage + "]");
        }
        return this;
    }

    public Index getIndex() {
        return index;
    }

    public Start getStart() {
        return this.start;
    }

    public Translog getTranslog() {
        return translog;
    }

    public Timer getTimer() {
        return timer;
    }

    public Type getType() {
        return type;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public RestoreSource getRestoreSource() {
        return restoreSource;
    }

    public boolean getPrimary() {
        return primary;
    }

    public static RecoveryState readRecoveryState(StreamInput in) throws IOException {
        RecoveryState recoveryState = new RecoveryState();
        recoveryState.readFrom(in);
        return recoveryState;
    }

    @Override
    public synchronized void readFrom(StreamInput in) throws IOException {
        timer.readFrom(in);
        type = Type.fromId(in.readByte());
        stage = Stage.fromId(in.readByte());
        shardId = ShardId.readShardId(in);
        restoreSource = RestoreSource.readOptionalRestoreSource(in);
        targetNode = DiscoveryNode.readNode(in);
        if (in.readBoolean()) {
            sourceNode = DiscoveryNode.readNode(in);
        }
        index.readFrom(in);
        translog.readFrom(in);
        start.readFrom(in);
        if (in.getVersion().before(Version.V_1_5_0)) {
            // used to the detailed flag
            in.readBoolean();
        }
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timer.writeTo(out);
        out.writeByte(type.id());
        out.writeByte(stage.id());
        shardId.writeTo(out);
        out.writeOptionalStreamable(restoreSource);
        targetNode.writeTo(out);
        out.writeBoolean(sourceNode != null);
        if (sourceNode != null) {
            sourceNode.writeTo(out);
        }
        index.writeTo(out);
        translog.writeTo(out);
        start.writeTo(out);
        if (out.getVersion().before(Version.V_1_5_0)) {
            // detailed flag
            out.writeBoolean(true);
        }
        out.writeBoolean(primary);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(Fields.ID, shardId.id());
        builder.field(Fields.TYPE, type.toString());
        builder.field(Fields.STAGE, stage.toString());
        builder.field(Fields.PRIMARY, primary);
        builder.dateValueField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, timer.startTime);
        if (timer.stopTime > 0) {
            builder.dateValueField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, timer.stopTime);
        }
        builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, timer.time());

        if (restoreSource != null) {
            builder.field(Fields.SOURCE);
            restoreSource.toXContent(builder, params);
        } else {
            builder.startObject(Fields.SOURCE);
            builder.field(Fields.ID, sourceNode.id());
            builder.field(Fields.HOST, sourceNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, sourceNode.address().toString());
            builder.field(Fields.IP, sourceNode.getHostAddress());
            builder.field(Fields.NAME, sourceNode.name());
            builder.endObject();
        }

        builder.startObject(Fields.TARGET);
        builder.field(Fields.ID, targetNode.id());
        builder.field(Fields.HOST, targetNode.getHostName());
        builder.field(Fields.TRANSPORT_ADDRESS, targetNode.address().toString());
        builder.field(Fields.IP, targetNode.getHostAddress());
        builder.field(Fields.NAME, targetNode.name());
        builder.endObject();

        builder.startObject(Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.TRANSLOG);
        translog.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.START);
        start.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final XContentBuilderString ID = new XContentBuilderString("id");
        static final XContentBuilderString TYPE = new XContentBuilderString("type");
        static final XContentBuilderString STAGE = new XContentBuilderString("stage");
        static final XContentBuilderString PRIMARY = new XContentBuilderString("primary");
        static final XContentBuilderString START_TIME = new XContentBuilderString("start_time");
        static final XContentBuilderString START_TIME_IN_MILLIS = new XContentBuilderString("start_time_in_millis");
        static final XContentBuilderString STOP_TIME = new XContentBuilderString("stop_time");
        static final XContentBuilderString STOP_TIME_IN_MILLIS = new XContentBuilderString("stop_time_in_millis");
        static final XContentBuilderString TOTAL_TIME = new XContentBuilderString("total_time");
        static final XContentBuilderString TOTAL_TIME_IN_MILLIS = new XContentBuilderString("total_time_in_millis");
        static final XContentBuilderString SOURCE = new XContentBuilderString("source");
        static final XContentBuilderString HOST = new XContentBuilderString("host");
        static final XContentBuilderString TRANSPORT_ADDRESS = new XContentBuilderString("transport_address");
        static final XContentBuilderString IP = new XContentBuilderString("ip");
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString TARGET = new XContentBuilderString("target");
        static final XContentBuilderString INDEX = new XContentBuilderString("index");
        static final XContentBuilderString TRANSLOG = new XContentBuilderString("translog");
        static final XContentBuilderString TOTAL_ON_START = new XContentBuilderString("total_on_start");
        static final XContentBuilderString START = new XContentBuilderString("start");
        static final XContentBuilderString RECOVERED = new XContentBuilderString("recovered");
        static final XContentBuilderString RECOVERED_IN_BYTES = new XContentBuilderString("recovered_in_bytes");
        static final XContentBuilderString CHECK_INDEX_TIME = new XContentBuilderString("check_index_time");
        static final XContentBuilderString CHECK_INDEX_TIME_IN_MILLIS = new XContentBuilderString("check_index_time_in_millis");
        static final XContentBuilderString LENGTH = new XContentBuilderString("length");
        static final XContentBuilderString LENGTH_IN_BYTES = new XContentBuilderString("length_in_bytes");
        static final XContentBuilderString FILES = new XContentBuilderString("files");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_IN_BYTES = new XContentBuilderString("total_in_bytes");
        static final XContentBuilderString REUSED = new XContentBuilderString("reused");
        static final XContentBuilderString REUSED_IN_BYTES = new XContentBuilderString("reused_in_bytes");
        static final XContentBuilderString PERCENT = new XContentBuilderString("percent");
        static final XContentBuilderString DETAILS = new XContentBuilderString("details");
        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SOURCE_THROTTLE_TIME = new XContentBuilderString("source_throttle_time");
        static final XContentBuilderString SOURCE_THROTTLE_TIME_IN_MILLIS = new XContentBuilderString("source_throttle_time_in_millis");
        static final XContentBuilderString TARGET_THROTTLE_TIME = new XContentBuilderString("target_throttle_time");
        static final XContentBuilderString TARGET_THROTTLE_TIME_IN_MILLIS = new XContentBuilderString("target_throttle_time_in_millis");
    }

    public static class Timer implements Streamable {
        protected long startTime = 0;
        protected long startNanoTime = 0;
        protected long time = -1;
        protected long stopTime = 0;

        public synchronized void start() {
            assert startTime == 0 : "already started";
            startTime = System.currentTimeMillis();
            startNanoTime = System.nanoTime();
        }

        /** Returns start time in millis */
        public synchronized long startTime() {
            return startTime;
        }

        /** Returns elapsed time in millis, or 0 if timer was not started */
        public synchronized long time() {
            if (startTime == 0) {
                return 0;
            }
            if (time >= 0) {
                return time;
            }
            return Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startNanoTime));
        }

        /** Returns stop time in millis */
        public synchronized long stopTime() {
            return stopTime;
        }

        public synchronized void stop() {
            assert stopTime == 0 : "already stopped";
            stopTime = Math.max(System.currentTimeMillis(), startTime);
            time = TimeValue.nsecToMSec(System.nanoTime() - startNanoTime);
            assert time >= 0;
        }

        public synchronized void reset() {
            startTime = 0;
            time = -1;
            stopTime = 0;
        }

        @Override
        public synchronized void readFrom(StreamInput in) throws IOException {
            startTime = in.readVLong();
            stopTime = in.readVLong();
            time = in.readVLong();
            if (in.getVersion().onOrAfter(Version.V_1_6_1)) {
                startNanoTime = in.readVLong();
            }
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(startTime);
            out.writeVLong(stopTime);
            // write a snapshot of current time, which is not per se the time field
            out.writeVLong(time());
            if (out.getVersion().onOrAfter(Version.V_1_6_1)) {
                // This field is used when time field isn't set. Since time field
                // will be set to the value of time() above, this value isn't really used.
                // Therefore it's safe to not serialize/deserialize it on versions < 1.6.1.
                out.writeVLong(startNanoTime);
            }
        }
    }

    public static class Start extends Timer implements ToXContent, Streamable {
        private volatile long checkIndexTime;

        public void reset() {
            super.reset();
            checkIndexTime = 0;
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_1_5_0)) {
                super.readFrom(in);
            } else {
                startTime = in.readVLong();
                long time = in.readVLong();
                stopTime = startTime + time;
            }
            checkIndexTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_1_5_0)) {
                super.writeTo(out);
            } else {
                out.writeVLong(startTime);
                out.writeVLong(time());
            }
            out.writeVLong(checkIndexTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.timeValueField(Fields.CHECK_INDEX_TIME_IN_MILLIS, Fields.CHECK_INDEX_TIME, checkIndexTime);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time());
            return builder;
        }
    }

    public static class Translog extends Timer implements ToXContent, Streamable {
        public static final int UNKNOWN = -1;

        private int recovered;
        private int total = UNKNOWN;
        private int totalOnStart = UNKNOWN;

        public synchronized void reset() {
            super.reset();
            recovered = 0;
            total = UNKNOWN;
            totalOnStart = UNKNOWN;
        }

        public synchronized void incrementRecoveredOperations() {
            recovered++;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        /**
         * returns the total number of translog operations recovered so far
         */
        public synchronized int recoveredOperations() {
            return recovered;
        }

        /**
         * returns the total number of translog operations needed to be recovered at this moment.
         * Note that this can change as the number of operations grows during recovery.
         * <p/>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperations() {
            return total;
        }

        public synchronized void totalOperations(int total) {
            this.total = total;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total [" + total + "], recovered [" + recovered + "]";
        }

        /**
         * returns the total number of translog operations to recovered, on the start of the recovery. Unlike {@link #totalOperations}
         * this does change during recovery.
         * <p/>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperationsOnStart() {
            return this.totalOnStart;
        }

        public synchronized void totalOperationsOnStart(int total) {
            this.totalOnStart = total;
        }

        public synchronized float recoveredPercent() {
            if (total == UNKNOWN) {
                return -1.f;
            }
            if (total == 0) {
                return 100.f;
            }
            return recovered * 100.0f / total;
        }

        @Override
        public synchronized void readFrom(StreamInput in) throws IOException {
            if (in.getVersion().onOrAfter(Version.V_1_5_0)) {
                super.readFrom(in);
                recovered = in.readVInt();
                total = in.readVInt();
                totalOnStart = in.readVInt();
            } else {
                startTime = in.readVLong();
                long time = in.readVLong();
                stopTime = startTime + time;
                recovered = in.readVInt();
                total = UNKNOWN;
                totalOnStart = UNKNOWN;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().onOrAfter(Version.V_1_5_0)) {
                super.writeTo(out);
                out.writeVInt(recovered);
                out.writeVInt(total);
                out.writeVInt(totalOnStart);
            } else {
                out.writeVLong(startTime);
                out.writeVLong(time());
                out.writeVInt(recovered);
            }
        }

        @Override
        public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.RECOVERED, recovered);
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredPercent()));
            builder.field(Fields.TOTAL_ON_START, totalOnStart);
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time());
            return builder;
        }
    }

    public static class File implements ToXContent, Streamable {
        private String name;
        private long length;
        private long recovered;
        private boolean reused;

        public File() {
        }

        public File(String name, long length, boolean reused) {
            assert name != null;
            this.name = name;
            this.length = length;
            this.reused = reused;
        }

        void addRecoveredBytes(long bytes) {
            assert reused == false : "file is marked as reused, can't update recovered bytes";
            assert bytes >= 0 : "can't recovered negative bytes. got [" + bytes + "]";
            recovered += bytes;
        }

        /**
         * file name *
         */
        public String name() {
            return name;
        }

        /**
         * file length *
         */
        public long length() {
            return length;
        }

        /**
         * number of bytes recovered for this file (so far). 0 if the file is reused *
         */
        public long recovered() {
            return recovered;
        }

        /**
         * returns true if the file is reused from a local copy
         */
        public boolean reused() {
            return reused;
        }

        boolean fullyRecovered() {
            return reused == false && length == recovered;
        }

        public static File readFile(StreamInput in) throws IOException {
            File file = new File();
            file.readFrom(in);
            return file;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            length = in.readVLong();
            recovered = in.readVLong();
            if (in.getVersion().onOrAfter(Version.V_1_5_0)) {
                reused = in.readBoolean();
            } else {
                reused = recovered <= 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeVLong(recovered);
            if (out.getVersion().onOrAfter(Version.V_1_5_0)) {
                out.writeBoolean(reused);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Fields.NAME, name);
            builder.byteSizeField(Fields.LENGTH_IN_BYTES, Fields.LENGTH, length);
            builder.field(Fields.REUSED, reused);
            builder.byteSizeField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, recovered);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof File) {
                File other = (File) obj;
                return name.equals(other.name) && length == other.length() && reused == other.reused() && recovered == other.recovered();
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + (int) (length ^ (length >>> 32));
            result = 31 * result + (int) (recovered ^ (recovered >>> 32));
            result = 31 * result + (reused ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "file (name [" + name + "], reused [" + reused + "], length [" + length + "], recovered [" + recovered + "])";
        }
    }

    public static class Index extends Timer implements ToXContent, Streamable {

        private Map<String, File> fileDetails = new HashMap<>();

        public final static long UNKNOWN = -1L;

        private long version = UNKNOWN;
        private long sourceThrottlingInNanos = UNKNOWN;
        private long targetThrottleTimeInNanos = UNKNOWN;

        public synchronized List<File> fileDetails() {
            return ImmutableList.copyOf(fileDetails.values());
        }

        public synchronized void reset() {
            super.reset();
            version = UNKNOWN;
            fileDetails.clear();
            sourceThrottlingInNanos = UNKNOWN;
            targetThrottleTimeInNanos = UNKNOWN;
        }

        public synchronized void addFileDetail(String name, long length, boolean reused) {
            File file = new File(name, length, reused);
            File existing = fileDetails.put(name, file);
            assert existing == null : "file [" + name + "] is already reported";
        }

        public synchronized void addRecoveredBytesToFile(String name, long bytes) {
            File file = fileDetails.get(name);
            file.addRecoveredBytes(bytes);
        }

        public synchronized long version() {
            return this.version;
        }

        public synchronized void addSourceThrottling(long timeInNanos) {
            if (sourceThrottlingInNanos == UNKNOWN) {
                sourceThrottlingInNanos = timeInNanos;
            } else {
                sourceThrottlingInNanos += timeInNanos;
            }
        }

        public synchronized void addTargetThrottling(long timeInNanos) {
            if (targetThrottleTimeInNanos == UNKNOWN) {
                targetThrottleTimeInNanos = timeInNanos;
            } else {
                targetThrottleTimeInNanos += timeInNanos;
            }
        }

        public synchronized TimeValue sourceThrottling() {
            return TimeValue.timeValueNanos(sourceThrottlingInNanos);
        }

        public synchronized TimeValue targetThrottling() {
            return TimeValue.timeValueNanos(targetThrottleTimeInNanos);
        }

        /**
         * total number of files that are part of this recovery, both re-used and recovered
         */
        public synchronized int totalFileCount() {
            return fileDetails.size();
        }

        /**
         * total number of files to be recovered (potentially not yet done)
         */
        public synchronized int totalRecoverFiles() {
            int total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                }
            }
            return total;
        }

        /**
         * number of file that were recovered (excluding on ongoing files)
         */
        public synchronized int recoveredFileCount() {
            int count = 0;
            for (File file : fileDetails.values()) {
                if (file.fullyRecovered()) {
                    count++;
                }
            }
            return count;
        }

        /**
         * percent of recovered (i.e., not reused) files out of the total files to be recovered
         */
        public synchronized float recoveredFilesPercent() {
            int total = 0;
            int recovered = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total++;
                    if (file.fullyRecovered()) {
                        recovered++;
                    }
                }
            }
            if (total == 0 && fileDetails.size() == 0) { // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                float result = 100.0f * (recovered / (float) total);
                return result;
            }
        }

        /**
         * total number of bytes in th shard
         */
        public synchronized long totalBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                total += file.length();
            }
            return total;
        }

        /**
         * total number of bytes recovered so far, including both existing and reused
         */
        public synchronized long recoveredBytes() {
            long recovered = 0;
            for (File file : fileDetails.values()) {
                recovered += file.recovered();
            }
            return recovered;
        }

        /**
         * total bytes of files to be recovered (potentially not yet done)
         */
        public synchronized long totalRecoverBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                }
            }
            return total;
        }

        public synchronized long totalReuseBytes() {
            long total = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    total += file.length();
                }
            }
            return total;
        }

        /**
         * percent of bytes recovered out of total files bytes *to be* recovered
         */
        public synchronized float recoveredBytesPercent() {
            long total = 0;
            long recovered = 0;
            for (File file : fileDetails.values()) {
                if (file.reused() == false) {
                    total += file.length();
                    recovered += file.recovered();
                }
            }
            if (total == 0 && fileDetails.size() == 0) {
                // indicates we are still in init phase
                return 0.0f;
            }
            if (total == recovered) {
                return 100.0f;
            } else {
                return 100.0f * recovered / total;
            }
        }

        public synchronized int reusedFileCount() {
            int reused = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    reused++;
                }
            }
            return reused;
        }

        public synchronized long reusedBytes() {
            long reused = 0;
            for (File file : fileDetails.values()) {
                if (file.reused()) {
                    reused += file.length();
                }
            }
            return reused;
        }

        public synchronized void updateVersion(long version) {
            this.version = version;
        }

        @Override
        public synchronized void readFrom(StreamInput in) throws IOException {
            if (in.getVersion().before(Version.V_1_5_0)) {
                startTime = in.readVLong();
                long time = in.readVLong();
                stopTime = startTime + time;
                // This may result in skewed reports as we didn't report all files in advance, relying on this totals
                in.readVInt(); // totalFileCount
                in.readVLong(); // totalBytes
                in.readVInt(); // reusedFileCount
                in.readVLong(); // reusedByteCount
                in.readVInt(); // recoveredFileCount
                in.readVLong(); // recoveredByteCount
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    File file = File.readFile(in);
                    fileDetails.put(file.name, file);
                }
                size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    File file = File.readFile(in);
                    fileDetails.put(file.name, file);
                }
            } else {
                super.readFrom(in);
                int size = in.readVInt();
                for (int i = 0; i < size; i++) {
                    File file = File.readFile(in);
                    fileDetails.put(file.name, file);
                }
                sourceThrottlingInNanos = in.readLong();
                targetThrottleTimeInNanos = in.readLong();
            }
        }

        @Override
        public synchronized void writeTo(StreamOutput out) throws IOException {
            if (out.getVersion().before(Version.V_1_5_0)) {
                out.writeVLong(startTime);
                out.writeVLong(time());
                out.writeVInt(totalFileCount());
                out.writeVLong(totalBytes());
                out.writeVInt(reusedFileCount());
                out.writeVLong(reusedBytes());
                out.writeVInt(recoveredFileCount());
                out.writeVLong(recoveredBytes());
                final File[] files = fileDetails.values().toArray(new File[0]);
                int nonReusedCount = 0;
                int reusedCount = 0;
                for (File file : files) {
                    if (file.reused()) {
                        reusedCount++;
                    } else {
                        nonReusedCount++;
                    }
                }
                out.writeVInt(nonReusedCount);
                for (File file : files) {
                    if (file.reused() == false) {
                        file.writeTo(out);
                    }
                }
                out.writeVInt(reusedCount);
                for (File file : files) {
                    if (file.reused()) {
                        file.writeTo(out);
                    }
                }
            } else {
                super.writeTo(out);
                final File[] files = fileDetails.values().toArray(new File[0]);
                out.writeVInt(files.length);
                for (File file : files) {
                    file.writeTo(out);
                }
                out.writeLong(sourceThrottlingInNanos);
                out.writeLong(targetThrottleTimeInNanos);
            }
        }

        @Override
        public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // stream size first, as it matters more and the files section can be long
            builder.startObject(Fields.SIZE);
            builder.byteSizeField(Fields.TOTAL_IN_BYTES, Fields.TOTAL, totalBytes());
            builder.byteSizeField(Fields.REUSED_IN_BYTES, Fields.REUSED, reusedBytes());
            builder.byteSizeField(Fields.RECOVERED_IN_BYTES, Fields.RECOVERED, recoveredBytes());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredBytesPercent()));
            builder.endObject();

            builder.startObject(Fields.FILES);
            builder.field(Fields.TOTAL, totalFileCount());
            builder.field(Fields.REUSED, reusedFileCount());
            builder.field(Fields.RECOVERED, recoveredFileCount());
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredFilesPercent()));
            if (params.paramAsBoolean("details", false)) {
                builder.startArray(Fields.DETAILS);
                for (File file : fileDetails.values()) {
                    file.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, time());
            builder.timeValueField(Fields.SOURCE_THROTTLE_TIME_IN_MILLIS, Fields.SOURCE_THROTTLE_TIME, sourceThrottling());
            builder.timeValueField(Fields.TARGET_THROTTLE_TIME_IN_MILLIS, Fields.TARGET_THROTTLE_TIME, targetThrottling());
            return builder;
        }

        @Override
        public synchronized String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return builder.string();
            } catch (IOException e) {
                return "{ \"error\" : \"" + e.getMessage() + "\"}";
            }
        }
    }
}
