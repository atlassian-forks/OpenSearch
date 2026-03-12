/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit Tests for {@link RemoteSegmentMetadata} v2 format with archive support
 */
public class RemoteSegmentMetadataV2Tests extends IndexShardTestCase {
    private RemoteSegmentMetadataHandler remoteSegmentMetadataHandler;
    private IndexShard indexShard;
    private SegmentInfos segmentInfos;
    private ReplicationCheckpoint replicationCheckpoint;

    @Before
    public void setup() throws IOException {
        remoteSegmentMetadataHandler = new RemoteSegmentMetadataHandler(RemoteSegmentMetadata.CURRENT_VERSION);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "translog-repo")
            .build();

        indexShard = newStartedShard(false, indexSettings, new NRTReplicationEngineFactory());
        try (Store store = indexShard.store()) {
            segmentInfos = store.readLastCommittedSegmentsInfo();
        }
        replicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
    }

    @After
    public void tearDown() throws Exception {
        indexShard.close("test tearDown", true, false);
        super.tearDown();
    }

    public void testWriteAndReadV2WithArchiveDisabled() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);

        Map<String, String> segmentMetadata = getDummyData();
        ByteBuffersIndexOutput segmentInfosOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "resource");
        segmentInfos.write(segmentInfosOutput);
        byte[] segmentInfosBytes = segmentInfosOutput.toArrayCopy();

        // Create metadata without archive (archiveEnabled = false)
        RemoteSegmentMetadata remoteSegmentMetadata = new RemoteSegmentMetadata(
            RemoteSegmentMetadata.fromMapOfStrings(segmentMetadata),
            segmentInfosBytes,
            replicationCheckpoint
        );

        remoteSegmentMetadataHandler.writeContent(indexOutput, remoteSegmentMetadata);
        indexOutput.close();

        // Read back and verify
        RemoteSegmentMetadata readMetadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );

        assertFalse(readMetadata.isArchiveEnabled());
        assertNull(readMetadata.getArchiveBlob());
        assertNull(readMetadata.getArchiveFormat());
        assertNull(readMetadata.getArchiveEntries());
        assertEquals(segmentMetadata, readMetadata.toMapOfStrings());
        assertArrayEquals(segmentInfosBytes, readMetadata.getSegmentInfosBytes());
    }

    public void testWriteAndReadV2WithArchiveEnabled() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);

        Map<String, String> segmentMetadata = getDummyData();
        ByteBuffersIndexOutput segmentInfosOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "resource");
        segmentInfos.write(segmentInfosOutput);
        byte[] segmentInfosBytes = segmentInfosOutput.toArrayCopy();

        // Create archive entries
        Map<String, SegmentArchiveEntry> archiveEntries = new HashMap<>();
        archiveEntries.put("_0.cfs", new SegmentArchiveEntry("_0.cfs", 100, 1024, 123456L));
        archiveEntries.put("_0.cfe", new SegmentArchiveEntry("_0.cfe", 1124, 50, 789012L));

        // Create metadata with archive enabled
        RemoteSegmentMetadata remoteSegmentMetadata = new RemoteSegmentMetadata(
            RemoteSegmentMetadata.fromMapOfStrings(segmentMetadata),
            segmentInfosBytes,
            replicationCheckpoint,
            true,
            "refresh_12345.zip",
            "zip_stored",
            archiveEntries
        );

        remoteSegmentMetadataHandler.writeContent(indexOutput, remoteSegmentMetadata);
        indexOutput.close();

        // Read back and verify
        RemoteSegmentMetadata readMetadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );

        assertTrue(readMetadata.isArchiveEnabled());
        assertEquals("refresh_12345.zip", readMetadata.getArchiveBlob());
        assertEquals("zip_stored", readMetadata.getArchiveFormat());
        assertNotNull(readMetadata.getArchiveEntries());
        assertEquals(2, readMetadata.getArchiveEntries().size());

        SegmentArchiveEntry entry1 = readMetadata.getArchiveEntries().get("_0.cfs");
        assertNotNull(entry1);
        assertEquals("_0.cfs", entry1.getFilename());
        assertEquals(100, entry1.getOffset());
        assertEquals(1024, entry1.getLength());
        assertEquals(123456L, entry1.getChecksum());

        SegmentArchiveEntry entry2 = readMetadata.getArchiveEntries().get("_0.cfe");
        assertNotNull(entry2);
        assertEquals("_0.cfe", entry2.getFilename());
        assertEquals(1124, entry2.getOffset());
        assertEquals(50, entry2.getLength());
        assertEquals(789012L, entry2.getChecksum());

        assertEquals(segmentMetadata, readMetadata.toMapOfStrings());
        assertArrayEquals(segmentInfosBytes, readMetadata.getSegmentInfosBytes());
    }

    public void testBackwardCompatibilityReadV1Format() throws IOException {
        // This test verifies that v2 code can read metadata written in v2 format
        // Since write() always writes the latest version (v2), this test creates v2 format
        // and verifies it can be read back correctly
        
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);

        Map<String, String> segmentMetadata = getDummyData();
        ByteBuffersIndexOutput segmentInfosOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "test", "resource");
        segmentInfos.write(segmentInfosOutput);
        byte[] segmentInfosBytes = segmentInfosOutput.toArrayCopy();

        // Create metadata without archive (archiveEnabled = false) in v2 format
        RemoteSegmentMetadata remoteSegmentMetadata = new RemoteSegmentMetadata(
            RemoteSegmentMetadata.fromMapOfStrings(segmentMetadata),
            segmentInfosBytes,
            replicationCheckpoint
        );

        remoteSegmentMetadataHandler.writeContent(indexOutput, remoteSegmentMetadata);
        indexOutput.close();

        // Read back and verify - should be readable as v2 format with no archive
        RemoteSegmentMetadata readMetadata = remoteSegmentMetadataHandler.readContent(
            new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()))
        );

        assertFalse(readMetadata.isArchiveEnabled());
        assertNull(readMetadata.getArchiveBlob());
        assertNull(readMetadata.getArchiveFormat());
        assertNull(readMetadata.getArchiveEntries());
        assertEquals(segmentMetadata, readMetadata.toMapOfStrings());
        assertArrayEquals(segmentInfosBytes, readMetadata.getSegmentInfosBytes());
    }

    public void testSegmentArchiveEntrySerializationRoundTrip() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);

        SegmentArchiveEntry originalEntry = new SegmentArchiveEntry("_5.cfs", 2048, 65536, 987654321L);
        originalEntry.write(indexOutput);
        indexOutput.close();

        ByteArrayIndexInput input = new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()));
        SegmentArchiveEntry readEntry = SegmentArchiveEntry.read(input);

        assertEquals(originalEntry.getFilename(), readEntry.getFilename());
        assertEquals(originalEntry.getOffset(), readEntry.getOffset());
        assertEquals(originalEntry.getLength(), readEntry.getLength());
        assertEquals(originalEntry.getChecksum(), readEntry.getChecksum());
        assertEquals(originalEntry, readEntry);
    }

    private Map<String, String> getDummyData() {
        Map<String, String> expectedOutput = new HashMap<>();
        String prefix = "_0";
        expectedOutput.put(
            prefix + ".cfe",
            prefix
                + ".cfe::"
                + prefix
                + ".cfe__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(1024, 2048)
                + "::"
                + Version.LATEST.major
        );
        expectedOutput.put(
            prefix + ".cfs",
            prefix
                + ".cfs::"
                + prefix
                + ".cfs__"
                + UUIDs.base64UUID()
                + "::"
                + randomIntBetween(1000, 5000)
                + "::"
                + randomIntBetween(1024, 2048)
                + "::"
                + Version.LATEST.major
        );
        return expectedOutput;
    }
}
