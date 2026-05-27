/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.remote.RemoteStoreSegmentStrategy;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TarSegmentUploadStrategy.
 */
public class TarSegmentUploadStrategyTests extends OpenSearchTestCase {

    public void testUploadTarArchiveStructureAndRead() throws IOException {
        Directory storeDirectory = new ByteBuffersDirectory();
        Directory remoteDataInnerDir = new ByteBuffersDirectory();

        // Create local dummy segment files with valid Lucene footer
        byte[] cfeContent = "cfe-dummy-data-bytes".getBytes(StandardCharsets.UTF_8);
        byte[] cfsContent = "cfs-dummy-data-bytes-longer-content".getBytes(StandardCharsets.UTF_8);

        try (IndexOutput out = storeDirectory.createOutput("_1.cfe", IOContext.DEFAULT)) {
            out.writeBytes(cfeContent, 0, cfeContent.length);
            CodecUtil.writeFooter(out);
        }
        try (IndexOutput out = storeDirectory.createOutput("_1.cfs", IOContext.DEFAULT)) {
            out.writeBytes(cfsContent, 0, cfsContent.length);
            CodecUtil.writeFooter(out);
        }

        // Instantiate real RemoteDirectory subclasses that delegate to ByteBuffersDirectory
        BlobContainer dummyBlobContainer = mock(BlobContainer.class);
        RemoteDirectory remoteDataDir = new RemoteDirectory(dummyBlobContainer) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
                remoteDataInnerDir.copyFrom(from, src, dest, context);
            }

            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                return remoteDataInnerDir.openInput(name, context);
            }

            @Override
            public IndexInput openBlockInput(String name, long position, long length, long fileLength, IOContext context)
                throws IOException {
                if (position < 0 || length <= 0 || (position + length > fileLength)) {
                    throw new IllegalArgumentException(
                        "Invalid values of block start and size: position=" + position + ", length=" + length + ", fileLength=" + fileLength
                    );
                }
                IndexInput fullInput = remoteDataInnerDir.openInput(name, context);
                return fullInput.slice("slice of " + name, position, length);
            }

            @Override
            public long fileLength(String name) throws IOException {
                return remoteDataInnerDir.fileLength(name);
            }

            @Override
            public String[] listAll() throws IOException {
                return remoteDataInnerDir.listAll();
            }

            @Override
            public boolean copyFrom(
                Directory from,
                String src,
                String remoteFileName,
                IOContext context,
                Runnable postUploadRunner,
                ActionListener<Void> listener,
                boolean lowPriorityUpload,
                CryptoMetadata cryptoMetadata
            ) {
                try {
                    remoteDataInnerDir.copyFrom(from, src, remoteFileName, context);
                    postUploadRunner.run();
                    listener.onResponse(null);
                } catch (IOException e) {
                    listener.onFailure(e);
                }
                return true;
            }
        };

        when(dummyBlobContainer.readBlob(anyString(), anyLong(), anyLong())).thenAnswer(invocation -> {
            String blobName = invocation.getArgument(0);
            long position = invocation.getArgument(1);
            long length = invocation.getArgument(2);
            IndexInput fullInput = remoteDataInnerDir.openInput(blobName, IOContext.DEFAULT);
            byte[] bytes = new byte[(int) length];
            fullInput.seek(position);
            fullInput.readBytes(bytes, 0, (int) length);
            fullInput.close();
            return new ByteArrayInputStream(bytes);
        });

        RemoteDirectory remoteMetadataDir = new RemoteDirectory(dummyBlobContainer) {
            @Override
            public List<String> listFilesByPrefixInLexicographicOrder(String filenamePrefix, int limit) throws IOException {
                return Collections.emptyList();
            }
        };

        RemoteStoreLockManager mdLockManager = mock(RemoteStoreLockManager.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ShardId shardId = new ShardId("index", "uuid", 0);

        RemoteSegmentStoreDirectory remoteDirectory = new RemoteSegmentStoreDirectory(
            remoteDataDir,
            remoteMetadataDir,
            mdLockManager,
            threadPool,
            shardId,
            new HashMap<>()
        );

        // Perform Upload
        final org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint checkpoint =
            new org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint(
                shardId,
                1L,
                1L,
                0L,
                0L,
                "codec",
                Collections.emptyMap(),
                0L
            );
        RemoteStoreSegmentStrategy strategy = new TarSegmentUploadStrategy();
        remoteDirectory.setActiveStrategy(strategy);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        RemoteStoreSegmentStrategy.UploadListener listener = new RemoteStoreSegmentStrategy.UploadListener() {
            @Override
            public void onUploadStart(String file) {}

            @Override
            public void onUploadSuccess(String file) {}

            @Override
            public void onUploadFailure(String file, Exception ex) {}

            @Override
            public void onAllUploadsSuccess() {
                listenerCalled.set(true);
            }

            @Override
            public void onAllUploadsFailure(Exception ex) {
                fail("Upload failed: " + ex.getMessage());
            }
        };

        RemoteStoreSegmentStrategy.UploadContext context = new RemoteStoreSegmentStrategy.UploadContext(
            Arrays.asList(
                new RemoteStoreSegmentStrategy.UploadContext.SegmentFile("_1.cfe", true),
                new RemoteStoreSegmentStrategy.UploadContext.SegmentFile("_1.cfs", true)
            ),
            storeDirectory,
            checkpoint,
            false,
            null
        );

        strategy.upload(remoteDirectory, shardId, context, listener);

        assertTrue(listenerCalled.get());

        // Check cache additions in remoteDirectory
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteDirectory
            .getSegmentsUploadedToRemoteStore();
        assertEquals(2, uploadedSegments.size());
        assertTrue(uploadedSegments.containsKey("_1.cfe"));
        assertTrue(uploadedSegments.containsKey("_1.cfs"));

        String uploadedCfePath = uploadedSegments.get("_1.cfe").getUploadedFilename();
        String uploadedCfsPath = uploadedSegments.get("_1.cfs").getUploadedFilename();

        assertTrue(uploadedCfePath.contains("#"));
        assertTrue(uploadedCfsPath.contains("#"));

        String[] cfeParts = uploadedCfePath.split("#");
        String remoteTarName = cfeParts[0];

        // Verify temporary local file is deleted
        assertEquals(2, storeDirectory.listAll().length); // only "_1.cfe" and "_1.cfs" should remain

        // Verify remote tar is created
        assertTrue(Arrays.asList(remoteDataInnerDir.listAll()).contains(remoteTarName));

        // Read physical tar and verify content using TarInputStream
        try (IndexInput tarInput = remoteDataInnerDir.openInput(remoteTarName, IOContext.DEFAULT)) {
            byte[] tarBytes = new byte[(int) tarInput.length()];
            tarInput.readBytes(tarBytes, 0, tarBytes.length);

            try (TarInputStream tis = new TarInputStream(new ByteArrayInputStream(tarBytes))) {
                // First Entry: index.bin
                TarInputStream.TarEntry entry = tis.getNextEntry();
                assertNotNull(entry);
                assertEquals("index.bin", entry.getName());
                byte[] indexBinData = tis.readAllBytes();
                assertTrue(indexBinData.length > 0);

                // Deserialize and verify using VersionedCodecStreamWrapper
                final VersionedCodecStreamWrapper<RemoteSegmentMetadata> wrapper = new VersionedCodecStreamWrapper<>(
                    new RemoteSegmentMetadataHandlerFactory(),
                    RemoteSegmentMetadata.VERSION_ONE,
                    RemoteSegmentMetadata.CURRENT_VERSION,
                    RemoteSegmentMetadata.METADATA_CODEC
                );
                final RemoteSegmentMetadata deserialized = wrapper.readStream(new ByteArrayIndexInput("index.bin", indexBinData));
                assertNotNull(deserialized);
                assertEquals(2, deserialized.getMetadata().size());
                assertTrue(deserialized.getMetadata().containsKey("_1.cfe"));
                assertTrue(deserialized.getMetadata().containsKey("_1.cfs"));

                // Second Entry: _1.cfe
                entry = tis.getNextEntry();
                assertNotNull(entry);
                assertEquals("_1.cfe", entry.getName());
                byte[] readCfe = tis.readAllBytes();
                try (IndexInput localInput = storeDirectory.openInput("_1.cfe", IOContext.DEFAULT)) {
                    byte[] originalCfeBytes = new byte[(int) localInput.length()];
                    localInput.readBytes(originalCfeBytes, 0, originalCfeBytes.length);
                    assertArrayEquals(originalCfeBytes, readCfe);
                }

                // Third Entry: _1.cfs
                entry = tis.getNextEntry();
                assertNotNull(entry);
                assertEquals("_1.cfs", entry.getName());
                byte[] readCfs = tis.readAllBytes();
                try (IndexInput localInput = storeDirectory.openInput("_1.cfs", IOContext.DEFAULT)) {
                    byte[] originalCfsBytes = new byte[(int) localInput.length()];
                    localInput.readBytes(originalCfsBytes, 0, originalCfsBytes.length);
                    assertArrayEquals(originalCfsBytes, readCfs);
                }

                assertNull(tis.getNextEntry());
            }
        }

        // Test reading segment data via RemoteSegmentStoreDirectory openInput (our offset parsing + block input logic)
        try (IndexInput cfeInput = remoteDirectory.openInput("_1.cfe", IOContext.DEFAULT)) {
            byte[] readBytes = new byte[(int) cfeInput.length()];
            cfeInput.readBytes(readBytes, 0, readBytes.length);
            try (IndexInput localInput = storeDirectory.openInput("_1.cfe", IOContext.DEFAULT)) {
                byte[] originalCfeBytes = new byte[(int) localInput.length()];
                localInput.readBytes(originalCfeBytes, 0, originalCfeBytes.length);
                assertArrayEquals(originalCfeBytes, readBytes);
            }
        }

        try (IndexInput cfsInput = remoteDirectory.openInput("_1.cfs", IOContext.DEFAULT)) {
            byte[] readBytes = new byte[(int) cfsInput.length()];
            cfsInput.readBytes(readBytes, 0, readBytes.length);
            try (IndexInput localInput = storeDirectory.openInput("_1.cfs", IOContext.DEFAULT)) {
                byte[] originalCfsBytes = new byte[(int) localInput.length()];
                localInput.readBytes(originalCfsBytes, 0, originalCfsBytes.length);
                assertArrayEquals(originalCfsBytes, readBytes);
            }
        }
    }
}
