/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentBlobLayout;
import org.opensearch.index.store.RemoteSegmentBlobLayoutFactory;
import org.opensearch.index.store.RemoteSegmentBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TarRemoteSegmentBlobLayoutTests extends OpenSearchTestCase {

    public void testLocationRoundTripAndMalformedLocation() throws Exception {
        TarLocation location = new TarLocation("bundle.tar", 42, 11);
        TarLocation parsed = TarLocation.parse(location.encode());

        assertEquals("bundle.tar", parsed.archive());
        assertEquals(42, parsed.offset());
        assertEquals(11, parsed.length());
        expectThrows(IOException.class, () -> TarLocation.parse("tar:v1:bad#not-a-number:1"));
        expectThrows(IOException.class, () -> TarLocation.parse("bundle.tar#1:2"));
        expectThrows(IOException.class, () -> TarLocation.parse("tar:v1:bundle.tar#-1:2"));
        expectThrows(IOException.class, () -> TarLocation.parse("tar:v1:bundle.tar"));
    }

    public void testPluginRegistersTarFactory() {
        Map<String, RemoteSegmentBlobLayoutFactory> factories = new RemoteSegmentTarPlugin().getRemoteSegmentBlobLayouts();
        assertEquals(1, factories.size());
        assertEquals(TarRemoteSegmentBlobLayoutFactory.NAME, factories.get(TarRemoteSegmentBlobLayoutFactory.NAME).getName());
        assertNotNull(
            factories.get(TarRemoteSegmentBlobLayoutFactory.NAME)
                .create(mock(RemoteSegmentBlobStore.class), new ShardId(new Index("index", "uuid"), 0))
        );
    }

    public void testEmptyBatchAndSyncFallback() throws Exception {
        RemoteSegmentBlobStore remoteStore = mock(RemoteSegmentBlobStore.class);
        TarRemoteSegmentBlobLayout layout = new TarRemoteSegmentBlobLayout(remoteStore);
        AtomicReference<Map<String, String>> locations = new AtomicReference<>();
        layout.writeBatch(
            context(checksummedDirectory(), List.of(), remoteStore),
            ActionListener.wrap(locations::set, exception -> fail(exception.getMessage()))
        );
        assertEquals(Map.of(), locations.get());

        Directory source = checksummedDirectory("a");
        when(remoteStore.copyFrom(any(), any(), any(), any(), any(), any(), anyBoolean(), any())).thenReturn(false);
        layout.writeBatch(
            context(source, List.of("a"), remoteStore),
            ActionListener.wrap(locations::set, exception -> fail(exception.getMessage()))
        );
        assertEquals(1, locations.get().size());
        verify(remoteStore).copyFrom(any(), any(), any(), any());
    }

    public void testFailedUploadDoesNotReturnLocationsAndDeletesArchiveBestEffort() throws Exception {
        RemoteSegmentBlobStore remoteStore = mock(RemoteSegmentBlobStore.class);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onFailure(new IOException("upload failed"));
            return true;
        }).when(remoteStore).copyFrom(any(), any(), any(), any(), any(), any(), anyBoolean(), any());
        AtomicReference<Exception> failure = new AtomicReference<>();

        new TarRemoteSegmentBlobLayout(remoteStore).writeBatch(
            context(checksummedDirectory("a"), List.of("a"), remoteStore),
            ActionListener.wrap(response -> fail("expected failure"), failure::set)
        );

        assertEquals("upload failed", failure.get().getMessage());
        verify(remoteStore).deleteFile(any());
    }

    public void testWriteBatchCreatesOneArchiveAndLocations() throws Exception {
        RemoteSegmentBlobStore remoteStore = mock(RemoteSegmentBlobStore.class);
        Directory source = checksummedDirectory("a", "b");
        AtomicReference<String> archive = new AtomicReference<>();
        AtomicReference<Map<String, String>> locations = new AtomicReference<>();
        doAnswer(invocation -> {
            Directory directory = invocation.getArgument(0);
            String temporaryName = invocation.getArgument(1);
            archive.set(invocation.getArgument(2));
            try (IndexInput input = directory.openInput(temporaryName, IOContext.DEFAULT)) {
                assertEquals("index.bin", readName(input));
            }
            ActionListener<Void> listener = invocation.getArgument(5);
            listener.onResponse(null);
            return true;
        }).when(remoteStore).copyFrom(any(), any(), any(), any(), any(), any(), anyBoolean(), any());

        TarRemoteSegmentBlobLayout layout = new TarRemoteSegmentBlobLayout(remoteStore);
        layout.writeBatch(
            new RemoteSegmentBlobLayout.WriteContext(
                source,
                List.of("a", "b"),
                remoteStore,
                new ShardId(new Index("index", "uuid"), 0),
                IOContext.DEFAULT,
                false,
                null
            ),
            ActionListener.wrap(locations::set, exception -> fail(exception.getMessage()))
        );

        assertNotNull(archive.get());
        assertTrue(archive.get().startsWith("rbs_segment_bundle__"));
        assertEquals(2, locations.get().size());
        assertTrue(locations.get().get("a").startsWith("tar:v1:" + archive.get() + "#"));
        assertTrue(locations.get().get("b").startsWith("tar:v1:" + archive.get() + "#"));
    }

    public void testReadsUseOnlyTheRequestedArchiveRange() throws Exception {
        RemoteSegmentBlobStore remoteStore = mock(RemoteSegmentBlobStore.class);
        when(remoteStore.openBlockInput(eq("bundle.tar"), eq(13L), eq(5L), eq(18L), any())).thenReturn(
            new ByteArrayIndexInput("bundle.tar", new byte[] { 1, 2, 3, 4, 5 })
        );
        TarRemoteSegmentBlobLayout layout = new TarRemoteSegmentBlobLayout(remoteStore);

        try (IndexInput input = layout.openInput("tar:v1:bundle.tar#13:5", 5, IOContext.DEFAULT)) {
            assertEquals(5, input.length());
            assertEquals((byte) 1, input.readByte());
        }
        verify(remoteStore).openBlockInput("bundle.tar", 13, 5, 18, IOContext.DEFAULT);
    }

    public void testBlockReadAndArchiveGc() throws Exception {
        RemoteSegmentBlobStore remoteStore = mock(RemoteSegmentBlobStore.class);
        when(remoteStore.openBlockInput(eq("bundle.tar"), eq(15L), eq(2L), eq(17L), any())).thenReturn(
            new ByteArrayIndexInput("bundle.tar", new byte[] { 7, 8 })
        );
        TarRemoteSegmentBlobLayout layout = new TarRemoteSegmentBlobLayout(remoteStore);

        try (IndexInput input = layout.openBlockInput("tar:v1:bundle.tar#13:5", 2, 2, 5, IOContext.DEFAULT)) {
            assertEquals((byte) 7, input.readByte());
        }
        layout.deleteUnreferenced(List.of("tar:v1:bundle.tar#13:5", "tar:v1:gone.tar#1:2"), Set.of("tar:v1:bundle.tar#20:4"));
        verify(remoteStore).deleteFiles(List.of("gone.tar"));
    }

    public void testRejectsInvalidRangesAndLengthMismatch() throws Exception {
        TarRemoteSegmentBlobLayout layout = new TarRemoteSegmentBlobLayout(mock(RemoteSegmentBlobStore.class));
        expectThrows(IOException.class, () -> layout.openInput("tar:v1:bundle.tar#1:2", 3, IOContext.DEFAULT));
        expectThrows(IOException.class, () -> layout.openBlockInput("tar:v1:bundle.tar#1:2", -1, 1, 2, IOContext.DEFAULT));
    }

    public void testTarArchiveRejectsLongNames() throws Exception {
        Directory directory = new ByteBuffersDirectory();
        try (IndexOutput output = directory.createOutput("tar", IOContext.DEFAULT)) {
            expectThrows(IllegalArgumentException.class, () -> TarArchive.writeEntry(output, "a".repeat(100), new byte[] { 1 }));
        }
    }

    private Directory checksummedDirectory(String... files) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        for (String file : files) {
            try (IndexOutput output = directory.createOutput(file, IOContext.DEFAULT)) {
                output.writeString(file);
                CodecUtil.writeFooter(output);
            }
        }
        return directory;
    }

    private RemoteSegmentBlobLayout.WriteContext context(Directory source, List<String> files, RemoteSegmentBlobStore remoteStore) {
        return new RemoteSegmentBlobLayout.WriteContext(
            source,
            files,
            remoteStore,
            new ShardId(new Index("index", "uuid"), 0),
            IOContext.DEFAULT,
            false,
            null
        );
    }

    private String readName(IndexInput input) throws IOException {
        byte[] bytes = new byte[100];
        input.readBytes(bytes, 0, bytes.length);
        int length = 0;
        while (bytes[length] != 0) {
            length++;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }
}
