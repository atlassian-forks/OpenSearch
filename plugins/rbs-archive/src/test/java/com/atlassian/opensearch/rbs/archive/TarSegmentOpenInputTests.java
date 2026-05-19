/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TarSegmentRemoteStoreStrategy#openInput}.
 *
 * <p>Each test builds a real TAR archive (via {@link TarArchiveBuilder}), plants bytes in a mock
 * {@link BlobContainer}, invokes {@code openInput()} and asserts the returned {@link IndexInput}
 * reads the correct file bytes.
 */
public class TarSegmentOpenInputTests extends OpenSearchTestCase {

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Builds a TAR archive containing a single file and returns its bytes along with the
     * offset and length of that file's data block inside the TAR.
     */
    static TarContents buildSingleFileTar(String tarPath, byte[] fileBytes) throws IOException {
        TarArchiveBuilder.ArchiveBuildEntry entry = TarArchiveBuilder.fromBytes(tarPath, fileBytes);
        java.util.List<TarArchiveBuilder.ArchiveBuildEntry> entries = java.util.List.of(entry);
        TarArchiveBuilder.TarLayout layout = TarArchiveBuilder.computeLayout(entries);

        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        TarArchiveBuilder.build(bos, layout, entries);

        TarArchiveBuilder.EntryLocation loc = layout.getEntries().get(0);
        return new TarContents(bos.toByteArray(), loc.getDataOffset(), loc.getDataLength());
    }

    /** Simple container for TAR bytes + one file's offset+length. */
    static final class TarContents {
        final byte[] tarBytes;
        final long dataOffset;
        final long dataLength;

        TarContents(byte[] tarBytes, long dataOffset, long dataLength) {
            this.tarBytes = tarBytes;
            this.dataOffset = dataOffset;
            this.dataLength = dataLength;
        }
    }

    /**
     * Creates a mock {@link BlobContainer} that serves range reads from the given TAR bytes.
     */
    static BlobContainer mockContainerWith(byte[] tarBytes) throws IOException {
        BlobContainer container = mock(BlobContainer.class);
        when(container.readBlob(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenAnswer(inv -> {
                long offset = inv.getArgument(1);
                long length = inv.getArgument(2);
                return new ByteArrayInputStream(tarBytes, (int) offset, (int) Math.min(length, tarBytes.length - offset));
            });
        return container;
    }

    /**
     * Constructs an {@link UploadedSegmentMetadata} with the given archive blob name, tarOffset,
     * and tarDataLength — mirroring what {@code postUploadForArchive()} sets.
     *
     * <p>Uses the S-5 extended {@code fromString()} format:
     * {@code originalName::archiveBlobName::checksum::length::writtenByMajor::tarOffset::tarDataLength}
     */
    static UploadedSegmentMetadata metadataFor(String originalName, String archiveBlobName, long tarOffset, long tarDataLength) {
        // Lucene major version 9 is max supported in OpenSearch 2.17; see UploadedSegmentMetadata.writtenByMajor.
        String s5Format = String.join("::",
            originalName, archiveBlobName, "dummy-checksum",
            String.valueOf(tarDataLength), "9",
            String.valueOf(tarOffset), String.valueOf(tarDataLength)
        );
        return UploadedSegmentMetadata.fromString(s5Format);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /** Creates a strategy with the given BlobContainer pre-injected (simulates post-upload state). */
    static TarSegmentRemoteStoreStrategy strategyWith(BlobContainer container) {
        return new TarSegmentRemoteStoreStrategy(container);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /** Basic range-GET: openInput returns IndexInput that reads the correct file bytes. */
    public void testOpenInputReadsCorrectBytesFromTar() throws Exception {
        byte[] fileBytes = "hello from segment file".getBytes(StandardCharsets.UTF_8);
        String tarPath = "segments_1";
        TarContents tar = buildSingleFileTar(tarPath, fileBytes);

        BlobContainer container = mockContainerWith(tar.tarBytes);
        String archiveBlobName = "segment_archive_abc";
        UploadedSegmentMetadata metadata = metadataFor(tarPath, archiveBlobName, tar.dataOffset, tar.dataLength);

        IndexInput input = strategyWith(container).openInput(tarPath, metadata);

        assertNotNull("openInput should return non-null IndexInput", input);
        assertEquals("IndexInput length should match file bytes length", fileBytes.length, input.length());

        byte[] readBack = new byte[fileBytes.length];
        input.readBytes(readBack, 0, readBack.length);
        assertEquals("Should read back the original file content",
            new String(fileBytes, StandardCharsets.UTF_8),
            new String(readBack, StandardCharsets.UTF_8));
        input.close();
    }

    /** Returns full content even when the file spans a partial TAR block. */
    public void testOpenInputHandlesNonBlockAlignedFile() throws Exception {
        // 17 bytes — not aligned to 512-byte TAR blocks
        byte[] fileBytes = "short-non-aligned".getBytes(StandardCharsets.UTF_8);
        TarContents tar = buildSingleFileTar("_si", fileBytes);

        BlobContainer container = mockContainerWith(tar.tarBytes);
        String archiveBlobName = "segment_archive_xyz";
        UploadedSegmentMetadata metadata = metadataFor("_si", archiveBlobName, tar.dataOffset, tar.dataLength);

        IndexInput input = strategyWith(container).openInput("_si", metadata);

        byte[] readBack = new byte[fileBytes.length];
        input.readBytes(readBack, 0, readBack.length);
        assertArrayEquals(fileBytes, readBack);
        input.close();
    }

    /** Missing tarOffset (-1) → throws IOException (no BlobContainer for per-file fallback in plugin). */
    public void testOpenInputThrowsWhenNoArchiveMetadata() throws Exception {
        BlobContainer container = mock(BlobContainer.class);
        // metadata with tarOffset == -1 (per-file upload, no TAR info); use 5-field format (no S-5 extension)
        UploadedSegmentMetadata metadata = UploadedSegmentMetadata.fromString("segments_1::segments_1.si::dummy::42::9");

        expectThrows(IOException.class, () -> strategyWith(container).openInput("segments_1", metadata));
    }

    /** Slice support: can slice IndexInput and read from a sub-range. */
    public void testOpenInputSupportsSlice() throws Exception {
        byte[] fileBytes = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        TarContents tar = buildSingleFileTar("seg_file", fileBytes);

        BlobContainer container = mockContainerWith(tar.tarBytes);
        String archiveBlobName = "segment_archive_slice";
        UploadedSegmentMetadata metadata = metadataFor("seg_file", archiveBlobName, tar.dataOffset, tar.dataLength);

        IndexInput full = strategyWith(container).openInput("seg_file", metadata);
        // slice(name, offset, length): offset=5 means bytes [5..15) = 'f','g','h','i','j','k','l','m','n','o'
        IndexInput slice = full.slice("test-slice", 5, 10);

        assertEquals(10L, slice.length());
        byte[] sliceBytes = new byte[10];
        slice.readBytes(sliceBytes, 0, 10);
        assertArrayEquals("fghijklmno".getBytes(StandardCharsets.UTF_8), sliceBytes);
        full.close();
    }

    /** Large file (> 1 MB) — verifies no truncation from InputStream reading. */
    public void testOpenInputHandlesLargeFile() throws Exception {
        byte[] fileBytes = new byte[1024 * 1024 + 13]; // 1 MB + 13 bytes
        random().nextBytes(fileBytes);
        TarContents tar = buildSingleFileTar("large_segment", fileBytes);

        BlobContainer container = mockContainerWith(tar.tarBytes);
        String archiveBlobName = "segment_archive_large";
        UploadedSegmentMetadata metadata = metadataFor("large_segment", archiveBlobName, tar.dataOffset, tar.dataLength);

        IndexInput input = strategyWith(container).openInput("large_segment", metadata);

        assertEquals(fileBytes.length, input.length());
        byte[] readBack = new byte[fileBytes.length];
        input.readBytes(readBack, 0, readBack.length);
        assertArrayEquals(fileBytes, readBack);
        input.close();
    }

    /** No BlobContainer injected (null) — i.e. no upload yet → openInput throws IOException. */
    public void testOpenInputThrowsWhenNoBlobContainer() throws Exception {
        UploadedSegmentMetadata metadata = metadataFor("seg", "segment_archive_x", 512L, 100L);
        // null dataContainer simulates node that has not uploaded any segment archives yet
        expectThrows(IOException.class, () -> new TarSegmentRemoteStoreStrategy((BlobContainer) null).openInput("seg", metadata));
    }
}
