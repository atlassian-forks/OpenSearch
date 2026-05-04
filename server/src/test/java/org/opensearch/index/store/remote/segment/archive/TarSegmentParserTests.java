/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import org.opensearch.index.store.remote.metadata.SegmentArchiveEntry;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link TarSegmentParser} — reads the TAR _index from the archive HEAD.
 */
public class TarSegmentParserTests extends OpenSearchTestCase {

    /**
     * Parse a built TAR archive and verify all entries are found with correct offsets.
     */
    public void testParseExtractsAllEntries() throws IOException {
        String path1 = "_0.si";
        byte[] content1 = "segment info".getBytes(StandardCharsets.UTF_8);
        String path2 = "_0.cfs";
        byte[] content2 = "compound file segment".getBytes(StandardCharsets.UTF_8);
        String path3 = "_0.cfe";
        byte[] content3 = "compound file entries".getBytes(StandardCharsets.UTF_8);

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = Arrays.asList(
            SegmentArchiveBuilder.fromBytes(path1, content1),
            SegmentArchiveBuilder.fromBytes(path2, content2),
            SegmentArchiveBuilder.fromBytes(path3, content3)
        );

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // When: parsing the TAR head
        Map<String, SegmentArchiveEntry> parsed = TarSegmentParser.parseToMap(tarBytes);

        // Then: all 3 entries present with correct lengths
        assertThat(parsed.size(), equalTo(3));

        SegmentArchiveEntry e1 = parsed.get(path1);
        assertThat(e1, notNullValue());
        assertThat(e1.getFilename(), equalTo(path1));
        assertThat(e1.getLength(), equalTo((long) content1.length));

        SegmentArchiveEntry e2 = parsed.get(path2);
        assertThat(e2, notNullValue());
        assertThat(e2.getFilename(), equalTo(path2));
        assertThat(e2.getLength(), equalTo((long) content2.length));

        SegmentArchiveEntry e3 = parsed.get(path3);
        assertThat(e3, notNullValue());
        assertThat(e3.getFilename(), equalTo(path3));
        assertThat(e3.getLength(), equalTo((long) content3.length));
    }

    /**
     * Parsed offsets allow byte-accurate range-reads of individual segment files.
     */
    public void testParsedOffsetsAllowRangeRead() throws IOException {
        Map<String, byte[]> files = new java.util.LinkedHashMap<>();
        files.put("_0.si", "segment info content".getBytes(StandardCharsets.UTF_8));
        files.put("_0.cfs", new byte[50_000]);
        Arrays.fill(files.get("_0.cfs"), (byte) 0xAB);
        files.put("_0.cfe", "entries content".getBytes(StandardCharsets.UTF_8));

        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = new java.util.ArrayList<>();
        for (Map.Entry<String, byte[]> f : files.entrySet()) {
            entries.add(SegmentArchiveBuilder.fromBytes(f.getKey(), f.getValue()));
        }

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        Map<String, SegmentArchiveEntry> parsed = TarSegmentParser.parseToMap(tarBytes);

        for (Map.Entry<String, byte[]> f : files.entrySet()) {
            SegmentArchiveEntry entry = parsed.get(f.getKey());
            assertThat("Entry must be found for " + f.getKey(), entry, notNullValue());
            byte[] extracted = Arrays.copyOfRange(tarBytes, (int) entry.getOffset(), (int) (entry.getOffset() + entry.getLength()));
            assertArrayEquals("Range-read must yield original for " + f.getKey(), f.getValue(), extracted);
        }
    }

    /**
     * computeHeadReadLength: returns the correct byte count to read from the archive start.
     */
    public void testComputeHeadReadLength() throws IOException {
        byte[] content = new byte[1024];
        Arrays.fill(content, (byte) 0xFF);
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = List.of(
            SegmentArchiveBuilder.fromBytes("_0.si", content)
        );

        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // Read just the first TAR header (512 bytes)
        byte[] header = Arrays.copyOfRange(tarBytes, 0, TarSegmentParser.TAR_HEADER_SIZE);
        int headLen = TarSegmentParser.computeHeadReadLength(header);

        assertTrue("Head read length must be positive", headLen > 0);
        assertTrue("Head read length must not exceed archive size", headLen <= tarBytes.length);

        // Reading headLen bytes from start must be sufficient to parse the full index
        byte[] head = Arrays.copyOfRange(tarBytes, 0, headLen);
        Map<String, SegmentArchiveEntry> parsed = TarSegmentParser.parseToMap(head);
        assertThat("Must parse 1 entry from head", parsed.size(), equalTo(1));
        assertThat(parsed.get("_0.si").getLength(), equalTo((long) content.length));
    }

    /**
     * parseTarSize: correctly reads the octal size field from a TAR header.
     */
    public void testParseTarSize() throws IOException {
        // Build a real TAR to get a real header
        byte[] content = new byte[12345];
        List<SegmentArchiveBuilder.SegmentArchiveBuildEntry> entries = List.of(
            SegmentArchiveBuilder.fromBytes("_0.cfs", content)
        );
        TarArchiveBuilder.TarLayout layout = SegmentArchiveBuilder.computeLayout(entries);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SegmentArchiveBuilder.buildAndExtractOffsets(out, layout, entries);
        byte[] tarBytes = out.toByteArray();

        // The first 512 bytes are the _index TAR header
        long indexSize = TarSegmentParser.parseTarSize(tarBytes, 0);
        assertTrue("Index size must be positive", indexSize > 0);
    }

    /**
     * Error case: buffer too short.
     */
    public void testParseToMapThrowsOnShortBuffer() {
        byte[] tooShort = new byte[100];
        expectThrows(IOException.class, () -> TarSegmentParser.parseToMap(tooShort));
    }

    /**
     * Error case: null buffer.
     */
    public void testParseToMapThrowsOnNullBuffer() {
        expectThrows(IOException.class, () -> TarSegmentParser.parseToMap(null));
    }
}
