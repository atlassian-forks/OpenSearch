/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ArchiveBuilderTests extends OpenSearchTestCase {

    public void testBuildAndParseStoredZip() throws IOException {
        String path1 = "index-uuid/0/1/translog-2.tlog";
        byte[] content1 = "translog content one".getBytes(StandardCharsets.UTF_8);
        String path2 = "index-uuid/0/1/checkpoint-2.ckp";
        byte[] content2 = "checkpoint content".getBytes(StandardCharsets.UTF_8);

        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(path1, content1),
            ArchiveBuilder.fromBytes(path2, content2)
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.build(out, entries);
        byte[] zipBytes = out.toByteArray();

        long tailStart = zipBytes.length - 4096;
        if (tailStart < 0) tailStart = 0;
        int tailLen = zipBytes.length - (int) tailStart;
        byte[] tail = Arrays.copyOfRange(zipBytes, (int) tailStart, zipBytes.length);

        List<ArchiveEntry> parsed = ZipCentralDirectoryParser.parse(tail, tailStart);
        assertThat(parsed, hasSize(2));
        Map<String, ArchiveEntry> byPath = ZipCentralDirectoryParser.parseToMap(tail, tailStart);
        assertThat(byPath.get(path1).getDataLength(), equalTo((long) content1.length));
        assertThat(byPath.get(path2).getDataLength(), equalTo((long) content2.length));

        ArchiveEntry e1 = byPath.get(path1);
        byte[] extracted1 = Arrays.copyOfRange(zipBytes, (int) e1.getDataOffset(), (int) (e1.getDataOffset() + e1.getDataLength()));
        assertArrayEquals(content1, extracted1);

        ArchiveEntry e2 = byPath.get(path2);
        byte[] extracted2 = Arrays.copyOfRange(zipBytes, (int) e2.getDataOffset(), (int) (e2.getDataOffset() + e2.getDataLength()));
        assertArrayEquals(content2, extracted2);
    }

    public void testParseTailTooShortThrows() {
        byte[] shortTail = new byte[10];
        IOException e = expectThrows(IOException.class, () -> ZipCentralDirectoryParser.parse(shortTail, 0));
        assertTrue(e.getMessage().contains("Tail too short"));
    }

    public void testBuildWithCommentAndParseComment() throws IOException {
        String pathPrefix = "index-uuid/0/1/";
        byte[] tlogContent = "translog content".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "checkpoint content".getBytes(StandardCharsets.UTF_8);
        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(pathPrefix + "translog-2.tlog", tlogContent),
            ArchiveBuilder.fromBytes(pathPrefix + "translog-2.ckp", ckpContent)
        );
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        byte[] zipBytes = out.toByteArray();

        int tailLen = (int) Math.min(zipBytes.length, ZipCentralDirectoryParser.MAX_ZIP_TAIL_BYTES);
        byte[] tail = Arrays.copyOfRange(zipBytes, zipBytes.length - tailLen, zipBytes.length);
        String comment = ZipCentralDirectoryParser.getComment(tail);
        assertThat(comment, notNullValue());
        assertFalse(comment.isEmpty());

        List<ArchiveIndexEntry> indexEntries = ArchiveCommentFormat.parse(comment);
        assertThat(indexEntries, hasSize(1));
        ArchiveIndexEntry entry = indexEntries.get(0);
        assertThat(entry.getShardId(), equalTo(0));
        assertThat(entry.getPrimaryTerm(), equalTo(1L));
        assertThat(entry.getGeneration(), equalTo(2L));
        assertThat(entry.getTlogLength(), equalTo((long) tlogContent.length));
        assertThat(entry.getCkpLength(), equalTo((long) ckpContent.length));

        byte[] extractedTlog = Arrays.copyOfRange(
            zipBytes,
            (int) entry.getTlogOffset(),
            (int) (entry.getTlogOffset() + entry.getTlogLength())
        );
        byte[] extractedCkp = Arrays.copyOfRange(zipBytes, (int) entry.getCkpOffset(), (int) (entry.getCkpOffset() + entry.getCkpLength()));
        assertArrayEquals(tlogContent, extractedTlog);
        assertArrayEquals(ckpContent, extractedCkp);
    }

    /**
     * End-to-end: build a ZIP from 2 shards' translog files, compute offsets, then range-read each
     * shard's individual files back from the ZIP bytes — proving the full archive→recovery cycle works.
     */
    public void testMultiShardArchiveBuildAndIndividualRecover() throws IOException {
        // Shard 0: translog-5.tlog + translog-5.ckp
        String indexUUID = "test-index-uuid";
        byte[] shard0Tlog = "shard0 translog content gen5".getBytes(StandardCharsets.UTF_8);
        byte[] shard0Ckp = "shard0 ckp gen5".getBytes(StandardCharsets.UTF_8);
        String shard0TlogPath = indexUUID + "/0/1/translog-5.tlog";
        String shard0CkpPath = indexUUID + "/0/1/translog-5.ckp";

        // Shard 1: translog-3.tlog + translog-3.ckp
        byte[] shard1Tlog = "shard1 translog content gen3".getBytes(StandardCharsets.UTF_8);
        byte[] shard1Ckp = "shard1 ckp gen3".getBytes(StandardCharsets.UTF_8);
        String shard1TlogPath = indexUUID + "/1/1/translog-3.tlog";
        String shard1CkpPath = indexUUID + "/1/1/translog-3.ckp";

        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(shard0TlogPath, shard0Tlog),
            ArchiveBuilder.fromBytes(shard0CkpPath, shard0Ckp),
            ArchiveBuilder.fromBytes(shard1TlogPath, shard1Tlog),
            ArchiveBuilder.fromBytes(shard1CkpPath, shard1Ckp)
        );

        // Step 1: compute size + offsets (same as upload path)
        ArchiveBuilder.SizeAndOffsets sizeAndOffsets = ArchiveBuilder.computeSizeAndOffsetsWithComment(entries);
        assertThat(sizeAndOffsets.getOffsets(), hasSize(4));

        // Step 2: build the actual ZIP (same as upload stream)
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        byte[] zipBytes = out.toByteArray();
        assertThat((long) zipBytes.length, equalTo(sizeAndOffsets.getSize()));

        // Step 3: build offset map (same as what TranslogArchiveCollector populates in metadata)
        Map<String, String> offsetMap = new java.util.HashMap<>();
        for (ArchiveCommentFormat.PathOffsetLength pol : sizeAndOffsets.getOffsets()) {
            offsetMap.put(pol.getPath(), pol.getOffset() + "," + pol.getLength());
        }
        assertThat(offsetMap.size(), equalTo(4));

        // Step 4: recover each shard's files individually via range-read (simulated)
        // Shard 0 translog
        assertEntryExtractedCorrectly(zipBytes, offsetMap, shard0TlogPath, shard0Tlog);
        // Shard 0 checkpoint
        assertEntryExtractedCorrectly(zipBytes, offsetMap, shard0CkpPath, shard0Ckp);
        // Shard 1 translog — different shard, same ZIP
        assertEntryExtractedCorrectly(zipBytes, offsetMap, shard1TlogPath, shard1Tlog);
        // Shard 1 checkpoint
        assertEntryExtractedCorrectly(zipBytes, offsetMap, shard1CkpPath, shard1Ckp);
    }

    private void assertEntryExtractedCorrectly(byte[] zipBytes, Map<String, String> offsetMap, String entryPath, byte[] expectedContent) {
        String offsetLength = offsetMap.get(entryPath);
        assertNotNull("offset should exist for " + entryPath, offsetLength);
        String[] parts = offsetLength.split(",");
        int offset = Integer.parseInt(parts[0]);
        int length = Integer.parseInt(parts[1]);
        byte[] extracted = Arrays.copyOfRange(zipBytes, offset, offset + length);
        assertArrayEquals("content mismatch for " + entryPath, expectedContent, extracted);
    }

    public void testComputeSizeAndOffsetsWithComment() throws IOException {
        String tlogPath = "index-uuid/0/1/translog-5.tlog";
        String ckpPath = "index-uuid/0/1/translog-5.ckp";
        byte[] tlogContent = "translog data here".getBytes(StandardCharsets.UTF_8);
        byte[] ckpContent = "ckp data".getBytes(StandardCharsets.UTF_8);

        List<ArchiveBuilder.ArchiveBuildEntry> entries = Arrays.asList(
            ArchiveBuilder.fromBytes(tlogPath, tlogContent),
            ArchiveBuilder.fromBytes(ckpPath, ckpContent)
        );

        ArchiveBuilder.SizeAndOffsets result = ArchiveBuilder.computeSizeAndOffsetsWithComment(entries);
        assertThat(result.getSize(), org.hamcrest.Matchers.greaterThan(0L));
        assertThat(result.getOffsets(), hasSize(2));

        // Verify offsets match actual ZIP content
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArchiveBuilder.buildWithComment(out, entries);
        byte[] zipBytes = out.toByteArray();
        assertThat((long) zipBytes.length, equalTo(result.getSize()));

        // Verify each offset allows correct extraction
        for (ArchiveCommentFormat.PathOffsetLength pol : result.getOffsets()) {
            int offset = (int) pol.getOffset();
            int length = (int) pol.getLength();
            byte[] extracted = Arrays.copyOfRange(zipBytes, offset, offset + length);
            if (pol.getPath().equals(tlogPath)) {
                assertArrayEquals(tlogContent, extracted);
            } else if (pol.getPath().equals(ckpPath)) {
                assertArrayEquals(ckpContent, extracted);
            } else {
                fail("Unexpected path: " + pol.getPath());
            }
        }
    }
}
