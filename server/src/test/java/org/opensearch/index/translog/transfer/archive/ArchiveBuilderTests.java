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
}
