/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Unit tests for {@link TarTranslogRemoteStoreStrategy#extractGenerationFromTarStream}.
 *
 * <p>Uses commons-compress (test-only dep) to build valid TAR blobs,
 * then verifies our manual JDK-only parser correctly extracts files.
 */
public class TarTranslogDownloadTests extends OpenSearchTestCase {

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Build a minimal TAR blob containing the given entries (name → content). */
    private static byte[] buildTar(String... namesAndContents) throws IOException {
        assert namesAndContents.length % 2 == 0 : "must be name/content pairs";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (TarArchiveOutputStream tos = new TarArchiveOutputStream(bos)) {
            tos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            for (int i = 0; i < namesAndContents.length; i += 2) {
                byte[] data = namesAndContents[i + 1].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                TarArchiveEntry entry = new TarArchiveEntry(namesAndContents[i]);
                entry.setSize(data.length);
                tos.putArchiveEntry(entry);
                tos.write(data);
                tos.closeArchiveEntry();
            }
            tos.finish();
        }
        return bos.toByteArray();
    }

    private static InputStream tarStream(String... namesAndContents) throws IOException {
        return new ByteArrayInputStream(buildTar(namesAndContents));
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /** Both .tlog and .ckp present → returns true and writes both files. */
    public void testExtractsMatchingGeneration() throws IOException {
        long gen = 42L;
        String tlogName = org.opensearch.index.translog.Translog.getFilename(gen);
        String ckpName = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(gen);

        byte[] tar = buildTar(tlogName, "tlog-data", ckpName, "ckp-data");
        Path dir = createTempDir();

        boolean found = TarTranslogRemoteStoreStrategy.extractGenerationFromTarStream(
            new ByteArrayInputStream(tar),
            tlogName,
            ckpName,
            dir
        );

        assertTrue("Should report found=true when both files present", found);
        assertEquals("tlog-data", new String(Files.readAllBytes(dir.resolve(tlogName))));
        assertEquals("ckp-data", new String(Files.readAllBytes(dir.resolve(ckpName))));
    }

    /** Only .tlog present (no .ckp) → returns false. */
    public void testReturnsFalseWhenCkpMissing() throws IOException {
        long gen = 7L;
        String tlogName = org.opensearch.index.translog.Translog.getFilename(gen);
        String ckpName = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(gen);

        byte[] tar = buildTar(tlogName, "tlog-only");
        Path dir = createTempDir();

        boolean found = TarTranslogRemoteStoreStrategy.extractGenerationFromTarStream(
            new ByteArrayInputStream(tar),
            tlogName,
            ckpName,
            dir
        );

        assertFalse("Should return false when only .tlog is present", found);
    }

    /** Generation not present in TAR at all → returns false. */
    public void testReturnsFalseWhenGenerationAbsent() throws IOException {
        long gen = 100L;
        long otherGen = 99L;
        String tlogName = org.opensearch.index.translog.Translog.getFilename(gen);
        String ckpName = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(gen);
        String otherTlog = org.opensearch.index.translog.Translog.getFilename(otherGen);
        String otherCkp = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(otherGen);

        byte[] tar = buildTar(otherTlog, "data", otherCkp, "data");
        Path dir = createTempDir();

        boolean found = TarTranslogRemoteStoreStrategy.extractGenerationFromTarStream(
            new ByteArrayInputStream(tar),
            tlogName,
            ckpName,
            dir
        );

        assertFalse("Should return false when generation not in TAR", found);
    }

    /** Files stored with directory prefix (e.g. "indexUUID/shardId/file") are still extracted. */
    public void testStripsDirectoryPrefixFromEntryName() throws IOException {
        long gen = 3L;
        String tlogName = org.opensearch.index.translog.Translog.getFilename(gen);
        String ckpName = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(gen);

        // Entries have a directory prefix (as the TAR builder uses)
        byte[] tar = buildTar("abc-uuid/0/" + tlogName, "tlog-content", "abc-uuid/0/" + ckpName, "ckp-content");
        Path dir = createTempDir();

        boolean found = TarTranslogRemoteStoreStrategy.extractGenerationFromTarStream(
            new ByteArrayInputStream(tar),
            tlogName,
            ckpName,
            dir
        );

        assertTrue("Should strip dir prefix and match entry", found);
        assertEquals("tlog-content", new String(Files.readAllBytes(dir.resolve(tlogName))));
        assertEquals("ckp-content", new String(Files.readAllBytes(dir.resolve(ckpName))));
    }

    /** TAR with multiple generations — only the requested one is extracted. */
    public void testMultipleGenerationsOnlyExtracts​Requested() throws IOException {
        long targetGen = 5L;
        long otherGen = 6L;
        String tlogName = org.opensearch.index.translog.Translog.getFilename(targetGen);
        String ckpName = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(targetGen);
        String otherTlog = org.opensearch.index.translog.Translog.getFilename(otherGen);
        String otherCkp = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(otherGen);

        byte[] tar = buildTar(otherTlog, "other-tlog", otherCkp, "other-ckp", tlogName, "target-tlog", ckpName, "target-ckp");
        Path dir = createTempDir();

        boolean found = TarTranslogRemoteStoreStrategy.extractGenerationFromTarStream(
            new ByteArrayInputStream(tar),
            tlogName,
            ckpName,
            dir
        );

        assertTrue("Should find requested generation", found);
        assertEquals("target-tlog", new String(Files.readAllBytes(dir.resolve(tlogName))));
        assertEquals("target-ckp", new String(Files.readAllBytes(dir.resolve(ckpName))));
        // Other generation files should NOT have been written
        assertFalse("Should not write other generation's .tlog", Files.exists(dir.resolve(otherTlog)));
    }

    /** Empty TAR → returns false without throwing. */
    public void testEmptyTar() throws IOException {
        long gen = 1L;
        String tlogName = org.opensearch.index.translog.Translog.getFilename(gen);
        String ckpName = org.opensearch.index.translog.Translog.getCommitCheckpointFileName(gen);

        byte[] tar = buildTar(); // no entries
        Path dir = createTempDir();

        boolean found = TarTranslogRemoteStoreStrategy.extractGenerationFromTarStream(
            new ByteArrayInputStream(tar),
            tlogName,
            ckpName,
            dir
        );

        assertFalse("Empty TAR should return false", found);
    }
}
