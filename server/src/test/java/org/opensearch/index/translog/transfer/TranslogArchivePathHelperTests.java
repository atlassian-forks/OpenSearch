/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;

/**
 * Tests for {@link TranslogArchivePathHelper}: path hashing, blob name generation, and timestamp parsing.
 */
public class TranslogArchivePathHelperTests extends OpenSearchTestCase {

    // --- hashTypeIndex ---

    /**
     * hashTypeIndex produces a non-empty, deterministic hash for a given indexUUID and algorithm.
     */
    public void testHashTypeIndexProducesDeterministicResult() {
        String hash1 = TranslogArchivePathHelper.hashTypeIndex("idx-uuid-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        String hash2 = TranslogArchivePathHelper.hashTypeIndex("idx-uuid-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        assertNotNull(hash1);
        assertFalse("hash should not be empty", hash1.isEmpty());
        assertEquals("same input should produce same hash", hash1, hash2);
    }

    /**
     * Different indexUUIDs produce different hashes (using realistic UUID-length inputs to avoid collisions).
     */
    public void testHashTypeIndexDifferentIndexUuids() {
        String hash1 = TranslogArchivePathHelper.hashTypeIndex(
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );
        String hash2 = TranslogArchivePathHelper.hashTypeIndex(
            "f9e8d7c6-b5a4-3210-fedc-ba0987654321",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );
        assertNotEquals("different UUIDs should produce different hashes", hash1, hash2);
    }

    // --- hashNodeId ---

    /**
     * hashNodeId produces a non-empty, deterministic hash.
     */
    public void testHashNodeIdDeterministic() {
        String hash1 = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        String hash2 = TranslogArchivePathHelper.hashNodeId("node-1", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1);
        assertNotNull(hash1);
        assertFalse(hash1.isEmpty());
        assertEquals(hash1, hash2);
    }

    /**
     * Different node IDs produce different hashes (using realistic node ID-length inputs).
     */
    public void testHashNodeIdDifferentNodes() {
        String hash1 = TranslogArchivePathHelper.hashNodeId(
            "node-a1b2c3d4e5f6g7h8i9j0",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );
        String hash2 = TranslogArchivePathHelper.hashNodeId(
            "node-z9y8x7w6v5u4t3s2r1q0",
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1
        );
        assertNotEquals(hash1, hash2);
    }

    // --- blobNameFromCurrentTime ---

    /**
     * blobNameFromCurrentTime returns a .zip file name in yyyyMMddHHmmssSSS format.
     */
    public void testBlobNameFromCurrentTimeFormat() {
        String name = TranslogArchivePathHelper.blobNameFromCurrentTime();
        assertNotNull(name);
        assertTrue("blob name should end with .zip", name.endsWith(".zip"));
        // yyyyMMddHHmmssSSS = 17 chars + ".zip" = 21 chars
        assertEquals("blob name should be 21 chars", 21, name.length());
        // The base part (without .zip) should be all digits
        String base = name.substring(0, 17);
        assertTrue("base should be all digits", base.matches("\\d{17}"));
    }

    /**
     * Two calls to blobNameFromCurrentTime produce names that are parseable as timestamps.
     */
    public void testBlobNameFromCurrentTimeIsParseable() {
        String name = TranslogArchivePathHelper.blobNameFromCurrentTime();
        Optional<Instant> parsed = TranslogArchivePathHelper.parseBlobNameTimestamp(name);
        assertTrue("generated blob name should be parseable", parsed.isPresent());
        // The timestamp should be close to now (within 10 seconds)
        Instant now = Instant.now();
        long diffMs = Math.abs(now.toEpochMilli() - parsed.get().toEpochMilli());
        assertTrue("parsed timestamp should be within 10s of now, diff=" + diffMs + "ms", diffMs < 10_000);
    }

    // --- parseBlobNameTimestamp ---

    /**
     * Valid blob name parses to correct Instant.
     */
    public void testParseBlobNameTimestampValid() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp("20260320181500123.zip");
        assertTrue(result.isPresent());
        // 2026-03-20T18:15:00.123Z
        Instant expected = Instant.parse("2026-03-20T18:15:00.123Z");
        assertEquals(expected, result.get());
    }

    /**
     * Null input returns empty.
     */
    public void testParseBlobNameTimestampNull() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp(null);
        assertFalse(result.isPresent());
    }

    /**
     * Non-.zip extension returns empty.
     */
    public void testParseBlobNameTimestampWrongExtension() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp("20260320181500123.tlog");
        assertFalse(result.isPresent());
    }

    /**
     * Wrong length base (not 17 digits) returns empty.
     */
    public void testParseBlobNameTimestampWrongLength() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp("2026032018.zip");
        assertFalse(result.isPresent());
    }

    /**
     * Non-numeric base returns empty.
     */
    public void testParseBlobNameTimestampNonNumeric() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp("abcdefghijklmnopq.zip");
        assertFalse(result.isPresent());
    }

    /**
     * Invalid date (month 99) returns empty.
     */
    public void testParseBlobNameTimestampInvalidDate() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp("20269920181500123.zip");
        assertFalse(result.isPresent());
    }

    /**
     * Empty string returns empty.
     */
    public void testParseBlobNameTimestampEmptyString() {
        Optional<Instant> result = TranslogArchivePathHelper.parseBlobNameTimestamp("");
        assertFalse(result.isPresent());
    }

    // ── New hierarchical path helpers ─────────────────────────────────────────

    /**
     * dayDir returns yyyyMMdd for a known instant.
     */
    public void testDayDir() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        assertEquals("20260501", TranslogArchivePathHelper.dayDir(instant));
    }

    /**
     * minuteDir returns HHmm for a known instant.
     */
    public void testMinuteDir() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        assertEquals("2230", TranslogArchivePathHelper.minuteDir(instant));
    }

    /**
     * tarBlobName returns ss.SSS.nodeIdShort.tar format.
     */
    public void testTarBlobNameFormat() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        String blobName = TranslogArchivePathHelper.tarBlobName(instant, "abc123de-xyz");
        // ss = 45, SSS = 123, nodeIdShort = "abc123de"
        assertEquals("45.123.abc123de.tar", blobName);
        assertTrue("should end with .tar", blobName.endsWith(".tar"));
    }

    /**
     * tarBlobDir returns base/txlog/day/minute/ path.
     */
    public void testTarBlobDir() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        BlobPath base = new BlobPath().add("repo-root");
        BlobPath dir = TranslogArchivePathHelper.tarBlobDir(base, instant);
        String pathStr = dir.buildAsString();
        assertTrue("should start with repo-root/txlog/", pathStr.startsWith("repo-root/txlog/"));
        assertTrue("should contain day", pathStr.contains("20260501"));
        assertTrue("should contain minute", pathStr.contains("2230"));
    }

    /**
     * txlogDayPath returns base/txlog/day/ path.
     */
    public void testTxlogDayPath() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        BlobPath base = new BlobPath().add("repo");
        BlobPath dayPath = TranslogArchivePathHelper.txlogDayPath(base, instant);
        assertTrue(dayPath.buildAsString().endsWith("txlog/20260501/")
            || dayPath.buildAsString().endsWith("txlog/20260501"));
    }

    /**
     * txlogRootPath returns base/txlog/.
     */
    public void testTxlogRootPath() {
        BlobPath base = new BlobPath().add("repo");
        BlobPath root = TranslogArchivePathHelper.txlogRootPath(base);
        assertTrue(root.buildAsString().contains("txlog"));
    }

    /**
     * parseTarBlobTimestamp round-trips with tarBlobName.
     */
    public void testParseTarBlobTimestampRoundTrip() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        String dayDir = TranslogArchivePathHelper.dayDir(instant);
        String minuteDir = TranslogArchivePathHelper.minuteDir(instant);
        String blobName = TranslogArchivePathHelper.tarBlobName(instant, "mynode1");

        Optional<Instant> parsed = TranslogArchivePathHelper.parseTarBlobTimestamp(dayDir, minuteDir, blobName);
        assertTrue("should parse", parsed.isPresent());
        assertEquals("parsed timestamp should match original", instant, parsed.get());
    }

    /**
     * parseTarBlobTimestamp returns empty for null inputs.
     */
    public void testParseTarBlobTimestampNullInputs() {
        assertFalse(TranslogArchivePathHelper.parseTarBlobTimestamp(null, "2230", "45.123.abc.tar").isPresent());
        assertFalse(TranslogArchivePathHelper.parseTarBlobTimestamp("20260501", null, "45.123.abc.tar").isPresent());
        assertFalse(TranslogArchivePathHelper.parseTarBlobTimestamp("20260501", "2230", null).isPresent());
    }

    /**
     * parseTarBlobTimestamp returns empty for non-.tar blobs.
     */
    public void testParseTarBlobTimestampNonTar() {
        assertFalse(TranslogArchivePathHelper.parseTarBlobTimestamp("20260501", "2230", "45.123.abc.zip").isPresent());
    }

    /**
     * isNewTarBlob recognizes valid new-format blob names.
     */
    public void testIsNewTarBlob() {
        assertTrue(TranslogArchivePathHelper.isNewTarBlob("45.123.abc123de.tar"));
        assertFalse("zip should not be new tar", TranslogArchivePathHelper.isNewTarBlob("20260501083045123.zip"));
        assertFalse("old flat tar should not be new tar", TranslogArchivePathHelper.isNewTarBlob("20260501083045123.tar"));
        assertFalse(TranslogArchivePathHelper.isNewTarBlob(null));
    }

    /**
     * shortNodeId returns first 8 alphanumeric chars of nodeId.
     */
    public void testShortNodeId() {
        assertEquals("abc123de", TranslogArchivePathHelper.shortNodeId("abc123de-xyz-extra"));
        assertEquals("00000000", TranslogArchivePathHelper.shortNodeId(null));
        assertEquals("abc00000", TranslogArchivePathHelper.shortNodeId("abc"));
    }
}
