/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.Optional;

/**
 * Tests for {@link TranslogArchivePathHelper}: path layout, blob name generation, and timestamp parsing.
 */
public class TranslogArchivePathHelperTests extends OpenSearchTestCase {

    // ── Hierarchical path helpers ─────────────────────────────────────────────

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
     * Asserts exact full path, not just substring presence.
     */
    public void testTarBlobDir() {
        Instant instant = Instant.parse("2026-05-01T22:30:45.123Z");
        BlobPath base = new BlobPath().add("repo-root");
        BlobPath dir = TranslogArchivePathHelper.tarBlobDir(base, instant);
        String pathStr = dir.buildAsString();
        // Exact full path: repo-root/txlog/20260501/2230/ (with or without trailing slash depending on impl)
        assertTrue(
            "full path must be 'repo-root/txlog/20260501/2230/' but was: " + pathStr,
            pathStr.equals("repo-root/txlog/20260501/2230/") || pathStr.equals("repo-root/txlog/20260501/2230")
        );
    }

    /**
     * tarBlobDir at midnight boundary: T23:59:00Z should use that day, T00:00:00Z the next.
     */
    public void testTarBlobDirMidnightBoundary() {
        Instant beforeMidnight = Instant.parse("2026-05-01T23:59:00Z");
        Instant atMidnight    = Instant.parse("2026-05-02T00:00:00Z");
        BlobPath base = new BlobPath().add("base");

        String dayBefore   = TranslogArchivePathHelper.tarBlobDir(base, beforeMidnight).buildAsString();
        String dayAtMidnight = TranslogArchivePathHelper.tarBlobDir(base, atMidnight).buildAsString();

        assertTrue("before midnight must use 20260501", dayBefore.contains("20260501"));
        assertTrue("at midnight must use 20260502", dayAtMidnight.contains("20260502"));
        assertFalse("before midnight must NOT use 20260502", dayBefore.contains("20260502"));
        assertFalse("at midnight must NOT use 20260501", dayAtMidnight.contains("20260501"));

        // Minute dirs
        assertTrue("23:59 → minute dir 2359", dayBefore.contains("2359"));
        assertTrue("00:00 → minute dir 0000", dayAtMidnight.contains("0000"));
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
