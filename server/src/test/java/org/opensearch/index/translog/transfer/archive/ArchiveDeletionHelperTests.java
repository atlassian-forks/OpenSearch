/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ArchiveDeletionHelperTests extends OpenSearchTestCase {

    public void testParsePathValid() {
        Optional<ArchiveDeletionHelper.ParsedEntry> p = ArchiveDeletionHelper.parsePath("idx-uuid/0/1/translog-2.tlog");
        assertTrue(p.isPresent());
        assertThat(p.get().getShardKey(), equalTo("idx-uuid/0"));
        assertThat(p.get().getPrimaryTerm(), equalTo(1L));
        assertThat(p.get().getGeneration(), equalTo(2L));

        p = ArchiveDeletionHelper.parsePath("idx-uuid/0/1/translog-2.ckp");
        assertTrue(p.isPresent());
        assertThat(p.get().getGeneration(), equalTo(2L));

        p = ArchiveDeletionHelper.parsePath("index-uuid/1/99/translog-0.tlog");
        assertTrue(p.isPresent());
        assertThat(p.get().getShardKey(), equalTo("index-uuid/1"));
        assertThat(p.get().getPrimaryTerm(), equalTo(99L));
        assertThat(p.get().getGeneration(), equalTo(0L));
    }

    public void testParsePathInvalid() {
        assertTrue(ArchiveDeletionHelper.parsePath("").isEmpty());
        assertTrue(ArchiveDeletionHelper.parsePath("only/three/parts").isEmpty());
        assertTrue(ArchiveDeletionHelper.parsePath("a/b/notnumber/file.tlog").isEmpty());
        assertTrue(ArchiveDeletionHelper.parsePath("a/b/1/other.txt").isEmpty());
    }

    public void testIsArchiveDeletableAllDeletable() {
        String key = "idx-uuid/0";
        List<ArchiveEntry> entries = List.of(
            new ArchiveEntry("idx-uuid/0/1/translog-1.tlog", 0, 10),
            new ArchiveEntry("idx-uuid/0/1/translog-1.ckp", 10, 20)
        );
        Map<String, ArchiveDeletionHelper.RetentionBounds> retention = new HashMap<>();
        retention.put(key, new ArchiveDeletionHelper.RetentionBounds(1L, 2L));
        assertTrue(ArchiveDeletionHelper.isArchiveDeletable(entries, retention));
    }

    public void testIsArchiveDeletableOneNotDeletable() {
        String key = "idx-uuid/0";
        List<ArchiveEntry> entries = List.of(
            new ArchiveEntry("idx-uuid/0/1/translog-1.tlog", 0, 10),
            new ArchiveEntry("idx-uuid/0/1/translog-2.ckp", 10, 20)
        );
        Map<String, ArchiveDeletionHelper.RetentionBounds> retention = new HashMap<>();
        retention.put(key, new ArchiveDeletionHelper.RetentionBounds(1L, 2L));
        assertFalse(ArchiveDeletionHelper.isArchiveDeletable(entries, retention));
    }

    public void testIsArchiveDeletableUnknownShard() {
        List<ArchiveEntry> entries = List.of(new ArchiveEntry("idx-uuid/0/1/translog-1.tlog", 0, 10));
        Map<String, ArchiveDeletionHelper.RetentionBounds> retention = new HashMap<>();
        retention.put("other/0", new ArchiveDeletionHelper.RetentionBounds(1L, 2L));
        assertFalse(ArchiveDeletionHelper.isArchiveDeletable(entries, retention));
    }

    public void testIsArchiveDeletableEmptyRetention() {
        List<ArchiveEntry> entries = List.of(new ArchiveEntry("idx-uuid/0/1/translog-1.tlog", 0, 10));
        assertFalse(ArchiveDeletionHelper.isArchiveDeletable(entries, Collections.emptyMap()));
        assertFalse(ArchiveDeletionHelper.isArchiveDeletable(entries, null));
    }

    public void testIsArchiveDeletableEmptyEntries() {
        assertTrue(
            ArchiveDeletionHelper.isArchiveDeletable(
                Collections.emptyList(),
                Map.of("k", new ArchiveDeletionHelper.RetentionBounds(1L, 1L))
            )
        );
        assertTrue(ArchiveDeletionHelper.isArchiveDeletable(null, Map.of("k", new ArchiveDeletionHelper.RetentionBounds(1L, 1L))));
    }

    public void testRetentionBoundsAccessors() {
        ArchiveDeletionHelper.RetentionBounds b = new ArchiveDeletionHelper.RetentionBounds(2L, 5L);
        assertThat(b, notNullValue());
        assertThat(b.getMinPrimaryTermToKeep(), equalTo(2L));
        assertThat(b.getMinGenerationToKeep(), equalTo(5L));
    }

    // ---- Retention time-gate tests ----

    public void testEffectiveRetentionMinutesUsesConfiguredValue() {
        ArchiveDeletionHelper.RetentionBounds b = new ArchiveDeletionHelper.RetentionBounds(1L, 1L, 120L);
        assertThat(b.getEffectiveRetentionMinutes(), equalTo(120L));
    }

    public void testEffectiveRetentionMinutesSmallValue() {
        // Small retention values are allowed (setting min is 1s); returned as-is
        ArchiveDeletionHelper.RetentionBounds b = new ArchiveDeletionHelper.RetentionBounds(1L, 1L, 1L);
        assertThat(b.getEffectiveRetentionMinutes(), equalTo(1L));
    }

    public void testEffectiveRetentionMinutesZero() {
        // retentionMinutes = 0 → returned as 0 (no time gate)
        ArchiveDeletionHelper.RetentionBounds b = new ArchiveDeletionHelper.RetentionBounds(1L, 1L, 0L);
        assertThat(b.getEffectiveRetentionMinutes(), equalTo(0L));
    }

    public void testDefaultConstructorReturnZero() {
        // Two-arg constructor sets retentionMinutes = -1 → returns 0 (no time gate)
        ArchiveDeletionHelper.RetentionBounds b = new ArchiveDeletionHelper.RetentionBounds(1L, 1L);
        assertThat(b.getEffectiveRetentionMinutes(), equalTo(0L));
    }
}
