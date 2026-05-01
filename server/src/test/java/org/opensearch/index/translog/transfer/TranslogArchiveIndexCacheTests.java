/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link TranslogArchiveIndexCache}: LRU eviction, put/get, clear.
 */
public class TranslogArchiveIndexCacheTests extends OpenSearchTestCase {

    public void testPutAndGet() {
        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache(10);
        List<TarArchiveBuilder.EntryLocation> entries = Collections.emptyList();
        cache.put("path/", "blob.tar", entries);
        assertSame("should return cached list", entries, cache.get("path/", "blob.tar"));
    }

    public void testGetMiss() {
        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache(10);
        assertNull("cache miss should return null", cache.get("path/", "missing.tar"));
    }

    public void testLruEviction() {
        int maxEntries = 3;
        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache(maxEntries);
        List<TarArchiveBuilder.EntryLocation> e1 = Collections.emptyList();
        List<TarArchiveBuilder.EntryLocation> e2 = Collections.emptyList();
        List<TarArchiveBuilder.EntryLocation> e3 = Collections.emptyList();
        List<TarArchiveBuilder.EntryLocation> e4 = Collections.emptyList();

        cache.put("p/", "a.tar", e1);
        cache.put("p/", "b.tar", e2);
        cache.put("p/", "c.tar", e3);
        assertEquals(3, cache.size());

        // Access "a.tar" to make it recently used
        cache.get("p/", "a.tar");

        // Add 4th entry → "b.tar" (LRU) should be evicted
        cache.put("p/", "d.tar", e4);
        assertEquals(3, cache.size());
        assertNull("b.tar should be evicted (LRU)", cache.get("p/", "b.tar"));
        assertNotNull("a.tar should still be in cache (recently accessed)", cache.get("p/", "a.tar"));
        assertNotNull("c.tar should still be in cache", cache.get("p/", "c.tar"));
        assertNotNull("d.tar should be in cache", cache.get("p/", "d.tar"));
    }

    public void testClear() {
        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache(10);
        cache.put("p/", "a.tar", Collections.emptyList());
        cache.put("p/", "b.tar", Collections.emptyList());
        cache.clear();
        assertEquals(0, cache.size());
        assertNull(cache.get("p/", "a.tar"));
    }

    public void testSize() {
        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache(10);
        assertEquals(0, cache.size());
        cache.put("p/", "a.tar", Collections.emptyList());
        assertEquals(1, cache.size());
        cache.put("p/", "b.tar", Collections.emptyList());
        assertEquals(2, cache.size());
    }

    public void testInvalidMaxEntriesThrows() {
        expectThrows(IllegalArgumentException.class, () -> new TranslogArchiveIndexCache(0));
        expectThrows(IllegalArgumentException.class, () -> new TranslogArchiveIndexCache(-1));
    }

    public void testUpdateExistingKey() {
        TranslogArchiveIndexCache cache = new TranslogArchiveIndexCache(10);
        List<TarArchiveBuilder.EntryLocation> v1 = Collections.emptyList();
        // v2 is a different (but equally empty) list instance to verify put overwrites
        List<TarArchiveBuilder.EntryLocation> v2 = new java.util.ArrayList<>();
        cache.put("p/", "a.tar", v1);
        cache.put("p/", "a.tar", v2);
        assertSame("should return the updated value", v2, cache.get("p/", "a.tar"));
        assertEquals("size should remain 1", 1, cache.size());
    }
}
