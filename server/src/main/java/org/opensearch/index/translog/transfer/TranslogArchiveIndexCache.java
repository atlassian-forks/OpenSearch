/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.translog.transfer.archive.TarArchiveBuilder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Node-level LRU cache for parsed TAR {@code _index} entries.
 *
 * <p>During restore, multiple shards from the same node may need to read index entries from the
 * same TAR blob. Without caching, each shard would perform a separate byte-range GET to read the
 * {@code _index} entry. This cache stores the parsed
 * {@link TarArchiveBuilder.EntryLocation} list keyed by the TAR blob's full path string
 * ({@code blobPathStr + "/" + blobName}), so subsequent shards get a cache hit.
 *
 * <p><b>Eviction</b>: LRU — the least-recently-accessed entry is evicted when the cache exceeds
 * {@code maxEntries}. Since TAR blobs are immutable once uploaded, there is no explicit
 * invalidation; entries age out naturally.
 *
 * <p><b>Thread safety</b>: All access is synchronized on the internal map.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class TranslogArchiveIndexCache {

    /** Default maximum number of TAR index entries to cache. */
    public static final int DEFAULT_MAX_ENTRIES = 2000;

    private final Map<String, List<TarArchiveBuilder.EntryLocation>> cache;

    /**
     * Creates a cache with {@link #DEFAULT_MAX_ENTRIES} maximum entries.
     */
    public TranslogArchiveIndexCache() {
        this(DEFAULT_MAX_ENTRIES);
    }

    /**
     * Creates a cache with the specified maximum number of entries.
     *
     * @param maxEntries maximum number of TAR index entries to cache (must be &gt; 0)
     */
    public TranslogArchiveIndexCache(int maxEntries) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0, got: " + maxEntries);
        }
        this.cache = Collections.synchronizedMap(new LinkedHashMap<String, List<TarArchiveBuilder.EntryLocation>>(
            maxEntries, 0.75f, true /* accessOrder = LRU */) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, List<TarArchiveBuilder.EntryLocation>> eldest) {
                return size() > maxEntries;
            }
        });
    }

    /**
     * Returns the cached entry location list for the given TAR blob, or {@code null} if not cached.
     *
     * @param blobPathStr  the blob path as a string (e.g. from {@code BlobPath.buildAsString()})
     * @param blobName     the blob file name (e.g. {@code "45.123.a3f7b2c1.tar"})
     * @return the cached {@link TarArchiveBuilder.EntryLocation} list, or {@code null} on cache miss
     */
    public List<TarArchiveBuilder.EntryLocation> get(String blobPathStr, String blobName) {
        return cache.get(cacheKey(blobPathStr, blobName));
    }

    /**
     * Stores the entry location list for the given TAR blob in the cache.
     *
     * @param blobPathStr  the blob path as a string
     * @param blobName     the blob file name
     * @param locations    the parsed {@link TarArchiveBuilder.EntryLocation} list (must not be null)
     */
    public void put(String blobPathStr, String blobName, List<TarArchiveBuilder.EntryLocation> locations) {
        cache.put(cacheKey(blobPathStr, blobName), locations);
    }

    /**
     * Returns the current number of entries in the cache.
     */
    public int size() {
        return cache.size();
    }

    /**
     * Clears all entries from the cache. Useful in tests.
     */
    public void clear() {
        cache.clear();
    }

    private static String cacheKey(String blobPathStr, String blobName) {
        return blobPathStr + "/" + blobName;
    }
}
