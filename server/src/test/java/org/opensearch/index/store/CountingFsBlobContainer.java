/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FsBlobContainer} subclass that counts LIST, GET, PUT, DELETE operations.
 * Optionally captures stack traces for LIST calls to identify call origins.
 *
 * <p>Uses global static counters for easy access in tests. Call {@link #resetCounters()} before
 * each measurement window.</p>
 */
public class CountingFsBlobContainer extends FsBlobContainer {

    /** Global counters — shared across all container instances. */
    public static final AtomicLong listCount = new AtomicLong();
    public static final AtomicLong getCount = new AtomicLong();
    public static final AtomicLong putCount = new AtomicLong();
    public static final AtomicLong deleteCount = new AtomicLong();

    /** When true, captures caller stack traces for every LIST call. */
    public static volatile boolean captureListStacks = false;

    /** Collected stack traces from LIST calls (bounded to most recent 200). */
    public static final ConcurrentLinkedDeque<String> listStackTraces = new ConcurrentLinkedDeque<>();

    private static final int MAX_TRACES = 200;

    public CountingFsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobStore, blobPath, path);
    }

    /** Reset all counters and stack traces. Call at the start of each measurement window. */
    public static void resetCounters() {
        listCount.set(0);
        getCount.set(0);
        putCount.set(0);
        deleteCount.set(0);
        listStackTraces.clear();
    }

    /** Human-readable summary of current counters. */
    public static String countersToString() {
        return String.format(
            "PUT=%d  GET=%d  LIST=%d  DELETE=%d",
            putCount.get(), getCount.get(), listCount.get(), deleteCount.get()
        );
    }

    // ======================== LIST operations ========================

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        recordList();
        return super.listBlobs();
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        recordList();
        return super.listBlobsByPrefix(blobNamePrefix);
    }

    // listBlobsByPrefixInSortedOrder(prefix, limit, sortOrder, listener) is a default method
    // that calls listBlobsByPrefixInSortedOrder(prefix, limit, sortOrder) — no override needed.

    // listBlobsByPrefixInSortedOrder(prefix, limit, sortOrder) is also a default in FsBlobContainer
    // that calls listBlobsByPrefix() — which we already count above. No separate override needed.

    // children() is used for path navigation — not a S3 LIST equivalent, skip counting

    // ======================== GET operations ========================

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        getCount.incrementAndGet();
        return super.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        getCount.incrementAndGet();
        return super.readBlob(blobName, position, length);
    }

    // ======================== PUT operations ========================

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        putCount.incrementAndGet();
        super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        putCount.incrementAndGet();
        super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    // ======================== DELETE operations ========================

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        deleteCount.addAndGet(blobNames.size());
        super.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public DeleteResult delete() throws IOException {
        deleteCount.incrementAndGet();
        return super.delete();
    }

    // ======================== Helpers ========================

    private void recordList() {
        listCount.incrementAndGet();
        if (captureListStacks) {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            StringBuilder sb = new StringBuilder();
            sb.append("LIST from thread=").append(Thread.currentThread().getName()).append("\n");
            // Skip: getStackTrace, recordList, the calling listXxx method → start at 3
            int start = Math.min(3, stack.length);
            int end = Math.min(start + 25, stack.length);
            for (int i = start; i < end; i++) {
                sb.append("  at ").append(stack[i]).append("\n");
            }
            listStackTraces.addLast(sb.toString());
            while (listStackTraces.size() > MAX_TRACES) {
                listStackTraces.pollFirst();
            }
        }
    }
}
