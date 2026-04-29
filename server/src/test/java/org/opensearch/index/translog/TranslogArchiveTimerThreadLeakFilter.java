/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Thread leak filter for the translog archive batch timer threads.
 * <p>
 * {@link TranslogArchiveBatchCoordinator} spawns a daemon timer thread per index to signal
 * the fixed-schedule batch window. These threads are stopped via
 * {@link TranslogArchiveBatchCoordinator#close()} which is called from
 * {@code IndicesService.removeIndex()}. However, test cluster teardown may not call
 * {@code removeIndex()} before the test framework checks for thread leaks, so we filter
 * out these known daemon threads to prevent false positives.
 */
public class TranslogArchiveTimerThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("translog-archive-batch-timer-");
    }
}
