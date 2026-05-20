/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.archive;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Thread leak filter for the translog archive batch timer threads.
 * <p>
 * {@code TranslogBatchCoordinator} spawns one daemon timer thread per node named
 * {@code "translog-archive-batch-timer-{nodeName}"}. These are stopped via
 * {@code TranslogBatchCoordinator.close()} → {@code TranslogBatchCollector.doStop()}.
 * <p>
 * Test cluster teardown may not call {@code removeIndex()} before the randomized-testing
 * framework checks for thread leaks, so we filter out these known daemon threads to prevent
 * false positives in integration tests.
 */
public class TranslogArchiveTimerThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("translog-archive-batch-timer-");
    }
}
