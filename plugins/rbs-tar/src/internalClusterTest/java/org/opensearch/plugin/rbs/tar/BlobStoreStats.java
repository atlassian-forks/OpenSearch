/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import java.util.concurrent.atomic.AtomicInteger;

public final class BlobStoreStats {
    public static final AtomicInteger putCount = new AtomicInteger(0);
    public static final AtomicInteger listCount = new AtomicInteger(0);
    public static final AtomicInteger getCount = new AtomicInteger(0);
    public static final AtomicInteger deleteCount = new AtomicInteger(0);

    public static void reset() {
        putCount.set(0);
        listCount.set(0);
        getCount.set(0);
        deleteCount.set(0);
    }
}
