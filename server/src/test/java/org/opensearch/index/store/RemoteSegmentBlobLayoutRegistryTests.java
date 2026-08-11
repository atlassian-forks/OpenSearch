/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache License, Version 2.0 or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class RemoteSegmentBlobLayoutRegistryTests extends OpenSearchTestCase {

    public void testDefaultLayoutIsAlwaysAvailable() {
        RemoteSegmentBlobLayoutRegistry registry = new RemoteSegmentBlobLayoutRegistry(Map.of());

        assertEquals(RemoteSegmentBlobLayoutRegistry.DEFAULT_LAYOUT_NAME, registry.getFactory("default").getName());
    }

    public void testUnknownLayoutIsRejected() {
        RemoteSegmentBlobLayoutRegistry registry = new RemoteSegmentBlobLayoutRegistry(Map.of());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> registry.getFactory("tar"));
        assertEquals("Unknown remote segment blob layout [tar]", exception.getMessage());
    }
}
