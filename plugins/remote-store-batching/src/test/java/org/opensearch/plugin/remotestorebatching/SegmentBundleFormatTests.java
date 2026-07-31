/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class SegmentBundleFormatTests extends OpenSearchTestCase {

    private static SegmentBundleFormat.NamedContent content(String name, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return new SegmentBundleFormat.NamedContent() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public byte[] getBytes() {
                return bytes;
            }
        };
    }

    public void testRoundTripSingleFile() throws IOException {
        byte[] bundle = SegmentBundleFormat.build(List.of(content("_0.si", "hello segment")));
        SegmentBundleFormat.HeaderProbe probe = SegmentBundleFormat.probeHeader(bundle);
        assertNotNull(probe.entries);
        SegmentBundleFormat.Entry entry = probe.entries.get("_0.si");
        assertNotNull(entry);
        byte[] extracted = new byte[(int) entry.length];
        System.arraycopy(bundle, (int) entry.offset, extracted, 0, extracted.length);
        assertEquals("hello segment", new String(extracted, StandardCharsets.UTF_8));
    }

    public void testRoundTripMultipleFilesPreservesEachFileExactly() throws IOException {
        Map<String, String> files = Map.of(
            "_0.si",
            "segment info bytes",
            "_0.cfe",
            "compound file entries table, a bit longer than the others",
            "_0.cfs",
            "compound file data"
        );
        List<SegmentBundleFormat.NamedContent> contents = files.entrySet()
            .stream()
            .map(e -> content(e.getKey(), e.getValue()))
            .collect(java.util.stream.Collectors.toList());

        byte[] bundle = SegmentBundleFormat.build(contents);
        SegmentBundleFormat.HeaderProbe probe = SegmentBundleFormat.probeHeader(bundle);
        assertEquals(files.size(), probe.entries.size());

        for (Map.Entry<String, String> expected : files.entrySet()) {
            SegmentBundleFormat.Entry entry = probe.entries.get(expected.getKey());
            assertNotNull("missing entry for " + expected.getKey(), entry);
            byte[] extracted = new byte[(int) entry.length];
            System.arraycopy(bundle, (int) entry.offset, extracted, 0, extracted.length);
            assertEquals(expected.getValue(), new String(extracted, StandardCharsets.UTF_8));
        }
    }

    public void testProbeHeaderReportsRequiredBytesOnPartialHead() throws IOException {
        byte[] bundle = SegmentBundleFormat.build(List.of(content("_0.si", "x".repeat(5000))));
        // Only hand over the first 4 bytes (just the headerLength field) — nowhere near enough for the full header.
        byte[] tinyHead = new byte[4];
        System.arraycopy(bundle, 0, tinyHead, 0, 4);
        SegmentBundleFormat.HeaderProbe probe = SegmentBundleFormat.probeHeader(tinyHead);
        assertNull("must report incomplete when head is too short", probe.entries);
        assertTrue(probe.requiredBytes > 4);

        byte[] fullHead = new byte[probe.requiredBytes];
        System.arraycopy(bundle, 0, fullHead, 0, probe.requiredBytes);
        SegmentBundleFormat.HeaderProbe fullProbe = SegmentBundleFormat.probeHeader(fullHead);
        assertNotNull(fullProbe.entries);
        assertEquals(1, fullProbe.entries.size());
    }

    public void testPointerEncodeDecodeRoundTrip() {
        String pointer = SegmentBundleFormat.encodePointer("segment_bundle_abc123", 4096L, 987654321L);
        assertTrue(SegmentBundleFormat.isBundlePointer(pointer));
        assertEquals("segment_bundle_abc123", SegmentBundleFormat.pointerBlobName(pointer));
        assertEquals(4096L, SegmentBundleFormat.pointerOffset(pointer));
        assertEquals(987654321L, SegmentBundleFormat.pointerLength(pointer));
    }

    public void testPlainBlobNameIsNotMistakenForAPointer() {
        assertFalse(SegmentBundleFormat.isBundlePointer("_0.si__gX7bNIIBrs0AUNsR2yEG"));
        assertFalse(SegmentBundleFormat.isBundlePointer(null));
    }
}
