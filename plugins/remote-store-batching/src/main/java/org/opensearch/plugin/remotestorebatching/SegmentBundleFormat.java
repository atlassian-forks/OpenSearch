/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.remotestorebatching;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A minimal, self-contained bundle format: N named byte blobs packed into a single blob, with a small header
 * up front describing each entry's name, offset, and length. Entirely private to this plugin: core never
 * parses this format, it only ever sees the opaque pointer string {@link #encodePointer} produces.
 * <p>
 * Layout:
 * <pre>
 *   [4 bytes: headerLength]
 *   [headerLength bytes: header]
 *     header = [4 bytes: numEntries]
 *       per entry: [2 bytes: nameLen][nameLen bytes: UTF-8 name][8 bytes: offset][8 bytes: length]
 *   [entry data, concatenated in the same order, at the offsets given above]
 * </pre>
 * Chosen over a real TAR/ZIP layout for simplicity. Swap for a standard archive format if interoperability
 * with external tooling matters.
 */
final class SegmentBundleFormat {

    /** Bytes read from the head of a bundle blob in one shot, hoping to cover header + full entry table. */
    static final int INITIAL_HEAD_READ_BYTES = 16 * 1024;

    private SegmentBundleFormat() {}

    static final class Entry {
        final String name;
        final long offset;
        final long length;

        Entry(String name, long offset, long length) {
            this.name = name;
            this.offset = offset;
            this.length = length;
        }
    }

    interface NamedContent {
        String getName();

        byte[] getBytes();
    }

    /**
     * Builds the full bundle bytes (header + concatenated content) for the given entries, in order.
     * Two passes: the header's byte length must be known before absolute data offsets can be computed.
     */
    static byte[] build(List<NamedContent> contents) throws IOException {
        ByteArrayOutputStream headerBody = new ByteArrayOutputStream();
        int headerLength = computeHeaderLength(contents);
        long offset = 4 + headerLength; // 4 bytes for headerLength field itself

        writeInt(headerBody, contents.size());
        long runningOffset = offset;
        for (NamedContent c : contents) {
            byte[] nameBytes = c.getName().getBytes(StandardCharsets.UTF_8);
            writeShort(headerBody, nameBytes.length);
            headerBody.write(nameBytes);
            writeLong(headerBody, runningOffset);
            writeLong(headerBody, c.getBytes().length);
            runningOffset += c.getBytes().length;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeInt(out, headerBody.size());
        headerBody.writeTo(out);
        for (NamedContent c : contents) {
            out.write(c.getBytes());
        }
        return out.toByteArray();
    }

    private static int computeHeaderLength(List<NamedContent> contents) {
        int len = 4; // numEntries
        for (NamedContent c : contents) {
            len += 2 + c.getName().getBytes(StandardCharsets.UTF_8).length + 8 + 8;
        }
        return len;
    }

    /**
     * Parses the entry table out of {@code head} — the first bytes of a bundle blob. {@code head} must contain
     * at least the first 4 bytes (headerLength); if it doesn't already contain the full header, callers should
     * re-read {@code 4 + headerLength} bytes and call this again.
     *
     * @return the parsed entries, or {@code null} if {@code head} did not contain the full header yet, along
     *         with the total bytes needed via {@link HeaderProbe#requiredBytes}.
     */
    static HeaderProbe probeHeader(byte[] head) {
        int headerLength = readInt(head, 0);
        int required = 4 + headerLength;
        if (head.length < required) {
            return new HeaderProbe(required, null);
        }
        Map<String, Entry> entries = new LinkedHashMap<>();
        int pos = 4;
        int numEntries = readInt(head, pos);
        pos += 4;
        for (int i = 0; i < numEntries; i++) {
            int nameLen = readShort(head, pos);
            pos += 2;
            String name = new String(head, pos, nameLen, StandardCharsets.UTF_8);
            pos += nameLen;
            long entryOffset = readLong(head, pos);
            pos += 8;
            long entryLength = readLong(head, pos);
            pos += 8;
            entries.put(name, new Entry(name, entryOffset, entryLength));
        }
        return new HeaderProbe(required, entries);
    }

    static final class HeaderProbe {
        final int requiredBytes;
        final Map<String, Entry> entries; // null if head didn't contain the full header yet

        HeaderProbe(int requiredBytes, Map<String, Entry> entries) {
            this.requiredBytes = requiredBytes;
            this.entries = entries;
        }
    }

    // ---- opaque pointer encoding, e.g. "bundle_<uuid>#12345:6789" ----

    static final String POINTER_SEPARATOR = "#";
    static final String RANGE_SEPARATOR = ":";

    static String encodePointer(String blobName, long offset, long length) {
        return blobName + POINTER_SEPARATOR + offset + RANGE_SEPARATOR + length;
    }

    /** Returns {@code true} if {@code remoteFilename} looks like a bundle pointer this class produced. */
    static boolean isBundlePointer(String remoteFilename) {
        return remoteFilename != null && remoteFilename.contains(POINTER_SEPARATOR) && remoteFilename.contains(RANGE_SEPARATOR);
    }

    static String pointerBlobName(String pointer) {
        return pointer.substring(0, pointer.indexOf(POINTER_SEPARATOR));
    }

    static long pointerOffset(String pointer) {
        String range = pointer.substring(pointer.indexOf(POINTER_SEPARATOR) + 1);
        return Long.parseLong(range.substring(0, range.indexOf(RANGE_SEPARATOR)));
    }

    static long pointerLength(String pointer) {
        String range = pointer.substring(pointer.indexOf(POINTER_SEPARATOR) + 1);
        return Long.parseLong(range.substring(range.indexOf(RANGE_SEPARATOR) + 1));
    }

    // ---- small binary helpers ----

    private static void writeInt(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeShort(OutputStream out, int v) throws IOException {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void writeLong(OutputStream out, long v) throws IOException {
        for (int i = 7; i >= 0; i--) {
            out.write((int) ((v >>> (i * 8)) & 0xFF));
        }
    }

    private static int readInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16) | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    private static int readShort(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off + 1] & 0xFF);
    }

    private static long readLong(byte[] b, int off) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[off + i] & 0xFF);
        }
        return v;
    }
}
