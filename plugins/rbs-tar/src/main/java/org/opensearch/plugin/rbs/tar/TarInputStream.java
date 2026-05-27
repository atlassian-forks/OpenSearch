/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * A simple zero-dependency stream for reading tar archives in USTAR/GNU format.
 * Supports reading file sizes >= 8GB using base-256 encoding.
 */
public final class TarInputStream extends FilterInputStream {
    private String currentName = null;
    private long currentSize = 0;
    private long currentBytesRead = 0;
    private boolean hasReachedEOF = false;

    public TarInputStream(final InputStream in) {
        super(in);
    }

    /**
     * Obtains the next entry in the tar archive.
     *
     * @return the TarEntry details, or null if EOF is reached.
     */
    public TarEntry getNextEntry() throws IOException {
        if (hasReachedEOF) {
            return null;
        }

        // If currently in an entry, consume/skip remaining bytes and padding
        if (currentName != null) {
            final long remaining = currentSize - currentBytesRead;
            final long padding = (512 - (currentSize % 512)) % 512;
            skipFully(remaining + padding);
            currentName = null;
        }

        final byte[] header = new byte[512];
        final int initialRead = in.read(header, 0, 512);
        if (initialRead == -1) {
            hasReachedEOF = true;
            return null;
        }
        if (initialRead < 512) {
            readFully(header, initialRead, 512 - initialRead);
        }

        // Check if header is empty (two consecutive blocks of zero indicate EOF)
        boolean allZeros = true;
        for (final byte b : header) {
            if (b != 0) {
                allZeros = false;
                break;
            }
        }

        if (allZeros) {
            // Read second EOF block of 512 bytes
            final byte[] secondBlock = new byte[512];
            readFully(secondBlock, 0, 512);
            hasReachedEOF = true;
            return null;
        }

        // Parse Name (offset 0, length 100)
        int nameLen = 0;
        while (nameLen < 100 && header[nameLen] != 0) {
            nameLen++;
        }
        currentName = new String(header, 0, nameLen, StandardCharsets.UTF_8);

        // Parse Size (offset 124, length 12)
        currentSize = parseNumeric(header, 124, 12);
        currentBytesRead = 0;

        return new TarEntry(currentName, currentSize);
    }

    long parseNumeric(final byte[] header, final int offset, final int length) {
        final byte leading = header[offset];
        if ((leading & 0x80) != 0) {
            // Base-256 encoding (positive if 0x80, negative if 0xFF)
            long result = 0;
            // Interpretation of base-256 big-endian format
            // If the leftmost bit is set, it's base-256.
            // In Java, bytes are signed, so check for negative value or & 0x80
            // For positive number (0x80), the remaining bits are treated as positive big-endian
            for (int i = offset + 1; i < offset + length; i++) {
                result = (result << 8) + (header[i] & 0xFF);
            }
            // If it was negative (0xFF), handle two's complement prefix
            if (leading == (byte) 0xFF) {
                // Not expected for sizes, but good practice to handle.
                // Subtract 1 from result's positive representation or build negatively.
                result = result - (1L << (8 * (length - 1)));
            }
            return result;
        } else {
            return parseOctal(header, offset, length);
        }
    }

    private long parseOctal(final byte[] header, final int offset, final int length) {
        long result = 0;
        final int end = offset + length;
        int start = offset;
        // Skip leading spaces or zeros
        while (start < end && (header[start] == ' ' || header[start] == '0')) {
            start++;
        }
        for (int i = start; i < end; i++) {
            final byte b = header[i];
            if (b == 0 || b == ' ') {
                break;
            }
            if (b < '0' || b > '7') {
                throw new IllegalArgumentException("Invalid octal digit: " + (char) b);
            }
            result = (result << 3) + (b - '0');
        }
        return result;
    }

    private void readFully(final byte[] b, final int offset, final int length) throws IOException {
        int read = 0;
        while (read < length) {
            final int r = in.read(b, offset + read, length - read);
            if (r == -1) {
                throw new IOException("Unexpected end of stream while reading tar archive");
            }
            read += r;
        }
    }

    private void skipFully(final long n) throws IOException {
        long skipped = 0;
        while (skipped < n) {
            final long s = in.skip(n - skipped);
            if (s <= 0) {
                // If skip doesn't work, read into a buffer
                final int toRead = (int) Math.min(4096, n - skipped);
                final byte[] temp = new byte[toRead];
                final int r = in.read(temp);
                if (r == -1) {
                    throw new IOException("Unexpected end of stream while skipping tar padding");
                }
                skipped += r;
            } else {
                skipped += s;
            }
        }
    }

    @Override
    public int read() throws IOException {
        if (currentName == null || currentBytesRead >= currentSize) {
            return -1;
        }
        final int b = in.read();
        if (b != -1) {
            currentBytesRead++;
        }
        return b;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (currentName == null || currentBytesRead >= currentSize) {
            return -1;
        }
        final int toRead = (int) Math.min(len, currentSize - currentBytesRead);
        final int r = in.read(b, off, toRead);
        if (r != -1) {
            currentBytesRead += r;
        }
        return r;
    }

    /**
     * Immutable representing a single entry in a tar archive.
     */
    public static final class TarEntry {
        private final String name;
        private final long size;

        public TarEntry(final String name, final long size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public long getSize() {
            return size;
        }
    }
}
