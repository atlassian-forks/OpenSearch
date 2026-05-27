/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * A simple zero-dependency stream for writing tar archives in USTAR/GNU format.
 * Supports sizes >= 8GB using base-256 encoding.
 */
public final class TarOutputStream extends FilterOutputStream {
    private long bytesWritten = 0;
    private long currentFileSize = 0;
    private long currentFileWritten = 0;
    private boolean inEntry = false;

    public TarOutputStream(final OutputStream out) {
        super(out);
    }

    public static byte[] buildHeader(final String name, final long size) {
        final byte[] header = new byte[512];

        // Name (offset 0, length 100)
        final byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > 99) {
            throw new IllegalArgumentException("Filename too long: " + name);
        }
        System.arraycopy(nameBytes, 0, header, 0, nameBytes.length);

        // Mode (offset 100, length 8) -> "0000644\0"
        writeOctal(header, 100, 8, 0644);

        // UID (offset 108, length 8) -> "0000000\0"
        writeOctal(header, 108, 8, 0);

        // GID (offset 116, length 8) -> "0000000\0"
        writeOctal(header, 116, 8, 0);

        // Size (offset 124, length 12)
        writeSize(header, 124, 12, size);

        // Mtime (offset 136, length 12)
        writeOctal(header, 136, 12, System.currentTimeMillis() / 1000L);

        // Checksum field (offset 148, length 8) must be filled with spaces for checksum calculation
        for (int i = 148; i < 156; i++) {
            header[i] = ' ';
        }

        // Type flag (offset 156) -> '0' for normal file
        header[156] = '0';

        // Magic "ustar\0" (offset 257, length 6)
        final byte[] magic = "ustar".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(magic, 0, header, 257, magic.length);
        header[262] = 0;

        // Version "00" (offset 263, length 2)
        header[263] = '0';
        header[264] = '0';

        // Calculate checksum
        final long checksum = calculateChecksum(header);
        writeOctal(header, 148, 8, checksum);

        return header;
    }

    public void putNextEntry(final String name, final long size) throws IOException {
        if (inEntry) {
            closeEntry();
        }
        final byte[] header = buildHeader(name, size);
        out.write(header);
        bytesWritten += 512;
        currentFileSize = size;
        currentFileWritten = 0;
        inEntry = true;
    }

    static void writeSize(final byte[] buffer, final int offset, final int length, final long value) {
        if (value < 8589934592L) { // 8 GiB
            writeOctal(buffer, offset, length, value);
        } else {
            // Base-256 encoding (high bit set of first byte)
            buffer[offset] = (byte) 0x80;
            // Clear remaining bytes
            for (int i = 1; i < length; i++) {
                buffer[offset + i] = 0;
            }
            // Write 8-byte big-endian long value into the end of the field
            long temp = value;
            for (int i = length - 1; i >= 1; i--) {
                buffer[offset + i] = (byte) (temp & 0xFF);
                temp = temp >>> 8;
            }
        }
    }

    private static void writeOctal(final byte[] buffer, final int offset, final int length, final long value) {
        int idx = length - 1;
        buffer[offset + idx] = 0; // null terminator
        idx--;
        if (value == 0) {
            buffer[offset + idx] = '0';
            idx--;
        } else {
            long temp = value;
            while (idx >= 0 && temp > 0) {
                buffer[offset + idx] = (byte) ('0' + (temp & 7));
                temp = temp >> 3;
                idx--;
            }
        }
        while (idx >= 0) {
            buffer[offset + idx] = '0';
            idx--;
        }
    }

    private static long calculateChecksum(final byte[] header) {
        long sum = 0;
        for (final byte b : header) {
            sum += (b & 0xFF);
        }
        return sum;
    }

    public void closeEntry() throws IOException {
        if (!inEntry) {
            return;
        }
        if (currentFileWritten < currentFileSize) {
            throw new IOException(
                "Entry was closed before all data was written. Expected size: " + currentFileSize + ", written: " + currentFileWritten
            );
        }
        final long remainder = currentFileSize % 512;
        if (remainder > 0) {
            final long padding = 512 - remainder;
            final byte[] padBytes = new byte[(int) padding];
            out.write(padBytes);
            bytesWritten += padding;
        }
        inEntry = false;
    }

    @Override
    public void write(final int b) throws IOException {
        if (!inEntry) {
            throw new IOException("No current tar entry");
        }
        if (currentFileWritten >= currentFileSize) {
            throw new IOException("Attempt to write more than the declared size of entry");
        }
        out.write(b);
        bytesWritten++;
        currentFileWritten++;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (!inEntry) {
            throw new IOException("No current tar entry");
        }
        if (currentFileWritten + len > currentFileSize) {
            throw new IOException("Attempt to write more than the declared size of entry");
        }
        out.write(b, off, len);
        bytesWritten += len;
        currentFileWritten += len;
    }

    public void finish() throws IOException {
        if (inEntry) {
            closeEntry();
        }
        // Write two blocks of zeros
        final byte[] endBlocks = new byte[1024];
        out.write(endBlocks);
        bytesWritten += 1024;
        out.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            finish();
        } finally {
            super.close();
        }
    }
}
