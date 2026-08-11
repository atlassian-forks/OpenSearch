/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Minimal TAR encoder for immutable remote segment archives.
 *
 * <p>The layout needs stable entry positions, so it writes headers and padding directly rather than using a general
 * archive library. Each entry occupies one 512-byte header, its data bytes, then zero padding to the next 512-byte
 * boundary. The end marker is two zero-filled blocks, as required by the TAR format.
 */
final class TarArchive {

    static final int HEADER_LENGTH = 512;
    private static final int END_LENGTH = 1024;

    private TarArchive() {}

    /**
     * Returns the complete space occupied by one entry, including its header and alignment padding.
     */
    static long entryLength(long length) {
        return HEADER_LENGTH + length + (HEADER_LENGTH - length % HEADER_LENGTH) % HEADER_LENGTH;
    }

    /**
     * Writes an in-memory control entry. The manifest is the only current caller.
     */
    static void writeEntry(IndexOutput output, String name, byte[] content) throws IOException {
        output.writeBytes(header(name, content.length), HEADER_LENGTH);
        output.writeBytes(content, content.length);
        writePadding(output, content.length);
    }

    /**
     * Streams one Lucene file into the archive using a bounded buffer.
     *
     * <p>The file length is determined before its header is written. This is required by TAR and keeps the previously
     * computed manifest offsets valid.
     */
    static void writeEntry(IndexOutput output, String name, Directory directory, String file, int bufferSize) throws IOException {
        long length = directory.fileLength(file);
        output.writeBytes(header(name, length), HEADER_LENGTH);
        byte[] buffer = new byte[bufferSize];
        try (IndexInput input = directory.openInput(file, IOContext.READONCE)) {
            long remaining = length;
            while (remaining > 0) {
                int bytesToRead = (int) Math.min(buffer.length, remaining);
                input.readBytes(buffer, 0, bytesToRead);
                output.writeBytes(buffer, bytesToRead);
                remaining -= bytesToRead;
            }
        }
        writePadding(output, length);
    }

    /**
     * Writes the two zero blocks that terminate a standard TAR archive.
     */
    static void finish(IndexOutput output) throws IOException {
        output.writeBytes(new byte[END_LENGTH], END_LENGTH);
    }

    /**
     * Builds the fixed USTAR header. Timestamps are zero so an identical batch yields stable archive bytes.
     */
    private static byte[] header(String name, long length) {
        byte[] header = new byte[HEADER_LENGTH];
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        if (nameBytes.length > 99) {
            throw new IllegalArgumentException("TAR entry name is too long [" + name + "]");
        }
        System.arraycopy(nameBytes, 0, header, 0, nameBytes.length);
        writeOctal(header, 100, 8, 0644);
        writeOctal(header, 108, 8, 0);
        writeOctal(header, 116, 8, 0);
        writeOctal(header, 124, 12, length);
        writeOctal(header, 136, 12, 0);
        // TAR checksums treat the checksum field itself as spaces.
        for (int index = 148; index < 156; index++) {
            header[index] = ' ';
        }
        header[156] = '0';
        byte[] magic = "ustar".getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(magic, 0, header, 257, magic.length);
        header[262] = 0;
        header[263] = '0';
        header[264] = '0';
        long checksum = 0;
        for (byte value : header) {
            checksum += value & 0xFF;
        }
        writeOctal(header, 148, 8, checksum);
        return header;
    }

    private static void writePadding(IndexOutput output, long length) throws IOException {
        int padding = (int) ((HEADER_LENGTH - length % HEADER_LENGTH) % HEADER_LENGTH);
        if (padding > 0) {
            output.writeBytes(new byte[padding], padding);
        }
    }

    private static void writeOctal(byte[] destination, int offset, int length, long value) {
        int position = length - 1;
        destination[offset + position--] = 0;
        do {
            destination[offset + position--] = (byte) ('0' + (value & 7));
            value >>>= 3;
        } while (value > 0 && position >= 0);
        while (position >= 0) {
            destination[offset + position--] = '0';
        }
    }
}
