/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.atlassian.opensearch.rbs.archive;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * An {@link IndexInput} backed by an in-memory byte array.
 * Used by {@link TarSegmentRemoteStoreStrategy} to wrap the bytes extracted from a TAR archive.
 */
class ByteArrayIndexInput extends IndexInput {

    private final byte[] bytes;
    private final int offset;
    private final int length;
    private int position;

    ByteArrayIndexInput(String resourceDescription, byte[] bytes) {
        this(resourceDescription, bytes, 0, bytes.length);
    }

    private ByteArrayIndexInput(String resourceDescription, byte[] bytes, int offset, int length) {
        super(resourceDescription);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.position = 0;
    }

    @Override
    public void close() {
        // nothing to close — in-memory
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0 || pos > length) {
            throw new IOException("seek position " + pos + " out of range [0, " + length + "]");
        }
        position = (int) pos;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long sliceOffset, long sliceLength) throws IOException {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > length) {
            throw new IOException(
                "slice ["
                    + sliceOffset
                    + ", "
                    + (sliceOffset + sliceLength)
                    + ") out of bounds for length="
                    + length
            );
        }
        return new ByteArrayIndexInput(sliceDescription, bytes, (int) (offset + sliceOffset), (int) sliceLength);
    }

    @Override
    public byte readByte() throws IOException {
        if (position >= length) {
            throw new IOException("read past EOF at position " + position + " (length=" + length + ")");
        }
        return bytes[offset + position++];
    }

    @Override
    public void readBytes(byte[] b, int off, int len) throws IOException {
        if (position + len > length) {
            throw new IOException(
                "read past EOF: position=" + position + " len=" + len + " file_length=" + length
            );
        }
        System.arraycopy(bytes, offset + position, b, off, len);
        position += len;
    }
}
