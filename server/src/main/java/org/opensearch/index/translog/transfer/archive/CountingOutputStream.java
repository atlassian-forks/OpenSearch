/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that counts bytes written. Used to track offsets when building ZIP with comment index.
 *
 * @opensearch.internal
 */
final class CountingOutputStream extends FilterOutputStream {

    private long count = 0;

    CountingOutputStream(OutputStream out) {
        super(out);
    }

    long getCount() {
        return count;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        count += len;
    }
}
