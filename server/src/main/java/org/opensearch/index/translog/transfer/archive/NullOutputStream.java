/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer.archive;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that discards all bytes. Used to compute ZIP size without buffering.
 *
 * @opensearch.internal
 */
final class NullOutputStream extends OutputStream {

    @Override
    public void write(int b) throws IOException {
        // discard
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        // discard
    }
}
