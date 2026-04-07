/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.segment.archive;

import java.io.OutputStream;

/**
 * An {@link OutputStream} that discards all bytes written to it.
 * Used for dry-run ZIP size computation without buffering.
 */
final class NullOutputStream extends OutputStream {

    static final NullOutputStream INSTANCE = new NullOutputStream();

    private NullOutputStream() {}

    @Override
    public void write(int b) {}

    @Override
    public void write(byte[] b, int off, int len) {}
}
