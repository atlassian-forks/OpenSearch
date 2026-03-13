/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

/**
 * Extension of {@link IndexIOStreamHandler} that is aware of the stream version being read.
 * When used with {@link VersionedCodecStreamWrapper}, the wrapper will call {@link #setReadVersion(int)}
 * before invoking {@link #readContent} and {@link #clearReadVersion()} after.
 *
 * @param <T> Type of content to be read/written
 *
 * @opensearch.internal
 */
public interface VersionAwareIndexIOStreamHandler<T> extends IndexIOStreamHandler<T> {

    /**
     * Called by {@link VersionedCodecStreamWrapper} before {@link #readContent} with the version
     * parsed from the stream header.
     *
     * @param version the version read from the stream header
     */
    void setReadVersion(int version);

    /**
     * Called by {@link VersionedCodecStreamWrapper} after {@link #readContent} to clean up any
     * thread-local version state.
     */
    void clearReadVersion();
}
