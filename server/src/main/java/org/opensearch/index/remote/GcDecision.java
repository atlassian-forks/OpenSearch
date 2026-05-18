/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.Set;

/**
 * Explicit decision type returned by remote store GC strategy methods.
 * <p>
 * Using a structured type instead of a nullable return value avoids ambiguity
 * between "not implemented", "delete nothing", and "let core decide":
 * <ul>
 *   <li>{@link Kind#USE_DEFAULT} — core performs its normal LIST + delete logic.</li>
 *   <li>{@link Kind#SKIP} — skip GC entirely; plugin manages cleanup via its own background task.</li>
 *   <li>{@link Kind#DELETE_BLOBS} — delete exactly the specified blobs and nothing else.</li>
 * </ul>
 *
 * <p>Example usage at call site:
 * <pre>
 *   GcDecision decision = strategy.resolveStaleBlobs(activeFiles, latestGen);
 *   switch (decision.kind()) {
 *       case USE_DEFAULT:  defaultDeleteStaleSegments(activeFiles); break;
 *       case SKIP:         break; // no-op — plugin self-manages
 *       case DELETE_BLOBS: remoteDirectory.deleteBlobs(decision.blobNames()); break;
 *   }
 * </pre>
 *
 * @opensearch.internal
 */
@ExperimentalApi
public final class GcDecision {

    /** Discriminant for the type of GC decision. */
    @ExperimentalApi
    public enum Kind {
        /** Core performs its normal LIST + delete logic. */
        USE_DEFAULT,
        /** Skip per-shard GC; plugin manages cleanup via background task. */
        SKIP,
        /** Delete exactly the specified set of blobs. */
        DELETE_BLOBS
    }

    /** Convenience constant: use built-in GC. */
    public static final GcDecision USE_DEFAULT = new GcDecision(Kind.USE_DEFAULT, Collections.emptySet());

    /** Convenience constant: skip GC (plugin self-manages). */
    public static final GcDecision SKIP = new GcDecision(Kind.SKIP, Collections.emptySet());

    /** Convenience factory: delete exactly these blobs. */
    public static GcDecision deleteBlobs(Set<String> blobNames) {
        return new GcDecision(Kind.DELETE_BLOBS, Collections.unmodifiableSet(blobNames));
    }

    private final Kind kind;
    private final Set<String> blobNames;

    private GcDecision(Kind kind, Set<String> blobNames) {
        this.kind = kind;
        this.blobNames = blobNames;
    }

    /** Returns the kind of this decision. */
    public Kind kind() {
        return kind;
    }

    /**
     * Returns the blob names to delete when {@link #kind()} is {@link Kind#DELETE_BLOBS}.
     * Returns an empty set for other kinds.
     */
    public Set<String> blobNames() {
        return blobNames;
    }

    @Override
    public String toString() {
        return "GcDecision{kind=" + kind + (kind == Kind.DELETE_BLOBS ? ", blobs=" + blobNames : "") + "}";
    }
}
