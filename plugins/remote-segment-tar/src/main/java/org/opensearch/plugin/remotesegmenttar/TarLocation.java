/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import java.io.IOException;

/**
 * Parses and emits the opaque location persisted by core for one logical file in a TAR archive.
 *
 * <p>The format is {@code tar:v1:<archive>#<data-offset>:<logical-length>}. Core treats this as an opaque string;
 * the plugin validates it before translating logical reads or garbage collection into physical archive operations.
 */
final class TarLocation {

    private static final String PREFIX = "tar:v1:";

    private final String archive;
    private final long offset;
    private final long length;

    TarLocation(String archive, long offset, long length) {
        this.archive = archive;
        this.offset = offset;
        this.length = length;
    }

    /**
     * Validates the version, delimiters, and non-negative numeric fields in a persisted location.
     */
    static TarLocation parse(String location) throws IOException {
        if (location.startsWith(PREFIX) == false) {
            throw new IOException("Invalid TAR remote segment location [" + location + "]");
        }
        int marker = location.indexOf('#', PREFIX.length());
        int separator = location.indexOf(':', marker + 1);
        if (marker <= PREFIX.length() || separator <= marker + 1 || separator == location.length() - 1) {
            throw new IOException("Invalid TAR remote segment location [" + location + "]");
        }
        try {
            long offset = Long.parseLong(location.substring(marker + 1, separator));
            long length = Long.parseLong(location.substring(separator + 1));
            if (offset < 0 || length < 0) {
                throw new NumberFormatException("negative offset or length");
            }
            return new TarLocation(location.substring(PREFIX.length(), marker), offset, length);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid TAR remote segment location [" + location + "]", exception);
        }
    }

    String archive() {
        return archive;
    }

    long offset() {
        return offset;
    }

    long length() {
        return length;
    }

    /**
     * Returns the stable value stored in uploaded segment metadata.
     */
    String encode() {
        return PREFIX + archive + "#" + offset + ":" + length;
    }
}
