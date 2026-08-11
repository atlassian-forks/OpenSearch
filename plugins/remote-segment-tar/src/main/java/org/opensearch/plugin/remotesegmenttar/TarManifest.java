/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.plugin.remotesegmenttar;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Encodes the first TAR entry that describes the logical files stored in an archive.
 *
 * <p>The binary format is {@code magic, version, entry count, then name/offset/length/checksum for each entry}. The
 * explicit version makes future format changes distinguishable from a corrupt manifest. Offsets point to file data,
 * not TAR headers, so a reader can issue an exact remote range request.
 */
final class TarManifest {

    private static final int MAGIC = 0x52425354;
    private static final int VERSION = 1;

    private TarManifest() {}

    /**
     * Serializes manifest entries in archive order.
     */
    static byte[] encode(List<Entry> entries) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(MAGIC);
            output.writeInt(VERSION);
            output.writeInt(entries.size());
            for (Entry entry : entries) {
                output.writeUTF(entry.name());
                output.writeLong(entry.offset());
                output.writeLong(entry.length());
                output.writeUTF(entry.checksum());
            }
        }
        return bytes.toByteArray();
    }

    /**
     * One logical segment file and the position of its data bytes in the archive.
     */
    record Entry(String name, long offset, long length, String checksum) {
    }
}
