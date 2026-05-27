/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Unit tests for TarOutputStream and TarInputStream.
 */
public class TarStreamTests extends OpenSearchTestCase {

    public void testRoundTrip() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TarOutputStream tos = new TarOutputStream(baos);

        byte[] content1 = "Hello, world!".getBytes(StandardCharsets.UTF_8);
        byte[] content2 = new byte[1024]; // exactly two 512-byte blocks
        new Random(42).nextBytes(content2);
        byte[] content3 = new byte[700]; // not a multiple of 512
        new Random(43).nextBytes(content3);
        byte[] content4 = new byte[0]; // empty file

        tos.putNextEntry("file1.txt", content1.length);
        tos.write(content1);
        tos.closeEntry();

        tos.putNextEntry("dir/file2.bin", content2.length);
        tos.write(content2);
        tos.closeEntry();

        tos.putNextEntry("file3.data", content3.length);
        tos.write(content3);
        tos.closeEntry();

        tos.putNextEntry("empty.txt", content4.length);
        tos.write(content4);
        tos.closeEntry();

        tos.finish();
        tos.close();

        byte[] tarBytes = baos.toByteArray();

        // Now read using TarInputStream
        ByteArrayInputStream bais = new ByteArrayInputStream(tarBytes);
        TarInputStream tis = new TarInputStream(bais);

        TarInputStream.TarEntry entry = tis.getNextEntry();
        assertNotNull(entry);
        assertEquals("file1.txt", entry.getName());
        assertEquals(content1.length, entry.getSize());
        byte[] readContent1 = tis.readAllBytes();
        assertArrayEquals(content1, readContent1);

        entry = tis.getNextEntry();
        assertNotNull(entry);
        assertEquals("dir/file2.bin", entry.getName());
        assertEquals(content2.length, entry.getSize());
        byte[] readContent2 = tis.readAllBytes();
        assertArrayEquals(content2, readContent2);

        entry = tis.getNextEntry();
        assertNotNull(entry);
        assertEquals("file3.data", entry.getName());
        assertEquals(content3.length, entry.getSize());
        byte[] readContent3 = tis.readAllBytes();
        assertArrayEquals(content3, readContent3);

        entry = tis.getNextEntry();
        assertNotNull(entry);
        assertEquals("empty.txt", entry.getName());
        assertEquals(content4.length, entry.getSize());
        byte[] readContent4 = tis.readAllBytes();
        assertArrayEquals(content4, readContent4);

        assertNull(tis.getNextEntry());
        tis.close();
    }

    public void testBase256LargeSizes() throws IOException {
        long[] largeSizes = {
            8589934592L,          // exactly 8 GB
            10737418240L,         // 10 GB
            1099511627776L,       // 1 TB
            9223372036854775807L  // Long.MAX_VALUE
        };

        TarInputStream tis = new TarInputStream(new ByteArrayInputStream(new byte[0]));

        for (long size : largeSizes) {
            byte[] header = new byte[512];
            TarOutputStream.writeSize(header, 124, 12, size);

            // Check that the leftmost byte has the 0x80 bit set
            assertEquals((byte) 0x80, header[124]);

            // Parse back the size and verify
            long parsedSize = tis.parseNumeric(header, 124, 12);
            assertEquals(size, parsedSize);
        }
    }
}
