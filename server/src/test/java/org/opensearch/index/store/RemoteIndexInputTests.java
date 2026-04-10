/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteIndexInputTests extends OpenSearchTestCase {

    private static final String FILENAME = "segment_1";
    private static final long FILESIZE = 200;

    private InputStream inputStream;
    private RemoteIndexInput remoteIndexInput;

    @Before
    public void setup() {
        inputStream = mock(InputStream.class);
        remoteIndexInput = new RemoteIndexInput(FILENAME, inputStream, FILESIZE);
    }

    public void testReadByte() throws IOException {
        InputStream inputStream = spy(InputStream.class);
        remoteIndexInput = new RemoteIndexInput(FILENAME, inputStream, FILESIZE);

        when(inputStream.read()).thenReturn(10);

        assertEquals(10, remoteIndexInput.readByte());
        assertEquals(1, remoteIndexInput.getFilePointer());

        verify(inputStream).read(any());
    }

    public void testReadByteIOException() throws IOException {
        when(inputStream.read(any())).thenThrow(new IOException("Error reading"));

        assertThrows(IOException.class, () -> remoteIndexInput.readByte());
        assertEquals(0, remoteIndexInput.getFilePointer());
    }

    public void testReadBytes() throws IOException {
        byte[] buffer = new byte[20];
        // All 20 bytes read in one call
        when(inputStream.read(eq(buffer), anyInt(), anyInt())).thenReturn(20);
        remoteIndexInput.readBytes(buffer, 0, 20);

        verify(inputStream).read(buffer, 0, 20);
        assertEquals(20, remoteIndexInput.getFilePointer());
    }

    public void testReadBytesMultipleIterations() throws IOException {
        byte[] buffer = new byte[20];
        // 10 + 5 + 5 = 20 bytes across three reads
        when(inputStream.read(eq(buffer), anyInt(), anyInt())).thenReturn(10).thenReturn(5).thenReturn(5);
        remoteIndexInput.readBytes(buffer, 0, 20);

        verify(inputStream).read(buffer, 0, 20);
        verify(inputStream).read(buffer, 10, 10);
        verify(inputStream).read(buffer, 15, 5);
        assertEquals(20, remoteIndexInput.getFilePointer());
    }

    /**
     * Regression test for segment replication failure:
     * "Premature end of Content-Length delimited message body (expected: 175,650; received: 8,192)"
     * Root cause: readBytes silently returned when stream hit premature EOF (-1) without reading
     * all requested bytes. copyBytes then closed the stream with unread bytes, causing
     * ConnectionClosedException in S3RetryingInputStream.close().
     * Fix: throw EOFException so S3RetryingInputStream.read() retry logic fires.
     */
    public void testReadBytesPrematureEOFThrowsEOFException() throws IOException {
        byte[] buffer = new byte[20];
        // Stream returns only 8 bytes then EOF — simulates truncated S3 response
        when(inputStream.read(eq(buffer), anyInt(), anyInt())).thenReturn(8).thenReturn(-1);

        EOFException ex = assertThrows(EOFException.class, () -> remoteIndexInput.readBytes(buffer, 0, 20));
        assertTrue(ex.getMessage().contains("Premature EOF"));
        // filePointer should reflect partial read
        assertEquals(8, remoteIndexInput.getFilePointer());
    }

    public void testReadBytesImmediateEOFThrowsEOFException() throws IOException {
        byte[] buffer = new byte[20];
        // Stream immediately returns EOF
        when(inputStream.read(eq(buffer), anyInt(), anyInt())).thenReturn(-1);

        EOFException ex = assertThrows(EOFException.class, () -> remoteIndexInput.readBytes(buffer, 0, 20));
        assertTrue(ex.getMessage().contains("Premature EOF"));
        assertEquals(0, remoteIndexInput.getFilePointer());
    }

    public void testReadBytesIOException() throws IOException {
        byte[] buffer = new byte[10];
        when(inputStream.read(buffer, 10, 20)).thenThrow(new IOException("Error reading"));

        assertThrows(IOException.class, () -> remoteIndexInput.readBytes(buffer, 10, 20));
    }

    public void testClose() throws IOException {
        remoteIndexInput.close();

        verify(inputStream).close();
    }

    public void testCloseIOException() throws IOException {
        doThrow(new IOException("Error closing")).when(inputStream).close();

        assertThrows(IOException.class, () -> remoteIndexInput.close());
    }

    public void testLength() {
        assertEquals(FILESIZE, remoteIndexInput.length());
    }

    public void testSeek() {
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexInput.seek(100L));
    }

    public void testGetFilePointer() throws IOException {
        when(inputStream.read(any(), eq(0), eq(8))).thenReturn(8);
        remoteIndexInput.readBytes(new byte[8], 0, 8);
        assertEquals(8, remoteIndexInput.getFilePointer());
    }

    public void testSlice() {
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexInput.slice("Slice middle", 50, 100));
    }
}
