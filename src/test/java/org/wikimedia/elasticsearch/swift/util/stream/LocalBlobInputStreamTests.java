/*
 * Copyright 2017 Wikimedia and BigData Boutique
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wikimedia.elasticsearch.swift.util.stream;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.PathUtils;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class LocalBlobInputStreamTests extends LuceneTestCase {
    private Path tmpFile;
    private static final String sampleData = "0123456789";
    private LocalBlobInputStream stream;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        tmpFile = PathUtils.get("stream.test");
        Files.copy(new ByteArrayInputStream(sampleData.getBytes(StandardCharsets.US_ASCII)), tmpFile);
        stream = new LocalBlobInputStream(tmpFile);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        stream.close();
        Files.delete(tmpFile);
        super.tearDown();
    }

    public void testReadThrowsClosedExceptionWhenStreamIsClosed() throws IOException {
        stream.close();
        try {
            byte[] b = new byte[8];
            stream.read(b, 0 , b.length);
            fail("Expected StreamClosedException");
        }
        catch (StreamClosedException ignored){
        }
    }

    public void testResetThrowsClosedExceptionWhenStreamIsClosed() throws IOException {
        stream.mark(Integer.MAX_VALUE);
        stream.close();
        try {
            stream.reset();
            fail("Expected StreamClosedException");
        }
        catch (StreamClosedException ignored){
        }
    }

    public void testCanResetAndReadFile() throws IOException {
        byte[] buf1 = new byte[sampleData.length()];
        byte[] buf2 = new byte[sampleData.length()];

        stream.mark(Integer.MAX_VALUE);
        assertEquals(buf1.length, stream.read(buf1));

        stream.reset();
        assertEquals(buf2.length, stream.read(buf2));
        assertArrayEquals(buf1, buf2);
    }

    public void testReadChunks() throws IOException {
        byte[] buf = new byte[sampleData.length()*4];
        int off = 0, read = 0;

        while(read != -1){
            off += read;
            read = stream.read(buf, off, sampleData.length()/2+1);
        }

        String compare = new String(buf, 0, off, StandardCharsets.US_ASCII);
        assertEquals(sampleData, compare);
    }

    public void testReadMoreThanArrayCanHold() throws IOException {
        final byte[] buf = new byte[3];
        assertTrue(buf.length < sampleData.length());

        int read = stream.read(buf, 0, sampleData.length());
        assertEquals(buf.length, read);

        String compare = new String(buf, StandardCharsets.US_ASCII);
        assertEquals(sampleData.substring(0, buf.length), compare);
    }

    public void testReadByteByByte() throws IOException {
        StringBuffer sb = new StringBuffer();
        do {
            int read = stream.read();
            if (read == -1){
                break;
            }
            sb.append((char)read);
        } while (true);

        assertEquals(sampleData, sb.toString());
    }

    public void testReadLargeBuffer() throws IOException {
        final int largeBufferSize = 48*1024; // ES defines CHUNK_SIZE as 16K
        final byte[] source = new byte[largeBufferSize];
        Randomness.get().nextBytes(source);
        Files.copy(new ByteArrayInputStream(source), tmpFile, StandardCopyOption.REPLACE_EXISTING);
        stream.close();
        stream = new LocalBlobInputStream(tmpFile);

        final byte[] buf = new byte[largeBufferSize/3*2];
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(largeBufferSize);
        int read = 0;

        while (read != -1) {
            baos.write(buf, 0, read);
            read = stream.read(buf);
        }

        assertEquals(source.length, baos.size());
        assertArrayEquals(source, baos.toByteArray());
    }
}
