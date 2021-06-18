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
import org.junit.Before;

import java.io.IOException;

public class SegmentedMemoryOutputStreamTests extends LuceneTestCase {
    private SegmentedMemoryOutputStream stream;

    @Before
    public void SetUp(){
        stream = new SegmentedMemoryOutputStream();
    }

    public void testEmptyStreamHasNothingAvailableToRead() throws IOException {
        SegmentedMemoryInputStream smis = new SegmentedMemoryInputStream(stream);

        assertEquals(0, smis.available());
    }

    public void testInputStreamClosesOutputStream() throws IOException {
        byte[] data = new byte[128];
        stream.write(data);
        SegmentedMemoryInputStream smis = new SegmentedMemoryInputStream(stream);

        try {
            stream.write("abc".getBytes());
            fail("Expected IOException");
        }
        catch (IOException ignored) {
        }
    }

    public void testWriteCompletelyWritesOutData() throws IOException {
        stream = new SegmentedMemoryOutputStream(16);
        byte[] data = new byte[128];
        Randomness.get().nextBytes(data);
        byte[] compare = new byte[data.length];

        stream.write(data);
        SegmentedMemoryInputStream readStream = new SegmentedMemoryInputStream(stream);

        assertEquals(data.length, readStream.available());

        int offset = 0;
        while (offset < data.length){
            int read = readStream.read(compare, offset, data.length-offset);
            if (read == -1){
                break;
            }
            offset += read;
        }

        assertEquals(data.length, offset);
        assertArrayEquals(data, compare);
    }

    public void testWriteCanSave2Gigs() throws IOException {
        final byte[] meg = new byte[1024*1024];
        final long megCount = 1024*2+1;

        for (int i = 0; i < megCount; i++) {
            stream.write(meg);
        }

        SegmentedMemoryInputStream smis = new SegmentedMemoryInputStream(stream);
        long totalAvailable = 0;
        do {
            int available = smis.available();
            totalAvailable += available;
            smis.skip(available);
        } while (smis.available() > 0);

        assertEquals(meg.length * megCount, totalAvailable);
    }
}
