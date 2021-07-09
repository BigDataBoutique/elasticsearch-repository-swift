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
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;

public class JossInputStreamTests extends LuceneTestCase {
    private InputStream mockStream;
    private JossInputStream stream;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        mockStream = mock(InputStream.class);
        when(mockStream.read()).thenReturn(-1);
        when(mockStream.read(any(byte[].class))).thenReturn(-1);
        when(mockStream.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);

        stream = new JossInputStream(mockStream);
    }

    public void testEofClosesInnerStream() throws IOException {
        int read = stream.read();

        assertEquals(-1, read);
        verify(mockStream).close();
    }

    public void testReadFullByteArrayCallsBaseClassRead() throws IOException {
        byte[] data = new byte[8];

        stream.read(data, 0, data.length);

        verify(mockStream).read(any(byte[].class));
    }

    public void testReadByteArrayWithOffsetReadsPartialArray() throws IOException {
        byte[] data = new byte[16];

        stream.read(data, 8, data.length - 8);

        ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
        verify(mockStream, times(0)).read(any(byte[].class), anyInt(), anyInt());
        verify(mockStream).read(captor.capture());
        assertEquals(data.length - 8, captor.getValue().length);
    }
}
