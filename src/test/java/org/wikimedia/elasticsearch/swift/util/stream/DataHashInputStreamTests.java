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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class DataHashInputStreamTests extends LuceneTestCase {
    private DataHashInputStream stream;
    private static byte[] data;
    private static String hash;

    @BeforeClass
    public static void beforeClass() {
        data = new byte[128];
        Randomness.get().nextBytes(data);
        hash = DigestUtils.md5Hex(data);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        stream = new DataHashInputStream("test", new ByteArrayInputStream(data), hash);
    }

    public void testRead_WhenHashIsCorrect_Succeeds() throws IOException {
        byte[] compare = new byte[data.length];
        int offset = 0;
        int read = 0;

        while (read != -1){
            offset += read;
            read = stream.read(compare, offset, compare.length - offset);
        }

        assertEquals(data.length, offset);
        assertArrayEquals(data, compare);
    }

    public void testRead_WhenHashIsWrong_Throws() throws IOException {
        String hash = DigestUtils.md5Hex("");
        stream = new DataHashInputStream("fail", new ByteArrayInputStream(data), hash);
        byte[] compare = new byte[data.length];
        int read = 0;

        while (read != -1){
            try {
                read = stream.read(compare);
            }
            catch (BlobStoreException ex){
                return;
            }
        }

        Assert.fail("Expected IOException");
    }
}
