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
package org.wikimedia.elasticsearch.swift.util.blob;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.Randomness;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SavedBlobFactoryTests extends LuceneTestCase {
    private InputStream input;
    private SavedBlob.Factory factory;
    private static final String sample = "sample";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        input = new ByteArrayInputStream(sample.getBytes(StandardCharsets.US_ASCII));
        factory = new SavedBlob.Factory("/tmp");
    }

    public void testFactoryCreateReadsInputStreamFully() throws IOException {
        try (SavedBlob blob = factory.create(String.valueOf(Randomness.get().nextLong()),
                String.valueOf(Randomness.get().nextLong()),
                input);
             InputStream in = blob.getReentrantStream()){

            byte[] bytestr = new byte[sample.length()];
            in.read(bytestr);
            assertEquals(sample, new String(bytestr, StandardCharsets.US_ASCII));
        }
    }

    public void testFactoryCreateCombinesPathComponents() throws IOException {
        final String one = String.valueOf(Randomness.get().nextLong());
        final String two = String.valueOf(Randomness.get().nextLong());
        try (SavedBlob blob = factory.create(one, two, input)){
            assertEquals("/tmp/"+one+"/"+two, blob.getPath().toString());
        }
    }
}
