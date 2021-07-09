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
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

@LuceneTestCase.SuppressSysoutChecks(bugUrl = "")
public class SavedBlobTests extends LuceneTestCase {
    private SavedBlob blob;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        blob = new SavedBlob.Factory("/tmp")
            .create(String.valueOf(Randomness.get().nextLong()),
                    String.valueOf(Randomness.get().nextLong()),
                    new ByteArrayInputStream("foo".getBytes(StandardCharsets.US_ASCII)));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        blob.close();
        super.tearDown();
    }

    public void testCloseIsIdempotent() {
        blob.close();
        blob.close();
    }

    public void testCloseRemovesFile() {
        assertTrue(Files.exists(blob.getPath()));
        blob.close();
        assertFalse(Files.exists(blob.getPath()));
    }


    public void testGetReentrantStreamSupportsMark() throws IOException {
        try (InputStream in = blob.getReentrantStream()) {
            assertTrue(in.markSupported());
        }
    }
}
