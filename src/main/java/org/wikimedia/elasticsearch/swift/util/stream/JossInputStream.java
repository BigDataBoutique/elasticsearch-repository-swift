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

import org.apache.commons.io.input.AutoCloseInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wrap input stream passed from JOSS and address peculiarities of read implementation
 * and instant closing on EOF and IO exception to conserve resources
 */
public class JossInputStream extends AutoCloseInputStream {
    public JossInputStream(InputStream jossStream) {
        super(jossStream);
    }

    @Override
    protected void handleIOException(IOException e) throws IOException {
        try {
            in.close();
        }
        catch (IOException ignored){
        }
        super.handleIOException(e);
    }

    /**
     * Elasticsearch favors this method. Route it through read(byte[]), due to latter implemented in JOSS.
     * @param b buffer
     * @param off offset in b
     * @param len max length to read
     * @return bytes actually read or -1 on EOF
     * @throws IOException on error in wrapped stream or md5 mismatch
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off == 0 && b.length == len){
            return read(b);
        }

        byte[] buffer = new byte[Math.min(b.length-off, len)];
        int bytesRead = read(buffer);
        if (bytesRead > 0){
            System.arraycopy(buffer, 0, b, off, bytesRead);
        }
        return bytesRead;
    }

    /**
     * All other read methods in <code>InputStream</code> default to abstract read().
     * Route the logic through read(byte[]).
     * This method does not appear to be called.
     * @return byte read or -1 on EOF
     * @throws IOException on I/O error or md5 mismatch
     * @see InputStream#read()
     */
    @Override
    public int read() throws IOException {
        byte[] b=new byte[1];
        return read(b) == -1 ? -1 : b[0]&0xff;
    }
}
