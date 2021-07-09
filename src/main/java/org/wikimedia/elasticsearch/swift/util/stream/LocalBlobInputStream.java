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

import org.elasticsearch.common.io.Channels;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Class implements seekable stream over random access file.
 * Note that plugin build forbids use of java.io.FileInputStream, java.io.File, etc., instead,
 * encouraging use of java.nio.file.* classes and internal ES utilities.
 * This class is not re-entrant
 */
public class LocalBlobInputStream extends InputStream {
    private final FileChannel channel;
    private final Path path;
    private long markedPos;
    private volatile boolean closed;

    public LocalBlobInputStream(Path path) throws IOException {
        this.path = path;
        channel = FileChannel.open(path, StandardOpenOption.READ);
    }

    /**
     * Useful for debugging.
     */
    @Override
    public String toString() {
        return super.toString() + '{' + path + '}';
    }

    /**
     * Close may be called from a thread that times execution, while reads are being retried in another thread,
     * causing ClosedChannelException
     *
     * @throws IOException @see java.nio.channels.FileChannel.close()
     */
    @Override
    public void close() throws IOException {
        closed = true;
        channel.close();
    }

    private void checkClosed() throws StreamClosedException {
        if (closed){
            throw new StreamClosedException();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off >= b.length){
            throw new IllegalArgumentException("offset outside array");
        }
        checkClosed();
        long position = channel.position();

        // dont read past EOF or buffer boundary - there is a bug in ES Channels implementation
        int adjustedLen = (int) Math.min(b.length - off, Math.min(len, channel.size() - position));
        if (adjustedLen <= 0){
            return -1;
        }

        int read = Channels.readFromFileChannel(channel, position, b, off, adjustedLen);
        if (read > 0) {
            channel.position(position + read);
        }
        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        long position = channel.position();

        try {
            byte[] b = Channels.readFromFileChannel(channel, position, 1);
            channel.position(position+1);
            return b[0];
        }
        catch (EOFException e){
            return -1;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        channel.position(channel.position() + n);
        return n;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            checkClosed();
            markedPos = channel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        checkClosed();
        channel.position(markedPos);
    }
}
