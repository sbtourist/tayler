/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tayler;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Implementation of the unix "tail -f" functionality, forked from the Apache Commons IO project and providing fixes, cleaner APIs and improved
 * performance with buffered reads.
 * <p>
 * <h2>1. Create a TailerListener implementation</h3>
 * <p>
 * First you need to create a {@link TailerListener} implementation
 * ({@link TailerListenerAdapter} is provided for convenience so that you don't have to
 * implement every method).
 * </p>
 *
 * <p>For example:</p>
 * <pre>
 *  public class MyTailerListener extends TailerListenerAdapter {
 *      public void handle(String line) {
 *          System.out.println(line);
 *      }
 *  }
 * </pre>
 *
 * <h2>2. Using a Tailer</h2>
 *
 * You can create and use a Tailer in one of three ways:
 * <ul>
 *   <li>Using an {@link java.util.concurrent.Executor}</li>
 *   <li>Using a {@link Thread}</li>
 * </ul>
 *
 * An example of each of these is shown below.
 * 
 *
 * <h3>2.1 Use an Executor</h3>
 *
 * <pre>
 *      TailerListener listener = new MyTailerListener();
 *      Tailer tailer = new Tailer(file, listener, delay);
 *
 *      // stupid executor impl. for demo purposes
 *      Executor executor = new Executor() {
 *          public void execute(Runnable command) {
 *              command.run();
 *           }
 *      };
 *
 *      executor.execute(tailer);
 * </pre>
 *
 *
 * <h3>2.2 Use a Thread</h3>
 * <pre>
 *      TailerListener listener = new MyTailerListener();
 *      Tailer tailer = new Tailer(file, listener, delay);
 *      Thread thread = new Thread(tailer);
 *      thread.setDaemon(true); // optional
 *      thread.start();
 * </pre>
 *
 * <h2>3. Stop Tailing</h3>
 * <p>Remember to stop the tailer when you have done with it:</p>
 * <pre>
 *      tailer.stop();
 * </pre>
 *
 * @see TailerListener
 * @see TailerListenerAdapter
 * @author Apache Commons IO Team
 * @author Sergio Bossa
 */
public class Tailer implements Runnable {

    /**
     * The file which will be tailed.
     */
    private final File file;
    /**
     * The amount of time to wait for the file to be updated.
     */
    private final long delay;
    /**
     * Whether to tail from the end or start of file
     */
    private final boolean end;
    /**
     * The listener to notify of events when tailing.
     */
    private final TailerListener listener;
    /**
     * The tailer will run as long as this value is true.
     */
    private volatile boolean run = true;
    /**
     * The "truncated" line from buffered reads (when buffer ends prior to reaching eol or eof).
     */
    private volatile String remaind = "";
    /**
     * The buffer size for buffered reads.
     */
    private final int bufferSize;
    /**
     * The recycled buffer for buffered reads.
     */
    private final byte[] buffer;

    /**
     * Creates a Tailer for the given file, starting from the beginning, with the default delay of 1.0s and the default buffer size of 1024 bytes.
     * 
     * @param file The file to follow.
     * @param listener the TailerListener to use.
     */
    public Tailer(File file, TailerListener listener) {
        this(file, listener, 1000);
    }

    /**
     * Creates a Tailer for the given file, starting from the beginning and with the default buffer size of 1024 bytes
     * 
     * @param file the file to follow.
     * @param listener the TailerListener to use.
     * @param delay the delay between checks of the file for new content in milliseconds.
     */
    public Tailer(File file, TailerListener listener, long delay) {
        this(file, listener, delay, false);
    }

    /**
     * Creates a Tailer for the given file, with the default buffer size of 1024 bytes.
     * 
     * @param file the file to follow.
     * @param listener the TailerListener to use.
     * @param delay the delay between checks of the file for new content in milliseconds.
     * @param end Set to true to tail from the end of the file, false to tail from the beginning of the file.
     */
    public Tailer(File file, TailerListener listener, long delay, boolean end) {
        this(file, listener, delay, end, 1024);
    }

    /**
     * Creates a Tailer for the given file.
     * 
     * @param file the file to follow.
     * @param listener the TailerListener to use.
     * @param delay the delay between checks of the file for new content in milliseconds.
     * @param end Set to true to tail from the end of the file, false to tail from the beginning of the file.
     * @param bufferSize Buffer size for buffered reads from the tailed file.
     */
    public Tailer(File file, TailerListener listener, long delay, boolean end, int bufferSize) {
        this.file = file;
        this.delay = delay;
        this.end = end;

        this.bufferSize = bufferSize;
        this.buffer = new byte[bufferSize];

        // Save and prepare the listener
        this.listener = listener;

        listener.init(this);
    }

    /**
     * Return the file.
     *
     * @return the file
     */
    public File getFile() {
        return file;
    }

    /**
     * Return the delay.
     *
     * @return the delay
     */
    public long getDelay() {
        return delay;
    }

    /**
     * Follows changes in the file, calling the TailerListener's handle method for each new line.
     */
    public void run() {
        RandomAccessFile reader = null;
        try {
            long position = 0; // position within the file
            // Open the file
            while (run && reader == null) {
                try {
                    reader = new RandomAccessFile(file, "r");
                } catch (FileNotFoundException e) {
                    listener.fileNotFound();
                }

                if (reader == null) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // The current position in the file
                    position = end ? file.length() : 0;
                    reader.seek(position);
                }
            }


            while (run) {

                // Check the file length to see if it was rotated
                long length = file.length();

                boolean shorterLength = length < position;
                boolean longerLength = length > position;

                if (shorterLength) {

                    // File was rotated
                    listener.fileRotated();

                    // Reopen the reader after rotation
                    try {
                        // Ensure that the old file is closed iff we re-open it successfully
                        closeQuietly(reader);
                        reader = new RandomAccessFile(file, "r");
                        position = 0;
                    } catch (FileNotFoundException e) {
                        listener.fileNotFound();
                    }
                    continue;
                } else if (longerLength) {

                    // File was not rotated
                    // The file has more content than it did last time
                    long oldPosition = position;
                    position = readLines(reader);

                    // If position is equal to old position but wasn't supposed to be because file seems to be modified, 
                    // it means file has been rotated but we're not correctly reading it, so force rotation:
                    if (position == oldPosition) {
                        listener.error(new IllegalStateException("Illegal position, try rotating..."));
                        position = Long.MAX_VALUE;
                        continue;
                    }
                }
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                }
            }
            listener.stop();
        } catch (Exception e) {
            listener.error(e);
        } finally {
            closeQuietly(reader);
        }
    }

    /**
     * Allows the tailer to complete its current loop and return.
     */
    public void stop() {
        this.run = false;
    }

    /**
     * Read new lines.
     *
     * @param reader The file to read
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(RandomAccessFile reader) throws IOException {
        int read = reader.read(buffer);
        boolean eof = read < bufferSize;
        readLinesFromBuffer(read);
        while (!eof) {
            read = reader.read(buffer);
            eof = read < bufferSize;
            readLinesFromBuffer(read);
        }
        return reader.getFilePointer();
    }

    /**
     * Read lines from the buffer of given size.
     *
     * @param size The buffer size.
     * @throws java.io.IOException if an I/O error occurs.
     */
    private void readLinesFromBuffer(int size) throws IOException {
        int read = readLineFromBuffer(0, size);
        int pos = read;
        while (pos < size) {
            read = readLineFromBuffer(pos, size);
            pos += read;
        }
    }

    /**
     * Buffered read line.
     *
     * @param start the buffer starting index.
     * @param size The buffer size.
     * @return Number of bytes read.
     * @throws java.io.IOException if an I/O error occurs.
     */
    private int readLineFromBuffer(int start, int size) throws IOException {
        StringBuilder builder = new StringBuilder(remaind);
        int read = 0;
        int current = start;
        int ch = 0;
        boolean eol = false;
        boolean seenCR = false;
        while (current < size && !eol) {
            ch = buffer[current++];
            read++;
            switch (ch) {
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    seenCR = true;
                    break;
                default:
                    if (seenCR) {
                        builder.append('\r');
                        seenCR = false;
                    }
                    builder.append((char) ch); // add character, not its ascii value
            }
        }
        if (!eol) {
            remaind = builder.toString();
        } else {
            remaind = "";
            listener.handle(builder.toString());
        }
        return read;
    }

    private void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }

}
