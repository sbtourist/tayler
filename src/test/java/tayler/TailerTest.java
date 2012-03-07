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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link Tailer}.
 *
 * @author Apache Commons IO Team
 * @author Sergio Bossa
 */
public class TailerTest {

    private Tailer tailer;

    @Test
    public void testOverwriteWithMoreDataIgnoresInitialLines() throws Exception {
        File file = new File(getTestDirectory(), "testOverwriteWithMoreDataIgnoresInitialLines.txt");
        createFile(file);

        TestTailerListener listener = new TestTailerListener();
        long delay = 50;

        tailer = new Tailer(file, listener, delay, false);

        Thread thread = new Thread(tailer);
        thread.start();

        // Write a single line:
        write(file, "Line 1");
        Thread.sleep(delay * 2);
        assertEquals("Expected one line.", 1, listener.getLines().size());

        // Rotate:
        eraseFile(file);

        // Write another line, which will be ignored because size is equal to previous one:
        write(file, "Line 2");
        // This will be read instead:
        write(file, "Line 3");
        Thread.sleep(delay * 2);
        assertEquals("Expected two lines.", 2, listener.getLines().size());
        assertEquals("Expected Line 3 value.", "Line 3", listener.getLines().get(1));

        // Stop
        tailer.stop();
        thread.interrupt();
    }

    @Test
    public void testTailerEof() throws Exception {
        // Create & start the Tailer
        long delay = 50;
        final File file = new File(getTestDirectory(), "testTailerEof.txt");
        createFile(file);
        final TestTailerListener listener = new TestTailerListener();
        final Tailer tailer = new Tailer(file, listener, delay, false);
        final Thread thread = new Thread(tailer);
        thread.start();

        // Write some lines to the file
        FileWriter writer = null;
        try {
            writeString(file, "Line");

            Thread.sleep(delay * 2);
            List<String> lines = listener.getLines();
            assertEquals("1 line count", 0, lines.size());

            writeString(file, " one\n");
            Thread.sleep(delay * 2);
            lines = listener.getLines();

            assertEquals("1 line count", 1, lines.size());
            assertEquals("1 line 1", "Line one", lines.get(0));

            listener.clear();
        } finally {
            tailer.stop();
            Thread.sleep(delay * 2);
            IOUtils.closeQuietly(writer);
        }
    }

    @Test
    public void testTailer() throws Exception {
        // Create & start the Tailer
        long delay = 50;
        final File file = new File(getTestDirectory(), "testTailer.txt");
        createFile(file);
        final TestTailerListener listener = new TestTailerListener();
        tailer = new Tailer(file, listener, delay, false);
        final Thread thread = new Thread(tailer);
        thread.start();

        // Write some lines to the file
        write(file, "Line one", "Line two");
        Thread.sleep(delay * 2);
        List<String> lines = listener.getLines();
        assertEquals("1 line count", 2, lines.size());
        assertEquals("1 line 1", "Line one", lines.get(0));
        assertEquals("1 line 2", "Line two", lines.get(1));
        listener.clear();

        // Write another line to the file
        write(file, "Line three");
        Thread.sleep(delay * 2);
        lines = listener.getLines();
        assertEquals("2 line count", 1, lines.size());
        assertEquals("2 line 3", "Line three", lines.get(0));
        listener.clear();

        // Check file does actually have all the lines
        lines = FileUtils.readLines(file);
        assertEquals("3 line count", 3, lines.size());
        assertEquals("3 line 1", "Line one", lines.get(0));
        assertEquals("3 line 2", "Line two", lines.get(1));
        assertEquals("3 line 3", "Line three", lines.get(2));

        // Delete & re-create
        file.delete();
        boolean exists = file.exists();
        String osname = System.getProperty("os.name");
        boolean isWindows = osname.startsWith("Windows");
        assertFalse("File should not exist (except on Windows)", exists && !isWindows);
        createFile(file);
        Thread.sleep(delay * 2);

        // Write another line
        write(file, "Line four");
        Thread.sleep(delay * 2);
        lines = listener.getLines();
        assertEquals("4 line count", 1, lines.size());
        assertEquals("4 line 3", "Line four", lines.get(0));
        listener.clear();

        // Stop
        tailer.stop();
        tailer = null;
        thread.interrupt();
        Thread.sleep(delay * 2);
        write(file, "Line five");
        assertEquals("4 line count", 0, listener.getLines().size());
        assertNull("Should not generate Exception", listener.exception);
        assertEquals("Expected init to be called", 1, listener.initialised.get());
        assertEquals("fileNotFound should not be called", 0, listener.notFound.get());
        assertEquals("fileRotated should be called", 1, listener.rotated.get());
        assertEquals("stop should be called", 1, listener.stopped.get());
    }

    @Test
    public void testStopWithNoFile() throws Exception {
        final File file = new File(getTestDirectory(), "nosuchfile");
        assertFalse("nosuchfile should not exist", file.exists());
        final TestTailerListener listener = new TestTailerListener();
        int delay = 100;
        int idle = 50; // allow time for thread to work
        tailer = new Tailer(file, listener, delay, false);
        final Thread thread = new Thread(tailer);
        thread.start();
        Thread.sleep(idle);
        tailer.stop();
        tailer = null;
        Thread.sleep(delay + idle);
        assertNull("Should not generate Exception", listener.exception);
        assertEquals("Expected init to be called", 1, listener.initialised.get());
        assertTrue("fileNotFound should be called", listener.notFound.get() > 0);
        assertEquals("fileRotated should not be called", 0, listener.rotated.get());
    }

    @Test
    public void testStopWithNoFileUsingExecutor() throws Exception {
        final File file = new File(getTestDirectory(), "nosuchfile");
        assertFalse("nosuchfile should not exist", file.exists());
        TestTailerListener listener = new TestTailerListener();
        int delay = 100;
        int idle = 50; // allow time for thread to work
        tailer = new Tailer(file, listener, delay, false);
        Executor exec = new ScheduledThreadPoolExecutor(1);
        exec.execute(tailer);
        Thread.sleep(idle);
        tailer.stop();
        tailer = null;
        Thread.sleep(delay + idle);
        assertNull("Should not generate Exception", listener.exception);
        assertEquals("Expected init to be called", 1, listener.initialised.get());
        assertTrue("fileNotFound should be called", listener.notFound.get() > 0);
        assertEquals("fileRotated should be not be called", 0, listener.rotated.get());
    }

    @After
    public void tearDown() throws Exception {
        if (tailer != null) {
            tailer.stop();
            Thread.sleep(1000);
        }
        Thread.sleep(1000);
    }

    protected void createFile(File file) throws IOException {
        FileUtils.deleteQuietly(file);
        FileUtils.touch(file);
    }

    protected void eraseFile(File file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rws");
        try {
            raf.setLength(0);
            raf.getFD().sync();
        } finally {
            IOUtils.closeQuietly(raf);
        }
    }

    /** Append some lines to a file */
    private void write(File file, String... lines) throws Exception {
        FileWriter writer = null;
        try {
            writer = new FileWriter(file, true);
            for (String line : lines) {
                writer.write(line + "\n");
            }
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    /** Append a string to a file */
    private void writeString(File file, String string) throws Exception {
        FileWriter writer = null;
        try {
            writer = new FileWriter(file, true);
            writer.write(string);
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    private String getTestDirectory() {
        return System.getProperty("java.io.tmpdir");
    }

    /**
     * Test {@link TailerListener} implementation.
     */
    private static class TestTailerListener implements TailerListener {

        private final List<String> lines = Collections.synchronizedList(new ArrayList<String>());
        final AtomicInteger notFound = new AtomicInteger();
        final AtomicInteger rotated = new AtomicInteger();
        final AtomicInteger initialised = new AtomicInteger();
        final AtomicInteger stopped = new AtomicInteger();
        volatile Exception exception = null;

        public void handle(String line) {
            lines.add(line);
        }

        public List<String> getLines() {
            return lines;
        }

        public void clear() {
            lines.clear();
        }

        public void error(Exception e) {
            exception = e;
        }

        public void init(Tailer tailer) {
            initialised.incrementAndGet();
        }

        public void stop() {
            stopped.incrementAndGet();
        }

        public void fileNotFound() {
            notFound.incrementAndGet();
        }

        public void fileRotated() {
            rotated.incrementAndGet();
        }

    }
}
