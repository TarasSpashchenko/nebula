package com.ts.nebula.srm.processingtools.impl;

import io.netty.buffer.Unpooled;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class VertxBufferInputStream extends InputStream {
  private static final Buffer END_BUFFER = Buffer.buffer(Unpooled.EMPTY_BUFFER);
  private static final Buffer END_ERROR_BUFFER = Buffer.buffer(Unpooled.EMPTY_BUFFER);

  private static final Logger log = LoggerFactory.getLogger(VertxBufferInputStream.class);

  private static final
  AtomicReferenceFieldUpdater<VertxBufferInputStream, Buffer> bufUpdater =
    AtomicReferenceFieldUpdater.newUpdater(VertxBufferInputStream.class, Buffer.class, "buffer");

  //TODO: get rid of magic numbers
  private final BlockingQueue<Buffer> buffersQueue = new LinkedBlockingQueue<>(100);

  private final AtomicInteger bytesAvailableInQueue = new AtomicInteger();

  private final AtomicInteger queueAccessCounter = new AtomicInteger();

  private final AtomicInteger queueMaxSize = new AtomicInteger();

  private volatile Buffer buffer = Buffer.buffer(Unpooled.EMPTY_BUFFER);

  protected int count;

  protected int pos;

  protected int markpos = -1;

  protected int marklimit;

  @Override
  public synchronized int read() throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count)
        return -1;
    }
    return getBufIfOpen().getByte(pos++) & 0xff;
  }

  private int read1(byte[] b, int off, int len) throws IOException {
    int avail = count - pos;
    if (avail <= 0) {
      fill();
      avail = count - pos;
      if (avail <= 0) return -1;
    }
    int cnt = Math.min(avail, len);

    getBufIfOpen().getBytes(pos, pos + cnt, b, off);
    pos += cnt;
    return cnt;
  }

  @Override
  public synchronized int read(byte b[], int off, int len)
    throws IOException {

    checkBufIfOpen(); // Check for closed stream

    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int n = 0;
    do {
      int nread = read1(b, off + n, len - n);
      if (nread <= 0) {
        return (n == 0) ? nread : n;
      }
      n += nread;
      if (n >= len) {
        return n;
      }
      // if not closed but no bytes available, return
      if (bytesAvailableInQueue.get() <= 0) {
        return n;
      }

    } while (true);
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    checkBufIfOpen(); // Check for closed stream

    if (n <= 0) {
      return 0;
    }

    long avail = count - pos;

    if (avail <= 0) {
      // Fill in buffer to save bytes for reset
      fill();
      avail = count - pos;
      if (avail <= 0) {
        return 0;
      }
    }

    long skipped = Math.min(avail, n);
    pos += skipped;
    return skipped;
  }

  @Override
  public synchronized int available() {
    int n = count - pos;
    int avail = bytesAvailableInQueue.get();
    return n > (Integer.MAX_VALUE - avail)
      ? Integer.MAX_VALUE
      : n + avail;
  }

  @Override
  public synchronized void mark(int readlimit) {
    marklimit = readlimit;
    markpos = pos;
  }

  @Override
  public synchronized void reset() throws IOException {
    checkBufIfOpen(); // Cause exception if closed
    if (markpos < 0)
      throw new IOException("Resetting to invalid mark");
    pos = markpos;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void close() {
    Buffer buffer;
    while ((buffer = this.buffer) != null) {
      if (bufUpdater.compareAndSet(this, buffer, null)) {
        return;
      }
    }
  }

  public void end() {
    putNextBuffer(END_BUFFER);
  }

  public void error() {
    putNextBuffer(END_ERROR_BUFFER);
  }

  public int remainingBuffersCapacity() {
    return buffersQueue.remainingCapacity();
  }

  public void populate(final Buffer buffer) {
    putNextBuffer(buffer);
  }

  public int getBytesAvailableInQueue() {
    return bytesAvailableInQueue.get();
  }

  private void putNextBuffer(final Buffer buffer) {
    try {
      buffersQueue.put(buffer);
      bytesAvailableInQueue.addAndGet(buffer.length());
    } catch (InterruptedException e) {
      e.printStackTrace();
      log.error("Interrupted while trying to put the next buffer", e);
      Thread.currentThread().interrupt();
    }

  }

  private void checkBufIfOpen() throws IOException {
    Buffer buffer = this.buffer;
    if (buffer == null)
      throw new IOException("Stream closed");
  }

  private Buffer getBufIfOpen() throws IOException {
    Buffer buffer = this.buffer;
    if (buffer == null)
      throw new IOException("Stream closed");
    return buffer;
  }

  /*
   * Since this method is called only from synchronized methods it is safe here to operate with the buffer in a straight way.
   */
  private void fill() throws IOException {
    Buffer currentBuffer = getBufIfOpen();
    if (markpos < 0) {
      pos = 0;            /* no mark: throw away the buffer */
    } else {
      if (pos >= currentBuffer.length()) {
        if (markpos > 0) {
          currentBuffer.setBytes(0, currentBuffer.getBytes(markpos, pos));
          pos = pos - markpos;
          markpos = 0;
        } else if (currentBuffer.length() >= marklimit) {
          markpos = -1;   /* buffer got too big, invalidate mark */
          pos = 0;        /* drop buffer contents */
        }
      }
    }
    count = pos;
    try {
      Buffer nextBuffer = buffersQueue.take();
      if (log.isDebugEnabled()) {
        int accessCounter = queueAccessCounter.incrementAndGet();
        int size = buffersQueue.size();
        int maxSize = queueMaxSize.accumulateAndGet(size, Math::max);
        log.debug("--->> queueAccessCounter: " + accessCounter + ", Queue Length: " + size + ", Queue Max Length: " + maxSize);
      }
      int length = nextBuffer.length();
      bytesAvailableInQueue.addAndGet(-length);

      if (pos == 0) {
        if (!bufUpdater.compareAndSet(this, currentBuffer, nextBuffer)) {
          throw new RuntimeException("Unexpected buffer or the stream is closed.\nExpected: " + currentBuffer + "\nGot: " + buffer);
        }
      } else {
        currentBuffer.setBytes(pos, nextBuffer.getBytes());
      }
      if (length > 0) {
        count += length;
      }

    } catch (InterruptedException e) {
      e.printStackTrace();
      log.error("Interrupted while trying to take the next buffer", e);
      Thread.currentThread().interrupt();
    }
  }
}
