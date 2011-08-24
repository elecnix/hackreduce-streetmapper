package org.hackreduce.streetmapper.stax;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Copied from Hadoop's BoundedRangeFileInputStream, with a soft end boundary,
 * allowing the reader to continue up to the end of a record.
 * <p>
 * BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop
 * FSDataInputStream as a regular input stream. One can create multiple
 * BoundedRangeFileInputStream on top of the same FSDataInputStream and they
 * would not interfere with each other.
 */
class SoftBoundedRangeFileInputStream extends InputStream {

  private FSDataInputStream in;
  private long pos;
  private long end;
  private long mark;
  private final byte[] oneByte = new byte[1];

  /**
   * Constructor
   * 
   * @param in
   *          The FSDataInputStream we connect to.
   * @param offset
   *          Begining offset of the region.
   * @param length
   *          Length of the region.
   * 
   *          The actual length of the region may be smaller if (off_begin +
   *          length) goes beyond the end of FS input stream.
   */
  public SoftBoundedRangeFileInputStream(FSDataInputStream in, long offset,
      long length) {
    if (offset < 0 || length < 0) {
      throw new IndexOutOfBoundsException("Invalid offset/length: " + offset
          + "/" + length);
    }

    this.in = in;
    this.pos = offset;
    this.end = offset + length;
    this.mark = -1;
  }

  @Override
  public int available() throws IOException {
    int avail = in.available();
    if (pos + avail > end) {
      avail = (int) (end - pos);
    }

    return avail;
  }

  @Override
  public int read() throws IOException {
    int ret = read(oneByte);
    if (ret == 1) return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }
    int ret = 0;
    synchronized (in) {
      in.seek(pos);
      ret = in.read(b, off, len);
    }
    if (ret < 0) {
      end = pos;
      return -1;
    }
    pos += ret;
    return ret;
  }

  @Override
  /*
   * We may skip beyond the end of the file.
   */
  public long skip(long n) throws IOException {
    long len = Math.min(n, end - pos);
    pos += len;
    return len;
  }

  @Override
  public void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public void reset() throws IOException {
    if (mark < 0) throw new IOException("Resetting to invalid mark");
    pos = mark;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void close() {
    // Invalidate the state of the stream.
    in = null;
    pos = end;
    mark = -1;
  }
}