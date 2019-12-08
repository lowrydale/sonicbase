/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.anarres.lzo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.anarres.lzo.LzoDecompressor1x;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzopConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LzopInputStream extends LzoInputStream {
  private static final Log LOG = LogFactory.getLog(org.anarres.lzo.LzopInputStream.class);
  private final int flags = this.readHeader();
  private final CRC32 c_crc32_c;
  private final CRC32 c_crc32_d;
  private final Adler32 c_adler32_c;
  private final Adler32 c_adler32_d;
  private boolean eof;

  public LzopInputStream(@Nonnull InputStream in) throws IOException {
    super(in, new LzoDecompressor1x());
    this.c_crc32_c = ((long)this.flags & 512L) == 0L ? null : new CRC32();
    this.c_crc32_d = ((long)this.flags & 256L) == 0L ? null : new CRC32();
    this.c_adler32_c = ((long)this.flags & 2L) == 0L ? null : new Adler32();
    this.c_adler32_d = ((long)this.flags & 1L) == 0L ? null : new Adler32();
    this.eof = false;
  }

  public int getFlags() {
    return this.flags;
  }

  @Nonnegative
  public int getCompressedChecksumCount() {
    int out = 0;
    if (this.c_crc32_c != null) {
      ++out;
    }

    if (this.c_adler32_c != null) {
      ++out;
    }

    return out;
  }

  @Nonnegative
  public int getUncompressedChecksumCount() {
    int out = 0;
    if (this.c_crc32_d != null) {
      ++out;
    }

    if (this.c_adler32_d != null) {
      ++out;
    }

    return out;
  }

  protected void logState(@Nonnull String when) {
    super.logState(when);
    LOG.info(when + " Flags = " + Integer.toHexString(this.flags));
  }

  private int readInt(@Nonnull byte[] buf, @Nonnegative int len) throws IOException {
    this.readBytes(buf, 0, len);
    int ret = (255 & buf[0]) << 24;
    ret |= (255 & buf[1]) << 16;
    ret |= (255 & buf[2]) << 8;
    ret |= 255 & buf[3];
    return len > 3 ? ret : ret >>> 8 * (4 - len);
  }

  private int readHeaderItem(@Nonnull byte[] buf, @Nonnegative int len, @Nonnull Adler32 adler, @Nonnull CRC32 crc32) throws IOException {
    int ret = this.readInt(buf, len);
    adler.update(buf, 0, len);
    crc32.update(buf, 0, len);
    Arrays.fill(buf, (byte)0);
    return ret;
  }

  protected int readHeader() throws IOException {
    byte[] buf = new byte[9];
    this.readBytes(buf, 0, 9);
    if (!Arrays.equals(buf, LzopConstants.LZOP_MAGIC)) {
      throw new IOException("Invalid LZO header");
    } else {
      Arrays.fill(buf, (byte)0);
      Adler32 adler = new Adler32();
      CRC32 crc32 = new CRC32();
      int hitem = this.readHeaderItem(buf, 2, adler, crc32);
      if (hitem > 4112) {
        LOG.debug("Compressed with later version of lzop: " + Integer.toHexString(hitem) + " (expected 0x" + Integer.toHexString(4112) + ")");
      }

      hitem = this.readHeaderItem(buf, 2, adler, crc32);
      if (hitem > 8272) {
        throw new IOException("Compressed with incompatible lzo version: 0x" + Integer.toHexString(hitem) + " (expected 0x" + Integer.toHexString(8272) + ")");
      } else {
        hitem = this.readHeaderItem(buf, 2, adler, crc32);
        if (hitem > 4112) {
          throw new IOException("Compressed with incompatible lzop version: 0x" + Integer.toHexString(hitem) + " (expected 0x" + Integer.toHexString(4112) + ")");
        } else {
          hitem = this.readHeaderItem(buf, 1, adler, crc32);
          switch(hitem) {
            case 1:
            case 2:
            case 3:
              this.readHeaderItem(buf, 1, adler, crc32);
              int flags = this.readHeaderItem(buf, 4, adler, crc32);
              boolean useCRC32 = ((long)flags & 4096L) != 0L;
              boolean extraField = ((long)flags & 64L) != 0L;
              if (((long)flags & 1024L) != 0L) {
                throw new IOException("Multipart lzop not supported");
              } else if (((long)flags & 2048L) != 0L) {
                throw new IOException("lzop filter not supported");
              } else if (((long)flags & 1032192L) != 0L) {
                throw new IOException("Unknown flags in header");
              } else {
                this.readHeaderItem(buf, 4, adler, crc32);
                this.readHeaderItem(buf, 4, adler, crc32);
                this.readHeaderItem(buf, 4, adler, crc32);
                hitem = this.readHeaderItem(buf, 1, adler, crc32);
                if (hitem > 0) {
                  byte[] tmp = hitem > buf.length ? new byte[hitem] : buf;
                  this.readHeaderItem(tmp, hitem, adler, crc32);
                }

                int checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
                hitem = this.readHeaderItem(buf, 4, adler, crc32);
                if (hitem != checksum) {
                  throw new IOException("Invalid header checksum: " + Long.toHexString((long)checksum) + " (expected 0x" + Integer.toHexString(hitem) + ")");
                } else {
                  if (extraField) {
                    LOG.debug("Extra header field not processed");
                    adler.reset();
                    crc32.reset();
                    hitem = this.readHeaderItem(buf, 4, adler, crc32);
                    this.readHeaderItem(new byte[hitem], hitem, adler, crc32);
                    checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
                    if (checksum != this.readHeaderItem(buf, 4, adler, crc32)) {
                      throw new IOException("Invalid checksum for extra header field");
                    }
                  }

                  return flags;
                }
              }
            default:
              throw new IOException("Invalid strategy " + Integer.toHexString(hitem));
          }
        }
      }
    }
  }

  private int readChecksum(@CheckForNull Checksum csum) throws IOException {
    return csum == null ? 0 : this.readInt(false);
  }

  private void testChecksum(@CheckForNull Checksum csum, int value, @Nonnull byte[] data, @Nonnegative int off, @Nonnegative int len) throws IOException {
    if (csum != null) {
      csum.reset();
      csum.update(data, off, len);
      if (value != (int)csum.getValue()) {
        throw new IOException("Checksum failure: Expected " + Integer.toHexString(value) + "; got " + Long.toHexString(csum.getValue()));
      }
    }
  }

  protected boolean readBlock() throws IOException {
    if (this.eof) {
      return false;
    } else {
      int outputBufferLength = this.readInt(false);
      if (outputBufferLength == 0) {
        this.eof = true;
        return false;
      } else {
        this.setOutputBufferSize(outputBufferLength);
        int inputBufferLength = this.readInt(false);
        this.setInputBufferSize(inputBufferLength);
        int v_adler32_d = this.readChecksum(this.c_adler32_d);
        int v_crc32_d = this.readChecksum(this.c_crc32_d);
        if (outputBufferLength == inputBufferLength) {
          this.outputBufferPos = 0;
          this.outputBufferLen.value = outputBufferLength;
          this.readBytes(this.outputBuffer, 0, outputBufferLength);
          this.testChecksum(this.c_adler32_d, v_adler32_d, this.outputBuffer, 0, outputBufferLength);
          this.testChecksum(this.c_crc32_d, v_crc32_d, this.outputBuffer, 0, outputBufferLength);
          return true;
        } else {
          int v_adler32_c = this.readChecksum(this.c_adler32_c);
          int v_crc32_c = this.readChecksum(this.c_crc32_c);
          this.readBytes(this.inputBuffer, 0, inputBufferLength);
          this.testChecksum(this.c_adler32_c, v_adler32_c, this.inputBuffer, 0, inputBufferLength);
          this.testChecksum(this.c_crc32_c, v_crc32_c, this.inputBuffer, 0, inputBufferLength);
          this.decompress(outputBufferLength, inputBufferLength);
          this.testChecksum(this.c_adler32_d, v_adler32_d, this.outputBuffer, 0, outputBufferLength);
          this.testChecksum(this.c_crc32_d, v_crc32_d, this.outputBuffer, 0, outputBufferLength);
          return true;
        }
      }
    }
  }
}
