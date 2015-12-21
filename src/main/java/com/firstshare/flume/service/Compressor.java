package com.firstshare.flume.service;

import com.firstshare.flume.utils.FileUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by wzk on 15/12/21.
 */
public class Compressor {

  private static final Logger LOG = LoggerFactory.getLogger(Compressor.class);

  final CompressionMode compressionMode;

  static final int BUFFER_SIZE = 8192;

  public Compressor(CompressionMode compressionMode) {
    this.compressionMode = compressionMode;
  }

  /**
   * @param nameOfFile2Compress
   * @param nameOfCompressedFile
   * @param innerEntryName       The name of the file within the zip file. Use for ZIP compression.
   */
  public void compress(String nameOfFile2Compress, String nameOfCompressedFile, String innerEntryName) {
    switch (compressionMode) {
      case GZ:
        gzCompress(nameOfFile2Compress, nameOfCompressedFile);
        break;
      case ZIP:
        zipCompress(nameOfFile2Compress, nameOfCompressedFile, innerEntryName);
        break;
      case NONE:
        throw new UnsupportedOperationException(
            "compress method called in NONE compression mode");
    }
  }

  private void zipCompress(String nameOfFile2zip, String nameOfZippedFile, String innerEntryName) {
    File file2zip = new File(nameOfFile2zip);

    if (!file2zip.exists()) {
      LOG.warn("The file to compress named [ {} ] does not exist.", nameOfFile2zip);
      return;
    }

    if (innerEntryName == null) {
      LOG.warn("The innerEntryName parameter cannot be null");
      return;
    }

    if (!nameOfZippedFile.endsWith(".zip")) {
      nameOfZippedFile = nameOfZippedFile + ".zip";
    }

    File zippedFile = new File(nameOfZippedFile);

    if (zippedFile.exists()) {
      LOG.warn("The target compressed file named [ {} ] exist already.", nameOfZippedFile);

      return;
    }

    LOG.info("ZIP compressing [" + file2zip + "] as ["+zippedFile+"]");
    createMissingTargetDirsIfNecessary(zippedFile);

    BufferedInputStream bis = null;
    ZipOutputStream zos = null;
    try {
      bis = new BufferedInputStream(new FileInputStream(nameOfFile2zip));
      zos = new ZipOutputStream(new FileOutputStream(nameOfZippedFile));

      ZipEntry zipEntry = computeZipEntry(innerEntryName);
      zos.putNextEntry(zipEntry);

      byte[] inbuf = new byte[BUFFER_SIZE];
      int n;

      while ((n = bis.read(inbuf)) != -1) {
        zos.write(inbuf, 0, n);
      }

      bis.close();
      bis = null;
      zos.close();
      zos = null;

      if (!file2zip.delete()) {
        LOG.warn("Could not delete [ {} ].", nameOfFile2zip);
      }
    } catch (Exception e) {
      LOG.error("Error occurred while compressing [ {} ].", nameOfFile2zip, e);
    } finally {
      if (bis != null) {
        try {
          bis.close();
        } catch (IOException e) {
          // ignore
        }
      }
      if (zos != null) {
        try {
          zos.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }


  }

  // http://jira.qos.ch/browse/LBCORE-98
  // The name of the compressed file as nested within the zip archive
  //
  // Case 1: RawFile = null, Patern = foo-%d.zip
  // nestedFilename = foo-${current-date}
  //
  // Case 2: RawFile = hello.txt, Pattern = = foo-%d.zip
  // nestedFilename = foo-${current-date}
  //
  // in both cases, the strategy consisting of removing the compression
  // suffix of zip file works reasonably well. The alternative strategy
  // whereby the nested file name was based on the value of the raw file name
  // (applicable to case 2 only) has the disadvantage of the nested files
  // all having the same name, which could make it harder for the user
  // to unzip the file without collisions
  ZipEntry computeZipEntry(File zippedFile) {
    return computeZipEntry(zippedFile.getName());
  }

  ZipEntry computeZipEntry(String filename) {
    String nameOfFileNestedWithinArchive = computeFileNameStr_WCS(filename, compressionMode);
    return new ZipEntry(nameOfFileNestedWithinArchive);
  }


  private void gzCompress(String nameOfFile2gz, String nameOfgzedFile) {
    File file2gz = new File(nameOfFile2gz);

    if (!file2gz.exists()) {
      LOG.warn("The file to compress named [ {} ] does not exist.", nameOfFile2gz);

      return;
    }


    if (!nameOfgzedFile.endsWith(".gz")) {
      nameOfgzedFile = nameOfgzedFile + ".gz";
    }

    File gzedFile = new File(nameOfgzedFile);

    if (gzedFile.exists()) {
      LOG.warn("The target compressed file named [ {} ] exist already. Aborting file compression.", nameOfFile2gz);
      return;
    }

    LOG.info("GZ compressing [ {} ] as [ {} ]", file2gz, gzedFile);
    createMissingTargetDirsIfNecessary(gzedFile);

    BufferedInputStream bis = null;
    GZIPOutputStream gzos = null;
    try {
      bis = new BufferedInputStream(new FileInputStream(nameOfFile2gz));
      gzos = new GZIPOutputStream(new FileOutputStream(nameOfgzedFile));
      byte[] inbuf = new byte[BUFFER_SIZE];
      int n;

      while ((n = bis.read(inbuf)) != -1) {
        gzos.write(inbuf, 0, n);
      }

      bis.close();
      bis = null;
      gzos.close();
      gzos = null;

      if (!file2gz.delete()) {
        LOG.warn("Could not delete [ {} ].", nameOfFile2gz);
      }
    } catch (Exception e) {
      LOG.error("Error occurred while compressing [ {} ].", nameOfFile2gz, e);
    } finally {
      if (bis != null) {
        try {
          bis.close();
        } catch (IOException e) {
          // ignore
        }
      }
      if (gzos != null) {
        try {
          gzos.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  static public String computeFileNameStr_WCS(String fileNamePatternStr,
                                              CompressionMode compressionMode) {
    int len = fileNamePatternStr.length();
    switch (compressionMode) {
      case GZ:
        if (fileNamePatternStr.endsWith(".gz"))
          return fileNamePatternStr.substring(0, len - 3);
        else
          return fileNamePatternStr;
      case ZIP:
        if (fileNamePatternStr.endsWith(".zip"))
          return fileNamePatternStr.substring(0, len - 4);
        else
          return fileNamePatternStr;
      case NONE:
        return fileNamePatternStr;
    }
    throw new IllegalStateException("Execution should not reach this point");
  }


  void createMissingTargetDirsIfNecessary(File file) {
    boolean result = FileUtil.createMissingParentDirectories(file);
    if (!result) {
      LOG.error("Failed to create parent directories for [{}]", file.getAbsolutePath());
    }
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
