package com.firstshare.flume.service;

/**
 * Created by wzk on 15/12/21.
 */
public class CompressionRunnable implements Runnable {
  final Compressor compressor;
  final String nameOfFile2Compress;
  final String nameOfCompressedFile;
  final String innerEntryName;

  public CompressionRunnable(Compressor compressor, String nameOfFile2Compress,
                             String nameOfCompressedFile, String innerEntryName) {
    this.compressor = compressor;
    this.nameOfFile2Compress = nameOfFile2Compress;
    this.nameOfCompressedFile = nameOfCompressedFile;
    this.innerEntryName = innerEntryName;
  }

  public void run() {
    compressor.compress(nameOfFile2Compress, nameOfCompressedFile, innerEntryName);
  }
}
