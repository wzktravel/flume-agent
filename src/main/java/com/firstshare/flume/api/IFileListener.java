package com.firstshare.flume.api;

import java.nio.file.Path;

/**
 * 文件更新通知
 * Created by wzk on 15/11/27.
 */
public interface IFileListener {

  void changed(Path path);

}
