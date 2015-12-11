package com.firstshare.flume.watcher;

import com.firstshare.flume.api.IFileListener;
import com.sun.nio.file.SensitivityWatchEventModifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * 监测特定目录下文件是否有创建和更新
 * Created by wzk on 15/11/27.
 */
public class FileModifyWatcher implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FileModifyWatcher.class);

  private WatchService watchService;
  private IFileListener fileListener;
  private WatchEvent.Kind[] watchEvents = {ENTRY_CREATE, ENTRY_MODIFY};
  private boolean running = false;

  public static FileModifyWatcher getInstance() {
    return LazyHolder.instance;
  }

  private FileModifyWatcher() {
    try {
      watchService = FileSystems.getDefault().newWatchService();
    } catch (IOException e) {
      LOG.error("cannot build watchservice", e);
    }
  }

  /**
   * @param dir 目录
   * @param listener 文件变更时执行{@link IFileListener#changed(Path)}
   * @throws IOException
   */
  public void watch(Path dir, IFileListener listener) {
    try {
      dir.register(watchService, watchEvents, SensitivityWatchEventModifier.HIGH);
      fileListener = listener;
      LOG.info("monitor directory {}", dir);
    } catch (Exception e) {
      LOG.error("cannot register path {}", dir, e);
    }
  }

  public void start() {
    if (!running) {
      Thread t = new Thread(this, "LocalFileUpdateWatcher");
      t.setDaemon(true);
      t.start();
    }
  }

  @Override
  public void run() {
    running = true;
    while (running) {
      WatchKey key = null;
      try {
        key = watchService.take();
        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind kind = event.kind();
          if (kind == OVERFLOW) {
            continue;
          }
          Path dir = (Path) key.watchable();
          WatchEvent<Path> ev = cast(event);
          Path name = ev.context();
          Path child = dir.resolve(name);

          if (fileListener == null) {
            LOG.error("no file listener found");
            continue;
          }
          if (child.toFile().exists()) {
            fileListener.changed(child);
          }

//          LOG.info("{}: {}", kind.name(), child);
        }
      } catch (InterruptedException x) {
        LOG.error("{} was interrupted, now EXIT", Thread.currentThread().getName());
        break;
      } catch (Exception e) {
        LOG.error("watch dir error. ", e);
      } finally {
        if (key != null) {
          key.reset();
        }
      }
    }
    running = false;
    try {
      watchService.close();
    } catch (IOException e) {
    }
  }

  private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
    return (WatchEvent<T>)event;
  }

  private static final class LazyHolder {
    private static final FileModifyWatcher instance = create();

    private static FileModifyWatcher create() {
      FileModifyWatcher watcher = new FileModifyWatcher();
      watcher.start();
      return watcher;
    }
  }

}
