/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server.osstats;

import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LogFileTailer extends Thread {
  private static Logger logger = LoggerFactory.getLogger(LogFileTailer.class);
  private File origLogFile;

  /**
   * How frequently to check for file changes; defaults to 5 seconds
   */
  private long sampleInterval = 1_000;

  /**
   * The log file to tail
   */
  private File logfile;

  /**
   * Defines whether the log file tailer should include the entire contents
   * of the exising log file or tail from the end of the file when the tailer starts
   */
  private boolean startAtBeginning = false;

  /**
   * Is the tailer currently tailing?
   */
  private boolean tailing = false;

  /**
   * Set of listeners
   */
  private Set listeners = new HashSet();

  /**
   * Creates a new log file tailer that tails an existing file and checks the file for
   * updates every 5000ms
   */
  public LogFileTailer(File file) {
    this.logfile = file;
  }

  /**
   * Creates a new log file tailer
   *
   * @param file             The file to tail
   * @param startAtBeginning Should the tailer simply tail or should it process the entire
   *                         file and continue tailing (true) or simply start tailing from the
   *                         end of the file
   */
  public LogFileTailer(File file, boolean startAtBeginning) {
    this.origLogFile = file;
    this.logfile = getCurrLogFile(file);
    this.startAtBeginning = startAtBeginning;;
  }

  private File getCurrLogFile(File file) {
    while (true) {
      final String filename = file.getName();
      File logDir = file.getParentFile();
      File[] files = logDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          if (name.startsWith(filename)) {
            if (name.endsWith("current")) {
              return true;
            }
          }
          return false;
        }
      });
      if (files != null && files.length > 0) {
        return files[0];
      }
      try {
        Thread.sleep(sampleInterval);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }
  }

  interface LogFileTailerListener {
    void newLogFileLine(String line);
  }

  public void addLogFileTailerListener(LogFileTailerListener l) {
    this.listeners.add(l);
  }

  public void removeLogFileTailerListener(LogFileTailerListener l) {
    this.listeners.remove(l);
  }

  protected void fireNewLogFileLine(String line) {
    for (Iterator i = this.listeners.iterator(); i.hasNext(); ) {
      LogFileTailerListener l = (LogFileTailerListener) i.next();
      l.newLogFileLine(line);
    }
  }

  public void stopTailing() {
    this.tailing = false;
  }

  public void run() {
    // The file pointer keeps track of where we are in the file
    long filePointer = 0;

    while (!logfile.exists()) {
      try {
        sleep(1_000);

        logfile = getCurrLogFile(origLogFile);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(e);
      }
    }

    // Determine start point
    if (this.startAtBeginning) {
      filePointer = 0;
    }
    else {
      filePointer = this.logfile.length();
    }

    try {
      // Start tailing
      this.tailing = true;
      RandomAccessFile file = new RandomAccessFile(logfile, "r");
      boolean reOpen = true;
      while (this.tailing) {
        try {
          // Compare the length of the file to the file pointer
          long fileLength = this.logfile.length();
          if (reOpen || fileLength < filePointer) {
            // Log file must have been rotated or deleted;
            // reopen the file and reset the file pointer
            file.close();

            logfile = getCurrLogFile(origLogFile);
            file = new RandomAccessFile(logfile, "r");
            filePointer = 0;
            reOpen = false;
          }

          if (fileLength > filePointer) {
            // There is data to read
            file.seek(filePointer);
            String line = file.readLine();
            while (line != null) {
              fireNewLogFileLine(line);
              line = file.readLine();
            }
            filePointer = file.getFilePointer();
          }

          // Sleep for the specified interval
          sleep(this.sampleInterval);
          File newLogFile = getCurrLogFile(origLogFile);
          if (!newLogFile.getName().equals(logfile.getName())) {
            reOpen = true;
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        catch (Exception e) {
          reOpen = true;
          logger.error("Error reading gc log file: filename=" + logfile.getAbsolutePath(), e);
        }
      }

      // Close the file that we are tailing
      file.close();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}