/*
 * Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package streamdrill.crawler

import java.util.{Date, TimeZone, Calendar}
import grizzled.slf4j.Logging
import java.io._
import java.util.zip.GZIPOutputStream
import java.util.concurrent.TimeUnit
import java.text.SimpleDateFormat

/**
 * Rolling file writer that creates new files for a certain time span and compresses finished files.
 *
 * @author Matthias L. Jugel
 */

object RollingFileWriter {
  def apply(dir: String) = new RollingFileWriter(new File(dir))

  def apply(dir: String, increment: Int, timeUnit: TimeUnit) = new RollingFileWriter(new File(dir), increment, timeUnit)
}

class RollingFileWriter(dir: File, increment: Int = 1, timeUnit: TimeUnit = TimeUnit.DAYS) extends Logging {
  info("saving dumps to %s".format(dir.getAbsolutePath))

  private val timeZone = TimeZone.getTimeZone("UTC")
  private val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z")

  private val current = Calendar.getInstance(timeZone)
  private val next = Calendar.getInstance(timeZone)

  val formatter = timeUnit match {
    case TimeUnit.DAYS =>
      next.set(Calendar.HOUR_OF_DAY, 0)
      next.set(Calendar.MINUTE, 0)
      next.set(Calendar.SECOND, 0)
      next.set(Calendar.MILLISECOND, 0)
      new SimpleDateFormat("yyyy-MM-dd")
    case TimeUnit.HOURS =>
      next.set(Calendar.MINUTE, 0)
      next.set(Calendar.SECOND, 0)
      next.set(Calendar.MILLISECOND, 0)
      new SimpleDateFormat("yyyy-MM-dd@HH")
    case TimeUnit.MINUTES =>
      next.set(Calendar.SECOND, 0)
      next.set(Calendar.MILLISECOND, 0)
      new SimpleDateFormat("yyyy-MM-dd@HHmm")
    case _ =>
      throw new IllegalArgumentException("unsupported time unit: %s".format(timeUnit))
  }


  private var writerFileName = "unknown.json"
  private var writer = createWriter(current.getTime)

  def createWriter(date: Date) = {
    writerFileName = "%s.json".format(formatter.format(date))
    info("creating new writer: %s".format(new File(dir, writerFileName)))

    next.setTimeInMillis(date.getTime)
    timeUnit match {
      case TimeUnit.DAYS =>
        next.add(Calendar.DAY_OF_YEAR, increment)
        next.set(Calendar.HOUR_OF_DAY, 0)
        next.set(Calendar.MINUTE, 0)
        next.set(Calendar.SECOND, 0)
        next.set(Calendar.MILLISECOND, 0)
      case TimeUnit.HOURS =>
        next.add(Calendar.HOUR_OF_DAY, increment)
        next.set(Calendar.MINUTE, 0)
        next.set(Calendar.SECOND, 0)
        next.set(Calendar.MILLISECOND, 0)
      case TimeUnit.MINUTES =>
        next.add(Calendar.MINUTE, increment)
        next.set(Calendar.SECOND, 0)
        next.set(Calendar.MILLISECOND, 0)
      case _ =>
        throw new IllegalArgumentException("unsupported time unit: %s".format(timeUnit))
    }
    info("rolling writer on %s".format(timeFormat.format(next.getTime)))

    new PrintWriter(new BufferedWriter(new FileWriter(new File(dir, writerFileName), true)))
  }

  def compress(sourceFile: File) {
    new Thread(new Runnable {
      def run() {
        try {
          val start = System.nanoTime
          val source = new BufferedInputStream(new FileInputStream(sourceFile))
          val targetFile = new File("%s.gz".format(sourceFile.getCanonicalPath))
          val target = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(targetFile)))
          var buffer = new Array[Byte](1024 * 1024) // 1 MB buffer
          var bytesRead = source.read(buffer)
          while (bytesRead != -1) {
            target.write(buffer, 0, bytesRead)
            bytesRead = source.read(buffer)
          }
          target.flush()
          target.close()
          source.close()
          val elapsed = System.nanoTime - start
          sourceFile.getTotalSpace
          targetFile.getTotalSpace
          info("compressed %s in %.2fs, saving %.2fMB".format(
            sourceFile, elapsed / 1e9,
            (sourceFile.length - targetFile.length) / (1024.0 * 1024.0)))
          sourceFile.delete()
        }
        catch {
          case e: Exception => error("can't compress %s".format(sourceFile), e)
        }
      }
    }).start()
  }

  var lineCount = 0L

  def write(line: String) {
    current.setTimeInMillis(System.currentTimeMillis)
    if (current.after(next)) {
      writer.flush()
      writer.close()
      info("wrote %d lines to %s".format(lineCount, writerFileName))
      compress(new File(dir, writerFileName))

      writer = createWriter(current.getTime)
      writer.println()
      lineCount = 0
    }

    writer.println(line)
    writer.flush()
    lineCount += 1
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      info("flushing and closing %s writer ...".format(writerFileName))
      try {
        writer.flush()
        writer.close()
      }
      catch {
        case e: Exception => error("can't close writer properly", e)
      }
    }
  }))
}