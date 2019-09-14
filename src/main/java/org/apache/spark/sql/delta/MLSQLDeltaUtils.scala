package org.apache.spark.sql.delta

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.delta.actions.{Action, RemoveFile}
import tech.mlsql.common.utils.log.Logging

/**
  * 2019-09-11 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLDeltaUtils(deltaLog: DeltaLog) extends Logging {
  protected def doLogCleanup(targetVersion: Long) = {
    val fs = deltaLog.fs
    var numDeleted = 0
    listExpiredDeltaLogs(targetVersion).map(_.getPath).foreach { path =>
      // recursive = false
      if (fs.delete(path, false)) {
        numDeleted += 1
      }
    }
    logInfo(s"Deleted $numDeleted log files earlier than $targetVersion")
  }

  /**
    * Returns an iterator of expired delta logs that can be cleaned up. For a delta log to be
    * considered as expired, it must:
    *  - have a checkpoint file after it
    *  - be earlier than `targetVersion`
    */
  private def listExpiredDeltaLogs(targetVersion: Long): Iterator[FileStatus] = {
    import org.apache.spark.sql.delta.util.FileNames._

    val latestCheckpoint = deltaLog.lastCheckpoint
    if (latestCheckpoint.isEmpty) return Iterator.empty

    def getVersion(filePath: Path): Long = {
      if (isCheckpointFile(filePath)) {
        checkpointVersion(filePath)
      } else {
        deltaVersion(filePath)
      }
    }

    val files = deltaLog.store.listFrom(deltaFile(deltaLog.logPath, 0))
      .filter(f => isCheckpointFile(f.getPath) || isDeltaFile(f.getPath))
      .filter { f =>
        getVersion(f.getPath) < targetVersion
      }
    files
  }

  protected def doRemoveFileCleanup(items: Seq[Action]) = {
    var numDeleted = 0
    items.filter(item => item.isInstanceOf[RemoveFile])
      .map(item => item.asInstanceOf[RemoveFile])
      .foreach { item =>
        val path = new Path(deltaLog.dataPath, item.path)
        val pathCrc = new Path(deltaLog.dataPath, "." + item.path + ".crc")
        val fs = deltaLog.fs
        try {
          fs.delete(path, false)
          fs.delete(pathCrc, false)
          numDeleted += 1
        } catch {
          case e: Exception =>
        }
      }
    logInfo(s"Deleted $numDeleted  files in optimization progress")
  }

}
