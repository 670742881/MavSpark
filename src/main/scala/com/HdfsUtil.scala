package com


import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object HdfsUtil {

    val conf: Configuration = new Configuration()
    var fs: FileSystem = null
    var files: RemoteIterator[LocatedFileStatus] = null


    def getFiles(HDFSPath: String) = {
      try {
        fs = FileSystem.get( new URI( HDFSPath ), conf )
      } catch {
        case e: IOException => {
          e.printStackTrace
        }
      }
      files
    }

    def getFiles(HDFSPath: String, targetPath: String) = {
      try {
        fs = FileSystem.get( new URI( HDFSPath ), conf )
        // 返回指定路径下所有的文件
        files = fs.listFiles( new Path( targetPath ), false )
      } catch {
        case e: IOException => {
          e.printStackTrace
        }
      }
      files
    }

    def mkdir(finalPath: String) = {
      fs.create( new Path( finalPath ) )
    }

    def rename(oldPath: String, finalPath: String) = {
      fs.rename( new Path( oldPath ), new Path( finalPath ) )
    }

    def exist(existPath: String): Boolean = {
      fs.exists( new Path( existPath ) )
    }

    def delete(deletePath: String) = {
      fs.delete( new Path( deletePath ), true )
    }

    def read(readPath: String) = {
      fs.open( new Path( readPath ) )
    }
 def  getAllFiles(path:String): Unit ={
   val fs = FileSystem.get(URI.create(path), conf)
   val files: Array[FileStatus] = fs.listStatus(new Path(path))
   println()


 }

    def close() = {
      try
          if (fs != null) fs.close()
      catch {
        case e: IOException => {
          e.printStackTrace
        }
      }
    }




}
