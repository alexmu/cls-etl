/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.fs;

import java.io.File;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author alexmu
 */
public class HDFSUtil {

    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(HDFSUtil.class.getName());
    }

    public static void put(String pSrcFilePathStr, String pHDFSFilePathStr) throws Exception {

        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(URI.create(pHDFSFilePathStr), conf);
            Path srcPath = new Path(pSrcFilePathStr);
            Path dstPath = new Path(pHDFSFilePathStr);
            if (fs.isFile(dstPath)) {
            } else {
                fs.mkdirs(dstPath);
            }
            fs.copyFromLocalFile(srcPath, dstPath);

        } catch (Exception ex) {
            logger.warn("put " + pSrcFilePathStr + " to hdfs(" + pHDFSFilePathStr + ") unsuccessfully for " + ex.getMessage(), ex);
            throw ex;
        } finally {
            try {
                fs.close();
            } catch (Exception ex) {
            }
        }
    }

    public static void get(String pHDFSFilePathStr, String pLocalFilePathStr) throws Exception {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            fs = FileSystem.get(URI.create(pHDFSFilePathStr), conf);
            fs.copyToLocalFile(new Path(pHDFSFilePathStr), new Path(pLocalFilePathStr));
        } catch (Exception ex) {
            logger.warn("get " + pHDFSFilePathStr + " to localfs(" + pLocalFilePathStr + ") unsuccessfully for " + ex.getMessage(), ex);
            throw ex;
        } finally {
            try {
                fs.close();
            } catch (Exception ex) {
            }
        }
    }
}
