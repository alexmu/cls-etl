/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.fs;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author alexmu
 */
public class VFSUtil {

    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

    public static File getFile(String pFilePathStr) {
        File file = null;
        if (pFilePathStr.startsWith("hdfs")) {
            try {
                String localTmpFilePathStr = "/tmp/xtld/data-cache/" + sdf.format(new Date());
                HDFSUtil.get(pFilePathStr, localTmpFilePathStr);
                file = new File(localTmpFilePathStr);
            } catch (Exception ex) {
                ex.printStackTrace();
                file = null;
            }
        } else if (pFilePathStr.startsWith("file")) {
            file = new File(pFilePathStr.replaceFirst("file://", ""));
        } else {
            file = new File("D:\\projects-workspace\\tmp\\sample.txt");
        }
        return file;
    }

    public static void putFile(String pSrcFilePathStr, String pDestFilePathStr) {
        if (pDestFilePathStr.startsWith("hdfs")) {
            try {
                HDFSUtil.put(pSrcFilePathStr, pDestFilePathStr);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
