/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util;

import java.io.File;

/**
 *
 * @author alexmu
 */
public class VFSUtil {

    public static File getFile(String pFilePathStr) {
        File file = null;
        if (pFilePathStr.startsWith("hdfs")) {
            try {
                HDFSUtil.get(pFilePathStr, "D:\\projects-workspace\\tmp\\sample.txt");
                file = new File("D:\\projects-workspace\\tmp\\sample.txt");
            } catch (Exception ex) {
                ex.printStackTrace();
                file = null;
            }
        } else if (pFilePathStr.startsWith("file")) {
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
