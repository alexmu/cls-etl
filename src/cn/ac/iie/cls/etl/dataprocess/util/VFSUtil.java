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

    public static File getFile(String pFilePath) {
        File file = null;
        if (pFilePath.startsWith("hdfs")) {
            try {
                HDFSUtil.get(pFilePath, "D:\\projects-workspace\\tmp\\sample.txt");
                file = new File("D:\\projects-workspace\\tmp\\sample.txt");
            } catch (Exception ex) {
                ex.printStackTrace();
                file = null;
            }
        } else if (pFilePath.startsWith("file")) {
        }
        return file;
    }
}
