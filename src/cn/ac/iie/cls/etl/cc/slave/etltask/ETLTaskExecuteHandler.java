/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.cc.slave.SlaveHandler;

/**
 *
 * @author alexmu
 */
public class ETLTaskExecuteHandler implements SlaveHandler {

    @Override
    public String execute(String pRequestContent) {
        String result = null;
        System.out.println(pRequestContent);
        ETLTask etlTask = ETLTask.getETLTask(pRequestContent);
        if (etlTask != null) {
            ETLTaskTracker.getETLTaskTracker().appendTask(etlTask);
            result = "ok";
        } else {
            result = "failed";
        }
        return result;
    }
}
