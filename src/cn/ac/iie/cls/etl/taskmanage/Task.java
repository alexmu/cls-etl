/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.taskmanage;

import cn.ac.iie.cls.etl.dataprocess.operator.DataProcess;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author alexmu
 */
public class Task implements Runnable {

    DataProcess dataProcess = null;
    Map<String, Map> operatorReportStore = new HashMap<String, Map>();

    public Task(DataProcess pDataProcess) {
        dataProcess = pDataProcess;
        dataProcess.setTaskManager(this);
    }

    public void run() {
        Thread t = new Thread(dataProcess);
        t.start();
        while (true) {
            try {
                //report port metrics of operator
                Iterator operatorIter = operatorReportStore.entrySet().iterator();
                while (operatorIter.hasNext()) {
                    Map.Entry operatorEntry = (Map.Entry<String, String>) operatorIter.next();
                    String msg = (String) operatorEntry.getKey() + "-->";
                    Iterator portIter = ((Map) operatorEntry.getValue()).entrySet().iterator();
                    while (portIter.hasNext()) {
                        Map.Entry portEntry = (Map.Entry<String, String>) portIter.next();
                        msg += portEntry.getKey() + ":" + portEntry.getValue() + ",";
                    }
                    System.out.println(msg);
                }
                if (!dataProcess.isAlive()) {
                    break;
                }
                Thread.sleep(100);
            } catch (Exception ex) {
            }
        }
        System.out.println("task exit.");
    }

    public void report(String operatorName, Map portMetrics) {
        synchronized (operatorReportStore) {
            operatorReportStore.put(operatorName, portMetrics);
        }
    }
}
