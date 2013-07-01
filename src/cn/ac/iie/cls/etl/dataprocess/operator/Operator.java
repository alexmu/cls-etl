/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator;

import cn.ac.iie.cls.etl.cc.slave.etltask.ETLTask;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author alexmu
 */
public abstract class Operator implements Runnable {

    protected String name;
    protected Operator parentOperator;
    protected Map<String, Port> portSet = new HashMap<String, Port>();
    protected ETLTask task = null;
    protected int status;
    public static final int STANDBY = 0;
    public static final int EXECUTING = 0;
    public static final int SUCCEEDED = 2;
    public static final int FAILED = -1;

    protected abstract void setupPorts() throws Exception;

    protected abstract void parseParameters(String pParameters) throws Exception;

    public void init(String pName, String pParameters) throws Exception {
        name = pName;
        status = STANDBY;
        setupPorts();
        parseParameters(pParameters);
    }

    public void setupPort(Port pPort) {
        portSet.put(pPort.getName(), pPort);
    }

    public Port getPort(String pPortName) {
        return portSet.get(pPortName);
    }

    public String getName() {
        return name;
    }

    public void setParentOperator(Operator parentOperator) {
        this.parentOperator = parentOperator;
    }

    public abstract void validate() throws Exception;

    public void setTaskManager(ETLTask pTaskManager) {
        task = pTaskManager;
    }

    public boolean isDone() {
        if (status == SUCCEEDED || status == FAILED) {
            return true;
        } else {
            return false;
        }
    }

    protected void reportExecuteStatus() {
        Map<String, Long> portMetrics = new HashMap<String, Long>();
        Iterator portIter = portSet.values().iterator();
        while (portIter.hasNext()) {
            Port port = (Port) portIter.next();
            portMetrics.put(port.getName(), port.getMetric());
        }
        task.report(name, portMetrics);
    }

    protected abstract void execute();

    public void run() {
        System.out.println(name + " starts.");
        status = EXECUTING;
        execute();        
        System.out.println(name + " exit.");
    }
}
