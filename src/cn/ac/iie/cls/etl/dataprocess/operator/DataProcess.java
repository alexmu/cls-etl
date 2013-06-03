/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator;

import cn.ac.iie.cls.etl.taskmanage.Task;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author alexmu
 */
public class DataProcess extends Operator {

    protected Map<String, Operator> subOperators = new HashMap<String, Operator>();
    protected List<Operator> exitOperatorList = new ArrayList<Operator>();
    protected Map<String, List> port2AtomicPortList = new HashMap<String, List>();

    protected void setupPorts() throws Exception{
    }

    public void putSubOperator(Operator pSubOperator) throws Exception {
        if (subOperators.get(pSubOperator.getName()) == null) {
            subOperators.put(pSubOperator.getName(), pSubOperator);
            pSubOperator.setParentOperator(this);
        } else {
            throw new Exception("duplicate sub operator found.");
        }
    }

    public Operator getSubOperator(String pSubOperatorName) {
        return subOperators.get(pSubOperatorName);
    }

    public void validate() throws Exception {
        Iterator subOperatorItor = subOperators.values().iterator();
        while (subOperatorItor.hasNext()) {
            ((Operator) subOperatorItor.next()).validate();
        }
    }

    protected void execute() {
        System.out.println(name + " starts with " + subOperators.size() + " sub operators");
        Iterator subOperatorItor = subOperators.values().iterator();
        while (subOperatorItor.hasNext()) {
            new Thread((Operator) subOperatorItor.next()).start();
        }

        while (true) {
            try {
                boolean subOperatorAlive = false;
                subOperatorItor = subOperators.values().iterator();
                while (subOperatorItor.hasNext()) {
                    Operator subOperator = (Operator) subOperatorItor.next();
                    if (subOperator.isAlive()) {
                        subOperatorAlive = true;
                        break;
                    }
                }
                //report port metrics
                reportExecuteStatus();
                if (!subOperatorAlive) {
                    alive = false;
                    break;
                }
                Thread.sleep(100);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public void setTaskManager(Task pTaskManager) {
        task = pTaskManager;
        Iterator subOperatorItor = subOperators.values().iterator();
        while (subOperatorItor.hasNext()) {
            ((Operator) subOperatorItor.next()).setTaskManager(pTaskManager);
        }
    }

    public void addAtomicPort(String pPortName, Port pAtomicPort) {
        List atomicPortList = port2AtomicPortList.get(pPortName);
        if (atomicPortList == null) {
            atomicPortList = new ArrayList<Port>();
            port2AtomicPortList.put(pPortName, atomicPortList);
        }
        atomicPortList.add(pAtomicPort);
    }

    public void setupPort(Port pPort) {
        portSet.put(pPort.getName(), pPort);
        List<Port> atomicPortList = port2AtomicPortList.get(pPort.getName());
        if (atomicPortList == null) {
            atomicPortList = new ArrayList<Port>();
            port2AtomicPortList.put(pPort.getName(), atomicPortList);
        }
    }

    public List getAtomicPortList(String pPortName) {
        return port2AtomicPortList.get(pPortName);
    }

    protected void reportExecuteStatus() {
        Map<String, Long> portMetrics = new HashMap<String, Long>();
        Iterator portIter = portSet.values().iterator();
        while (portIter.hasNext()) {
            Port port = (Port) portIter.next();
            List<Port> atomicPortList = port2AtomicPortList.get(port.getName());
            long metrics = 0;
            for (Port atomicPort : atomicPortList) {
                metrics += atomicPort.getMetric();
            }
            portMetrics.put(port.getName(), metrics);
        }
        task.report(name, portMetrics);
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
