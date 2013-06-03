/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.connector;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author alexmu
 */
public class Connector {

    private BlockingQueue<DataSet> dataCache = null;

    public Connector() {
        this.dataCache = new LinkedBlockingQueue<DataSet>(10);
    }

    public DataSet getNext() {
        try {
            DataSet dataSet = this.dataCache.take();
//            System.out.println(this + " read :" + this.dataCache.size());
            return dataSet;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public void write(DataSet _dataSet) {
        try {
            dataCache.put(_dataSet);
//            System.out.println(this + " write :" + this.dataCache.size());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void createConnector(Port pFromPort, Port pToPort) throws Exception {
        Connector connector = new Connector();
        if (!pFromPort.equals(pToPort)) {
            pFromPort.setConnector(connector);
            pToPort.setConnector(connector);
        } else {
            throw new Exception("can't create one connection between the same port");
        }
    }
}
