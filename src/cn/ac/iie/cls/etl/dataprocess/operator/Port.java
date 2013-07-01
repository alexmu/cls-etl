/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator;

import cn.ac.iie.cls.etl.dataprocess.connector.Connector;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class Port {

    public static final int INPUT = 0;
    public static final int OUTPUT = 1;
    private int type;
    private String name;
    private List<Connector> availConnectorSet = new ArrayList<Connector>();
    private int availConnectorSetSize = 0;
    private List<Connector> unavailConnectorSet = new ArrayList<Connector>();
    private long metric;

    public Port(int pType, String pName) {
        type = pType;
        name = pName;
        metric = 0;
    }

    public void setConnector(Connector pConnector) {
        availConnectorSet.add(pConnector);
        availConnectorSetSize = availConnectorSet.size();
    }

    public List<Connector> getConnector() {
        return availConnectorSet;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void write(DataSet pDataSet) throws Exception {
        if (type == OUTPUT) {
            for (Connector connector : availConnectorSet) {
                connector.write(pDataSet);
            }
            if (pDataSet.isValid()) {
                metric += pDataSet.size();
            }
        } else {
            throw new Exception("the type of current port is input,can't write");
        }
    }

    public DataSet getNext() throws Exception {
        if (type == INPUT) {
            DataSet dataSet = null;
            while (true) {
                for (int i = 0; i < availConnectorSetSize; i++) {
                    dataSet = availConnectorSet.get(i).getNext();
                    if (dataSet.isValid()) {
                        metric += dataSet.size();
                        return dataSet;
                    } else {
                        unavailConnectorSet.add(availConnectorSet.get(i));
                        availConnectorSet.remove(i);
                        availConnectorSetSize = availConnectorSet.size();
                        if (availConnectorSetSize == 0) {
                            return dataSet;
                        } else {
                            break;
                        }
                    }
                }
            }
        } else {
            throw new Exception("the type of current port is output,can't read");
        }
    }

    public void incMetric(int pDelta) {
        metric += pDelta;
    }

    public long getMetric() {
        return metric;
    }

    public String getName() {
        return name;
    }

    public void setName(String pName) {
        this.name = pName;
    }
}
