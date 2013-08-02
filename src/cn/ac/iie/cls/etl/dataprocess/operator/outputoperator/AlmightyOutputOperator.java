/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

/**
 *
 * @author alexmu
 */
public class AlmightyOutputOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
    }

    public void validate() throws Exception {
    }

    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                if (dataSet.isValid()) {
                    portSet.get(OUT_PORT).incMetric(dataSet.size());
                    int dataSize = dataSet.size();
                    int fieldSize = dataSet.getFieldNum();
                    System.out.println(dataSet.getFieldNameList());
                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        for (int j = 0; j < fieldSize; j++) {
                            System.out.print(j == 0 ? record.getField(j) : "," + record.getField(j));
                        }
                        System.out.println();
                    }
                    System.out.println("output " + dataSet.size() + " records");
                    reportExecuteStatus();
                } else {
                    break;
                }
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            status = FAILED;
            ex.printStackTrace();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
    }
}
