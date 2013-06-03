/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

/**
 *
 * @author alexmu
 */
public class AlmightyOutputOperator extends Operator {

    public static final String INPUT_PORT = "input1";
    public static final String OUTPUT_PORT = "output1";
    public static final String ERRDATA_PORT = "error1";

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, INPUT_PORT));
        setupPort(new Port(Port.OUTPUT, OUTPUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    public void validate() throws Exception {
    }

    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(INPUT_PORT).getNext();
                if (dataSet.isValid()) {
                    portSet.get(OUTPUT_PORT).incMetric(dataSet.size());
                    System.out.println("output " + dataSet.size() + " records");
                    reportExecuteStatus();
                } else {
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
