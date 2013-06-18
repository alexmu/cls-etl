/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.recordoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

/**
 *
 * @author alexmu
 */
public class RecordFilterOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERRDATA_PORT = "error1";

    protected void setupPorts() throws Exception{
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    public void validate() throws Exception {
    }

    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                for (int i = 0; i < dataSet.size(); i++) {
                    Record record = dataSet.getRecord(i);
                }

                if (dataSet.isValid()) {
                    portSet.get(OUT_PORT).write(dataSet);
                    reportExecuteStatus();
                } else {
                    portSet.get(OUT_PORT).write(dataSet);
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
