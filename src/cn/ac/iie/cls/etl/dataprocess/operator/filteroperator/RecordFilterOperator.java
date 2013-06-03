/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.filteroperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

/**
 *
 * @author alexmu
 */
public class RecordFilterOperator extends Operator {

    public static final String INPUT_PORT = "input1";
    public static final String OUTPUT_PORT = "output1";
    public static final String ERRDATA_PORT = "error1";

    protected void setupPorts() throws Exception{
        setupPort(new Port(Port.INPUT, INPUT_PORT));
        setupPort(new Port(Port.OUTPUT, OUTPUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    public void validate() throws Exception {
    }

    protected void execute() {
        int dataSetCnt = 0;
        try {
            while (true) {
//                System.out.println(name + " will get " + "next dataset");
                DataSet dataSet = portSet.get(INPUT_PORT).getNext();
                for (int i = 0; i < dataSet.size(); i++) {
                    Record record = dataSet.getRecord(i);
//                    System.out.println(record.getField("fa"));
                }

                if (dataSet.isValid()) {
                    portSet.get(OUTPUT_PORT).write(dataSet);
                    dataSetCnt++;
//                    System.out.println(name + " get " + dataSetCnt + "st dataset");
                    reportExecuteStatus();
                } else {
                    portSet.get(OUTPUT_PORT).write(dataSet);
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
