/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;

/**
 *
 * @author alexmu
 */
public class CSVFileInputOperator extends Operator {

    public static final String OUTPUT_PORT = "output1";
    public static final String ERRDATA_PORT = "error1";

    protected void setupPorts() throws Exception{
        setupPort(new Port(Port.OUTPUT, OUTPUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    public void validate() throws Exception {
        if (getPort(OUTPUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    protected void execute() {
        try {
            int batchSize = 0;
            while (true) {
                DataSet dataSet = batchSize++ < 1000 ? getNextDataSet(true, 1000) : getNextDataSet(false, 1000);
                getPort(OUTPUT_PORT).write(dataSet);
                if (dataSet.isValid()) {
                    reportExecuteStatus();
                } else {
                    break;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private DataSet getNextDataSet(boolean valid, int _size) {
        DataSet dataSet = null;
        if (valid) {
            dataSet = new DataSet(DataSet.VALID);
            dataSet.putFieldName2Idx("fa", 0);
            dataSet.putFieldName2Idx("fb", 1);
            dataSet.putFieldName2Idx("fc", 2);
            for (int i = 0; i < _size; i++) {
                Record record = new Record();
                record.appendField("a");
                record.appendField("b");
                record.appendField("c");
                dataSet.appendRecord(record);
            }
        } else {
            dataSet = new DataSet(DataSet.EOS);
        }
        return dataSet;
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
