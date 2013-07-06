/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class AddFieldOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private List<Field2Add> field2AddSet = new ArrayList<Field2Add>();

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
    }

    public void validate() throws Exception {
        if (getPort(OUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    protected void execute() {
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                
                if (dataSet.isValid()) {
                    int dataSize = dataSet.size();
                    
                    int dataSetFieldNum = dataSet.getFieldNum();
                    int idx = 0;
                    for (Field2Add columnValueHolder : field2AddSet) {
                        dataSet.putFieldName2Idx(columnValueHolder.fieldName, dataSetFieldNum + (idx++));
                    }

                    for (int i = 0; i < dataSize; i++) {
                        Record record = dataSet.getRecord(i);
                        for (Field2Add columnValueHolder : field2AddSet) {
                            record.appendField(columnValueHolder.fieldValue);
                        }
                    }
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
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");

        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            field2AddSet.add(new Field2Add(paraMapElt.attributeValue("fieldname"), paraMapElt.attributeValue("fieldvalue")));
        }
    }

    class Field2Add {

        String fieldName;
        String fieldValue;

        public Field2Add(String pColumnName, String pColumnValue) {
            fieldName = pColumnName;
            fieldValue = pColumnValue;
        }
    }

    public static void main(String[] args) {
        File inputXml = new File("addFieldOperator-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            AddFieldOperator addFieldOperator = new AddFieldOperator();
            addFieldOperator.parseParameters(dataProcessDescriptor);
            addFieldOperator.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
