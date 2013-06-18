/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
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
public class FieldLUConvertOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2LUConvert> field2LUConvertSet = new ArrayList<Field2LUConvert>();

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
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
                int dataSize = dataSet.size();
                for (Field2LUConvert field2LUConvert : field2LUConvertSet) {
                    if (field2LUConvert.convertType.equals("lower")) {
                        for (int i = 0; i < dataSize; i++) {
                            Record record = dataSet.getRecord(i);
                            record.setField(field2LUConvert.fieldName, record.getField(field2LUConvert.fieldName).toLowerCase());
                        }
                    } else if (field2LUConvert.convertType.equals("upper")) {
                        for (int i = 0; i < dataSize; i++) {
                            Record record = dataSet.getRecord(i);
                            record.setField(field2LUConvert.fieldName, record.getField(field2LUConvert.fieldName).toUpperCase());
                        }
                    }
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
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");

        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            String fieldName = paraMapElt.attributeValue("fieldname");
            String convertType = paraMapElt.attributeValue("converttype");
            field2LUConvertSet.add(new Field2LUConvert(fieldName, convertType));
        }
    }

    class Field2LUConvert {

        String fieldName;
        String convertType;

        public Field2LUConvert(String pFieldName, String pConvertType) {
            fieldName = pFieldName;
            convertType = pConvertType;
        }
    }
}
