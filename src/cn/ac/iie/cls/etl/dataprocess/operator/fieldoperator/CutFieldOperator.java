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
public class CutFieldOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2Cut> field2CutSet = new ArrayList<Field2Cut>();

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
                for (Field2Cut field2Cut : field2CutSet) {
                    if (field2Cut.useReg) {
                        //fixme
                        String currentFieldValue = null;
                        for (int i = 0; i < dataSize; i++) {
                            Record record = dataSet.getRecord(i);
                            currentFieldValue = record.getField(field2Cut.fieldName);
                            record.setField(field2Cut.fieldName, currentFieldValue.replaceAll(field2Cut.regPattern, name));
                        }
                    } else {
                        String currentFieldValue = null;
                        for (int i = 0; i < dataSize; i++) {
                            Record record = dataSet.getRecord(i);
                            currentFieldValue = record.getField(field2Cut.fieldName);
                            record.setField(field2Cut.fieldName, currentFieldValue.substring(field2Cut.startIdx, field2Cut.endIdx));
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
            boolean useReg = Boolean.parseBoolean(paraMapElt.attributeValue("useregularexpression"));
            int startIdx = Integer.parseInt(paraMapElt.attributeValue("firstcharacterindex"));
            int endIdx = Integer.parseInt(paraMapElt.attributeValue("lastcharacterindex"));
            String regPattern = paraMapElt.attributeValue("pattern");
            field2CutSet.add(new Field2Cut(fieldName, useReg, startIdx, endIdx, regPattern));
        }
    }

    class Field2Cut {

        String fieldName;
        boolean useReg;
        String regPattern;
        int startIdx;
        int endIdx;

        public Field2Cut(String pFieldName, boolean pUseReg, int pStartIdx, int pEndIdx, String pRegPattern) {
            fieldName = pFieldName;
            useReg = pUseReg;
            regPattern = pRegPattern;
            startIdx = pStartIdx;
            endIdx = pEndIdx;
        }
    }
}
