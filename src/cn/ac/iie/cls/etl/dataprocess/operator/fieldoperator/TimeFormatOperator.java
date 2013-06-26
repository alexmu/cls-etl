/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class TimeFormatOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2TimeFormat> field2TimeFormatSet = new ArrayList<Field2TimeFormat>();

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
                for (Field2TimeFormat field2TimeFormat : field2TimeFormatSet) {
                    if (field2TimeFormat.fromPattern.equals("integer") && field2TimeFormat.toPattern.equals("string")) {
                        SimpleDateFormat toSDF = new SimpleDateFormat(field2TimeFormat.toPattern);
                        String currentFieldValue = null;
                        for (int i = 0; i < dataSize; i++) {
                            Record record = dataSet.getRecord(i);
                            currentFieldValue = record.getField(field2TimeFormat.fieldName);
                            record.setField(field2TimeFormat.fieldName, toSDF.format(new Date(Long.parseLong(currentFieldValue))));
                        }
                    } else if (field2TimeFormat.fromPattern.equals("string") && field2TimeFormat.toPattern.equals("string")) {
                        SimpleDateFormat fromSDF = new SimpleDateFormat(field2TimeFormat.fromPattern);
                        SimpleDateFormat toSDF = new SimpleDateFormat(field2TimeFormat.toPattern);
                        String currentFieldValue = null;
                        for (int i = 0; i < dataSize; i++) {
                            Record record = dataSet.getRecord(i);
                            currentFieldValue = record.getField(field2TimeFormat.fieldName);
                            record.setField(field2TimeFormat.fieldName, toSDF.format(fromSDF.parse(currentFieldValue)));
                        }
                    } else {
                        //fixme                        
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
            String fromType = paraMapElt.attributeValue("fromtype");
            String fromPattern = paraMapElt.attributeValue("frompattern");
            String toType = paraMapElt.attributeValue("totype");
            String toPattern = paraMapElt.attributeValue("topattern");
            field2TimeFormatSet.add(new Field2TimeFormat(fieldName, fromType, fromPattern, toType, toPattern));
        }
    }

    class Field2TimeFormat {

        String fieldName;
        String fromType;
        String fromPattern;
        String toType;
        String toPattern;

        public Field2TimeFormat(String pFieldName, String pFromType, String pFromPattern, String pToType, String pToPattern) {
            this.fieldName = pFieldName;
            this.fromType = pFromType;
            this.fromPattern = pFromPattern;
            this.toType = pToType;
            this.toPattern = pToPattern;
        }
    }
}
