/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.util.ip.IPUtil;
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
public class IPStandardizeOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2IPStd> field2IPStdSet = new ArrayList<Field2IPStd>();

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
                    for (Field2IPStd field2IPStd : field2IPStdSet) {
                        if (field2IPStd.fromPattern.equals("integer") && field2IPStd.toPattern.equals("string")) {
                            //fixme
                            String currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2IPStd.fieldName);
                                record.setField(field2IPStd.fieldName, IPUtil.IPV4Long2Str(currentFieldValue));
                            }
                        } else if (field2IPStd.fromPattern.equals("string") && field2IPStd.toPattern.equals("integer")) {
                            String currentFieldValue = null;
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                currentFieldValue = record.getField(field2IPStd.fieldName);
                                record.setField(field2IPStd.fieldName, String.valueOf(IPUtil.IPV4Str2Long(currentFieldValue)));
                            }
                        } else {
                            //fixme
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
            String fieldName = paraMapElt.attributeValue("fieldname");
            String type = paraMapElt.attributeValue("type");
            String fromPattern = paraMapElt.attributeValue("frompattern");
            String toPattern = paraMapElt.attributeValue("topattern");
            field2IPStdSet.add(new Field2IPStd(fieldName, type, fromPattern, toPattern));
        }
    }

    class Field2IPStd {

        String fieldName;
        String type;
        String fromPattern;
        String toPattern;

        public Field2IPStd(String pFieldName, String pType, String pFromPattern, String pToPattern) {
            fieldName = pFieldName;
            type = pType;
            fromPattern = pFromPattern;
            toPattern = pToPattern;
        }
    }
}
