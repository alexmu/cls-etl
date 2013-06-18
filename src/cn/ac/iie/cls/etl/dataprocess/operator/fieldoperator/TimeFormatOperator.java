/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

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
