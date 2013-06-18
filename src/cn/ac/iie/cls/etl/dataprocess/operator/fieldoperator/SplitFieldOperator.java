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
public class SplitFieldOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2Split> field2SplitSet = new ArrayList<Field2Split>();

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
            String splitPattern = paraMapElt.attributeValue("splitpattern");
            field2SplitSet.add(new Field2Split(fieldName,splitPattern));
        }
    }

    class Field2Split {

        String fieldName;
        String splitPattern;

        public Field2Split(String pFieldName, String pSplitPattern) {
            fieldName = pFieldName;
            splitPattern = pSplitPattern;
        }
    }
}
