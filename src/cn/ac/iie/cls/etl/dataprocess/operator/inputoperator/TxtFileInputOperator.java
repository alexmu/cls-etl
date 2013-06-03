/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class TxtFileInputOperator extends Operator {

    //ports
    public static final String OUTPUT_PORT = "output1";
    public static final String ERRDATA_PORT = "error1";
    //parameters
    private String txtFilePath = "";
    private boolean hasTitile = false;
    private String fieldDelimiter = "";
    private String recordDelimiter = "";
    private boolean trimLines = false;
    private String fileEncoding = "";
    private List<Column> columnSet = new ArrayList<Column>();

    @Override
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.OUTPUT, OUTPUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("txtfile")) {
                txtFilePath = parameterElement.getStringValue();
            } else if (parameterName.equals("hasHeader")) {
                hasTitile = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("fieldDelimiter")) {
                fieldDelimiter = parameterElement.getStringValue();
            } else if (parameterName.equals("recordDelimiter")) {
                recordDelimiter = parameterElement.getStringValue();
            } else if (parameterName.equals("trimLines")) {
                trimLines = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("fileEncoding")) {
                fileEncoding = parameterElement.getStringValue();
            }
        }

        Element parameterListElt = operatorElt.element("parameterlist");
        Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
        while (parametermapItor.hasNext()) {
            Element parametermapElt = (Element) parametermapItor.next();
            String columnName = parametermapElt.attributeValue("columnname");
            int columnIdx = Integer.parseInt(parametermapElt.attributeValue("columnindex"));
            int columnType = Column.parseType(parametermapElt.attributeValue("columntype"));
            columnSet.add(new Column(columnName, columnIdx, columnType));
        }
        Collections.sort(columnSet);
    }

    @Override
    public void validate() throws Exception {
        if (getPort(OUTPUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    @Override
    protected void execute() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(txtFilePath)));
            String line = "";
            DataSet dataSet = getDataSet();
            while ((line = br.readLine()) != null) {
                String[] fieldItems = line.split(fieldDelimiter);
                Record record = new Record();
                for (Column column : columnSet) {
                    record.appendField(fieldItems[column.columnIdx - 1]);
                }
                dataSet.appendRecord(record);
                if (dataSet.size() >= 1000) {
                    System.out.println(dataSet.size());
                    dataSet = getDataSet();
                }
            }
            System.out.println(dataSet.size());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private DataSet getDataSet() {
        DataSet dataSet = new DataSet(DataSet.VALID);
        int idx = 0;
        for (Column column : columnSet) {
            dataSet.putFieldName2Idx(column.columnName, idx++);
        }
        return dataSet;
    }

    public static void main(String[] args) {
        File inputXml = new File("txtfileinputoperator-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            System.out.println(dataProcessDescriptor);
            TxtFileInputOperator txtFileInputOperator = new TxtFileInputOperator();
            txtFileInputOperator.parseParameters(dataProcessDescriptor);
            txtFileInputOperator.execute();
            System.out.println("parse ok");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
