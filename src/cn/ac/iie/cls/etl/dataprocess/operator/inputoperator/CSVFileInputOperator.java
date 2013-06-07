/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import au.com.bytecode.opencsv.CSVReader;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
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
public class CSVFileInputOperator extends Operator {

    public static final String OUTPUT_PORT = "output1";
    public static final String ERRDATA_PORT = "error1";
    private String csvFilePath = "";
    private boolean hasTitile = false;
    private boolean trimLines = false;
    private String fileEncoding = "";
    private List<Column> columnSet = new ArrayList<Column>();

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.OUTPUT, OUTPUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    public void validate() throws Exception {
        if (getPort(OUTPUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    protected void execute() {
        CSVReader reader = null;//读取csv文件的类
        String[] line = null;//存放读取出来的一行的数据
        FileReader fr = null;//文件读取
        Record record = null;

        try {
            DataSet dataSet = getDataSet();
            fr = new FileReader(csvFilePath);
            reader = new CSVReader(fr);
            while ((line = reader.readNext()) != null) {
                record = new Record();
                for (Column column : columnSet) {
                    record.appendField(line[column.columnIdx - 1]); //取出数据
                }
                dataSet.appendRecord(record);
                System.out.print(record.getField(0));
                System.out.println(record.getField(1));
                if (dataSet.size() >= 1000) {
                    dataSet = getDataSet();
                    //write to outport
                }
            }
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

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("csvFile")) {
                csvFilePath = parameterElement.getStringValue();
            } else if (parameterName.equals("hasHeader")) {
                hasTitile = Boolean.parseBoolean(parameterElement.getStringValue());
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

    public static void main(String[] args) {
        File inputXml = new File("csvfileinputoperator-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            CSVFileInputOperator csvFileInputOperator = new CSVFileInputOperator();
            csvFileInputOperator.parseParameters(dataProcessDescriptor);
            csvFileInputOperator.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
