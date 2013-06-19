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
import cn.ac.iie.cls.etl.dataprocess.util.VFSUtil;
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

    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    private String csvFile = "";
    private boolean hasTitile = false;
    private boolean trimLines = false;
    private String fileEncoding = "";
    private List<Column> columnSet = new ArrayList<Column>();

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    public void validate() throws Exception {
        if (getPort(OUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    protected void execute() {
        CSVReader reader = null;//读取csv文件的类
        String[] line = null;//存放读取出来的一行的数据
        FileReader fr = null;//文件读取
        Record record = null;

        try {
            DataSet dataSet = getDataSet(DataSet.VALID);
            File inputFile = VFSUtil.getFile(csvFile);
            if (inputFile == null) {
                reportExecuteStatus();
            } else {
                fr = new FileReader(inputFile);
                reader = new CSVReader(fr);
                while ((line = reader.readNext()) != null) {
                    record = new Record();
                    for (Column column : columnSet) {
                        record.appendField(line[column.columnIdx - 1]); //取出数据
                    }
                    dataSet.appendRecord(record);
                    if (dataSet.size() >= 1000) {
                        portSet.get(OUT_PORT).write(dataSet);
                        reportExecuteStatus();
                        dataSet = getDataSet(DataSet.VALID);
                    }
                }
                if (dataSet.size() > 0) {
                    portSet.get(OUT_PORT).write(dataSet);
                    reportExecuteStatus();
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                fr.close();
            } catch (Exception ex) {
            }

            try {
                portSet.get(OUT_PORT).write(getDataSet(DataSet.EOS));
            } catch (Exception ex2) {
            }
        }
    }

    private DataSet getDataSet(int dataSetType) {
        DataSet dataSet = new DataSet(dataSetType);
        if (dataSetType == DataSet.VALID) {
            int idx = 0;
            for (Column column : columnSet) {
                dataSet.putFieldName2Idx(column.columnName, idx++);
            }
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
                csvFile = parameterElement.getStringValue();
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
