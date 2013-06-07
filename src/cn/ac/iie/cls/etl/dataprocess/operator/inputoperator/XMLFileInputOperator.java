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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 *
 * @author alexmu
 */
public class XMLFileInputOperator extends Operator {

    public static final String OUTPUT_PORT = "output1";
    public static final String ERRDATA_PORT = "error1";

    private String xmlFilePath="";
    private String fileEncoding="";
    private List<Column> columnSet = new ArrayList<Column>();
    private Map<String,String> xPathMap = new HashMap<String,String>();
    
    
    protected void setupPorts() throws Exception{
        setupPort(new Port(Port.OUTPUT, OUTPUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERRDATA_PORT));
    }

    public void validate() throws Exception {
         if (getPort(OUTPUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }
    //读取文件
    protected void execute() {
        //得到解析器
        SAXReader saxReader = new SAXReader();
        DataSet dataSet = getDataSet();
        try {
            //指定解析器解析文件
            Document document =saxReader.read(xmlFilePath); 
            List<Element> listIP =document.selectNodes(xPathMap.get("ip"));
            List<Element> listTime = document.selectNodes(xPathMap.get("time"));
            Record record = null;
            for(int i =0;i< listIP.size();i++){  
               record = new Record();
               record.appendField(listIP.get(i).getText());
               record.appendField(listTime.get(i).getText());
               dataSet.appendRecord(record);
               System.out.println(dataSet.size());
               System.out.println(record.getField("ip"));
               System.out.println(record.getField("time"));
            }
            if (dataSet.size() >= 1000) {                
                dataSet = getDataSet();
            } 
            System.out.println(dataSet.size());
        } catch (DocumentException ex) {
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
            if (parameterName.equals("xmlFile")) {
                xmlFilePath = parameterElement.getStringValue();
            } else if (parameterName.equals("fileEncoding")) {
                fileEncoding = parameterElement.getStringValue();
            }
        }

        Element parameterListElt = operatorElt.element("parameterlist");
        Iterator parametermapItor = parameterListElt.elementIterator("parametermap");
        while (parametermapItor.hasNext()) {
            Element parametermapElt = (Element) parametermapItor.next();
            String columnName = parametermapElt.attributeValue("alias");
            int columnType = Column.parseType(parametermapElt.attributeValue("columntype"));
            String xPath =parametermapElt.attributeValue("xpath");
            xPathMap.put(columnName, xPath);
            columnSet.add(new Column(columnName, -1, columnType));
        }
        Collections.sort(columnSet);
       
    }
    
    public static void main(String[] args){
        File inputXml = new File("xmlfileinputoperator-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            XMLFileInputOperator xmlFileInputOperator = new XMLFileInputOperator();
            xmlFileInputOperator.parseParameters(dataProcessDescriptor);
            xmlFileInputOperator.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }
    
    
}
