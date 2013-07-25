/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.outputoperator;

import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.util.fs.VFSUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author root
 */
public class GlobalTableOuputOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "errport1";
    private List<String> datasourceList = new ArrayList<String>();
    private String tableName;
    private boolean syncOutput;
    private String compareFiledLogicExp;
    private List<DatasourceDivider> datasourceDividerSet = new ArrayList<DatasourceDivider>();
    private List<Field2TableOutput> field2TableOutputSet = new ArrayList<Field2TableOutput>();
    private static Map<String, List<TableColumn>> tableSet = new HashMap<String, List<TableColumn>>();
    private String outputFormat = "";

    @Override
    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.elementIterator("parameter");
        while (parameterItor.hasNext()) {
            Element parameterElement = (Element) parameterItor.next();
            String parameterName = parameterElement.attributeValue("name");
            if (parameterName.equals("datasourceList")) {
                String datasourceListStr = parameterElement.getStringValue();
                String[] datasourceItems = datasourceListStr.split(",");
                for (String datasourceItem : datasourceItems) {
                    datasourceList.add(datasourceItem);
                }
            } else if (parameterName.equals("tableName")) {
                tableName = parameterElement.getStringValue();
            } else if (parameterName.equals("syncOutput")) {
                syncOutput = Boolean.parseBoolean(parameterElement.getStringValue());
            } else if (parameterName.equals("compareFieldLogicExp")) {
                compareFiledLogicExp = parameterElement.getStringValue();
            }
        }

        parameterItor = operatorElt.elementIterator("parameterlist");

        while (parameterItor.hasNext()) {
            Element parameterlistElt = (Element) parameterItor.next();
            if (parameterlistElt.attributeValue("name").equals("mapRules")) {
                Iterator mapRulesItor = parameterlistElt.elementIterator("parametermap");
                while (mapRulesItor.hasNext()) {
                    Element mapRuleElt = (Element) mapRulesItor.next();
                    String mapKey = mapRuleElt.attributeValue("mapKey");
                    String datasource = mapRuleElt.attributeValue("datasource");
                    DatasourceDivider datasourceDivider = new DatasourceDivider(mapKey, datasource);
                    datasourceDividerSet.add(datasourceDivider);
                }
            } else if (parameterlistElt.attributeValue("name").equals("fields")) {
                Iterator fieldsItor = parameterlistElt.elementIterator("parametermap");
                while (fieldsItor.hasNext()) {
                    Element fieldElt = (Element) fieldsItor.next();
                    field2TableOutputSet.add(new Field2TableOutput(fieldElt.attributeValue("streamfield"), fieldElt.attributeValue("tablefield").toLowerCase()));
                }
            }

        }

        List<TableColumn> columnSet = null;
        synchronized (tableSet) {
            columnSet = tableSet.get(tableName);
            if (columnSet == null) {
                columnSet = getTable(tableName);
                if (columnSet == null) {
                    throw new Exception("no table named " + tableName);
                }
                tableSet.put(tableName, columnSet);
            }
        }

        for (TableColumn tableColumn : columnSet) {
            if (outputFormat.isEmpty()) {
                outputFormat = tableColumn.name + "_VAL";
            } else {
                outputFormat += "," + tableColumn.name + "_VAL";
            }
        }

        for (Field2TableOutput field2TableOutput : field2TableOutputSet) {
            outputFormat = outputFormat.replace(field2TableOutput.tableFieldName + "_VAL", field2TableOutput.tableFieldName + "_REP");
        }

        for (TableColumn tableColumn : columnSet) {
            outputFormat = outputFormat.replaceFirst(tableColumn.name + "_VAL", "\\\\N");
        }

        System.out.println("outputFormat:" + outputFormat);

    }

    private List getTable(String pTableName) {
        List<TableColumn> columnSet = new ArrayList<TableColumn>();
        Connection conn = null;
        try {
            String databaseName = "default";
//            Class.forName("org.apache.hive.jdbc.HiveDriver");//impala
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");//hive
            conn = DriverManager.getConnection("jdbc:hive://10.128.125.73:10000/default", "", "");
//            conn = DriverManager.getConnection((String) RuntimeEnv.getParam(RuntimeEnv.HIVE_CONN_STR), "", "");
//            conn = DriverManager.getConnection("jdbc:hive2://192.168.84.2:21050/;auth=noSasl", "", "");
            Statement stmt = conn.createStatement();
            //parse xml
            //then create table
            String sql = "USE " + databaseName;

            stmt.execute(sql);

            sql = "describe " + pTableName;

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getString(1) + " " + rs.getString(2));
                columnSet.add(new TableColumn(rs.getString(1).toLowerCase(), rs.getString(2).toLowerCase()));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            columnSet = null;
        }

        return columnSet;

    }

    @Override
    protected void init0() throws Exception {
    }

    @Override
    public void validate() throws Exception {
        if (getPort(OUT_PORT).getConnector().size() < 1) {
            throw new Exception("out port with no connectors");
        }
    }

    @Override
    protected void execute() {
        Map<String, BufferedWriter> ds2bwSet = new HashMap<String, BufferedWriter>();
        Map<String, String> ds2fileSet = new HashMap<String, String>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        try {
            while (true) {
                DataSet dataSet = portSet.get(IN_PORT).getNext();
                List<String> fieldNameList = dataSet.getFieldNameList();
                for (int i = 0; i < dataSet.size(); i++) {
                    Record record = dataSet.getRecord(i);
                    String outString = outputFormat;

                    for (Field2TableOutput field2TableOutput : field2TableOutputSet) {
                        String fieldVal = record.getField(field2TableOutput.streamFieldName);
                        if (fieldVal == null) {
                            outString = outString.replaceFirst(field2TableOutput.tableFieldName + "_REP", "\\\\N");
                        } else {
                            fieldVal = fieldVal.replaceAll("\\\\", "\\\\\\\\\\\\\\\\");
                            fieldVal = fieldVal.replaceAll(",", "\\\\\\\\,");
                            outString = outString.replaceFirst(field2TableOutput.tableFieldName + "_REP", fieldVal);
                        }
                    }
                    outString = outString.replaceFirst("dms_update_time_REP", sdf.format(new Date()));

                    String compareFieldContent = compareFiledLogicExp;

                    for (String fieldName : fieldNameList) {
                        String fieldVal = record.getField(fieldName);
                        compareFieldContent = compareFieldContent.replaceAll(fieldName, fieldVal == null ? "" : fieldVal);
                    }
                    int size = datasourceDividerSet.size();
                    for (int k = 0; k < size; k++) {
                        DatasourceDivider datasourceDivider = datasourceDividerSet.get(k);
                        System.out.println(datasourceDivider.datasource);
                        String datasource = datasourceDivider.getDataSource(compareFieldContent);
                        if (datasource != null) {
                            BufferedWriter bw = ds2bwSet.get(datasource);
                            if (bw == null) {
                                String tmpDataFileName = tableName + "_" + sdf.format(new Date()) + "_" + datasource;
                                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(tmpDataFileName))));
                                ds2bwSet.put(datasource, bw);
                                ds2fileSet.put(datasource, tmpDataFileName);
                            }
                            bw.write(outString + "\n");
                        }
                    }

                }

                if (dataSet.isValid()) {
//                    portSet.get(OUT_PORT).incMetric(dataSet.size());
                    System.out.println("output " + dataSet.size() + " records");
//                    reportExecuteStatus();
                } else {
                    for (String datasource : ds2bwSet.keySet()) {
                        BufferedWriter bw = ds2bwSet.get(datasource);
                        bw.close();
                        VFSUtil.putFile(ds2fileSet.get(datasource), RuntimeEnv.getParam(RuntimeEnv.HDFS_CONN_STR) + "/user/hive/warehouse/" + datasource + ".db/" + tableName);
                    }
                    break;
                }
            }
            status = SUCCEEDED;
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
        }
    }

    class Field2TableOutput {

        String streamFieldName;
        String tableFieldName;

        public Field2TableOutput(String pStreamFieldName, String pTableFieldName) {
            streamFieldName = pStreamFieldName;
            tableFieldName = pTableFieldName;
        }
    }

    class TableColumn {

        String name;
        String type;

        public TableColumn(String pName, String pType) {
            this.name = pName;
            this.type = pType;
        }
    }

    class DatasourceDivider {

        Pattern dividePattern;
        String datasource;

        public DatasourceDivider(String pDivideRegxStr, String pDataSource) {
            dividePattern = Pattern.compile(pDivideRegxStr);
            datasource = pDataSource;
        }

        public String getDataSource(String pContent) {
            Matcher matcher = dividePattern.matcher(pContent);
            if (matcher.find()) {
                return datasource;
            } else {
                return null;
            }
        }
    }
}
