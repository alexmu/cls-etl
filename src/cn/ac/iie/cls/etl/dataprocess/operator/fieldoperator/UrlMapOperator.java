/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class UrlMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2UrlMap> field2UrlMapSet = new ArrayList<Field2UrlMap>();
    //
    private static Map<String, String> urlAttackFeatureIDLocator = null;
    private static Lock urlAttackFeatureIDLocatorLock = new ReentrantLock();

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
        try {
            urlAttackFeatureIDLocatorLock.lock();
            if (urlAttackFeatureIDLocator == null) {
                ResultSet rs = null;
                try {
                    String sql = "select id,feature from dim_attack_feature";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        urlAttackFeatureIDLocator.put(rs.getString("id"), rs.getString("feature"));
                    }
                } finally {
                    Connection tmpConn = null;
                    try {
                        tmpConn = rs.getStatement().getConnection();
                    } catch (Exception ex) {
                    }
                    try {
                        rs.close();
                    } catch (Exception ex) {
                    }
                    try {
                        tmpConn.close();
                    } catch (Exception ex) {
                    }
                }
            }
        } finally {
            urlAttackFeatureIDLocatorLock.unlock();
        }
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
                    for (Field2UrlMap field2UrlMap : field2UrlMapSet) {
                        if (field2UrlMap.locateMethod.equals("URL2ATTACKFEATUREID")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2UrlMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                record.appendField(urlAttackFeatureIDLocator.get(record.getField(field2UrlMap.srcFieldName)));
                            }
                        } else if (field2UrlMap.locateMethod.equals("URL2HOSTNAME")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2UrlMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                record.appendField(new URL(record.getField(field2UrlMap.srcFieldName).trim()).getHost());
                            }
                        } else {
                            // fixme
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
            String srcFieldName = paraMapElt.attributeValue("srcFieldName");
            String dstFieldName = paraMapElt.attributeValue("dstFieldName");
            String locateMethod = paraMapElt.attributeValue("locateMethod");
            field2UrlMapSet.add(new Field2UrlMap(srcFieldName, dstFieldName, locateMethod));
        }
    }

    class Field2UrlMap {

        String srcFieldName;
        String dstFieldName;
        String locateMethod;

        public Field2UrlMap(String pSrcFieldName, String pDstFieldName, String pLocateMethod) {
            this.srcFieldName = pSrcFieldName;
            this.dstFieldName = pDstFieldName;
            this.locateMethod = pLocateMethod;
        }
    }
}
