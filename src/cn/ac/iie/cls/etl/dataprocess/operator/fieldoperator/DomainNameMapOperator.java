/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.fieldoperator;

import cn.ac.ict.ncic.util.dao.DaoPool;
import cn.ac.iie.cls.etl.dataprocess.commons.RuntimeEnv;
import cn.ac.iie.cls.etl.dataprocess.config.Configuration;
import cn.ac.iie.cls.etl.dataprocess.dataset.DataSet;
import cn.ac.iie.cls.etl.dataprocess.dataset.Record;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.server.CLSETLServer;
import cn.ac.iie.cls.etl.dataprocess.util.ip.IPUtil;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class DomainNameMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2DNMap> field2DNMapSet = new ArrayList<Field2DNMap>();
    //
    private static Map<String, String> dnGeoLocator = null;
    private static Lock dnGeoLocatorLock = new ReentrantLock();
    private static Map<String, String> dnCategoryLocator = null;
    private static Lock dnCategoryLocatorLock = new ReentrantLock();
    private static Map<String, String> dnVipLocator = null;
    private static Lock dnVipLocatorLock = new ReentrantLock();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(DomainNameMapOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    private void initDNGeoLocator() throws Exception {
        try {
            dnGeoLocatorLock.lock();
            if (dnGeoLocator == null) {
                ResultSet rs = null;
                try {
                    String sql = "select dn,dn from dim_geo";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        dnGeoLocator.put(rs.getString("dn"), rs.getString("dn"));
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage(), ex);
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
            dnGeoLocatorLock.unlock();
        }
    }

    private void initDNCategoryLocator() throws Exception {
        try {
            dnCategoryLocatorLock.lock();
            if (dnCategoryLocator == null) {
                ResultSet rs = null;
                try {
                    String sql = "select id,category_name from dim_dn_category";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        dnCategoryLocator.put(rs.getString("id"), rs.getString("region_id"));
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage(), ex);
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
            dnCategoryLocatorLock.unlock();
        }
    }

    private void initDNVipLocator() throws Exception {
        try {
            dnVipLocatorLock.lock();
            if (dnVipLocator == null) {
                ResultSet rs = null;
                try {
                    String sql = "select domain,vip_id from dic_vip_domain";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        dnVipLocator.put(rs.getString("domain"), rs.getString("vip_id"));
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage(), ex);
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
            dnVipLocatorLock.unlock();
        }
    }

    protected void init0() throws Exception {
        initDNGeoLocator();
        initDNCategoryLocator();
        initDNVipLocator();
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
                    for (Field2DNMap field2DNMap : field2DNMapSet) {
                        if (field2DNMap.locateMethod.equals("DN2GEO")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                record.appendField(dnGeoLocator.get(record.getField(field2DNMap.srcFieldName)));
                            }
                        } else if (field2DNMap.locateMethod.equals("DN2CATEGORY")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                record.appendField(dnCategoryLocator.get(record.getField(field2DNMap.srcFieldName)));
                            }
                        } else if (field2DNMap.locateMethod.equals("DN2VIP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                record.appendField(dnVipLocator.get(record.getField(field2DNMap.srcFieldName)));
                            }
                        } else if (field2DNMap.locateMethod.equals("DN2TLD")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2DNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                String domainName = record.getField(field2DNMap.srcFieldName);
                                record.appendField(dnVipLocator.get(record.getField(field2DNMap.srcFieldName)));
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
            String srcFieldName = paraMapElt.attributeValue("srcFieldName");
            String dstFieldName = paraMapElt.attributeValue("dstFieldName");
            String locateMethod = paraMapElt.attributeValue("locateMethod");
            field2DNMapSet.add(new Field2DNMap(srcFieldName, dstFieldName, locateMethod));
        }
    }

    class Field2DNMap {

        String srcFieldName;
        String dstFieldName;
        String locateMethod;

        public Field2DNMap(String pSrcFieldName, String pDstFieldName, String pLocateMethod) {
            this.srcFieldName = pSrcFieldName;
            this.dstFieldName = pDstFieldName;
            this.locateMethod = pLocateMethod;
        }
    }

    public static void main(String[] args) throws Exception {
        String configurationFileName = "cls-etl.properties";
        Configuration conf = Configuration.getConfiguration(configurationFileName);
        if (conf == null) {
            throw new Exception("reading " + configurationFileName + " is failed.");
        }

        logger.info("initializng runtime enviroment...");
        if (!RuntimeEnv.initialize(conf)) {
            throw new Exception("initializng runtime enviroment is failed");
        }
        logger.info("initialize runtime enviroment successfully");

        Operator dnmOp = new DomainNameMapOperator();
        dnmOp.init(IN_PORT, IN_PORT);
    }
}
