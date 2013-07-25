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
import cn.ac.iie.cls.etl.dataprocess.util.ip.IPUtil;
import cn.ac.iie.cls.etl.dataprocess.util.rangesearch.RangeSearch;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
public class IPMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2IPMap> field2IPMapSet = new ArrayList<Field2IPMap>();
    //
    private static RangeSearch ipGeoLocator = null;
    private static Lock ipGeoLocatorLock = new ReentrantLock();
    private static RangeSearch ipVipLocator = null;
    private static Lock ipVipLocatorLock = new ReentrantLock();
    private static LookupService ipLLLocator = null;
    private static Lock ipLLLocatorLock = new ReentrantLock();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(IPMapOperator.class.getName());
    }

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    private void initIPGeoLocator() throws Exception {
        try {
            ipGeoLocatorLock.lock();
            if (ipGeoLocator == null) {
                ipGeoLocator = new RangeSearch();
                ResultSet rs = null;
                try {
                    String sql = "select start_ip_n,end_ip_n,country,district,isp from dic_ip_location_2";
                    while (true) {
                        try {
                            rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                            break;
                        } catch (Exception ex) {
                            continue;
                        }
                    }
                    int num = 0;
                    while (rs.next()) {
                        long startIPN = rs.getLong("start_ip_n");
                        long endIPN = rs.getLong("end_ip_n");
                        ipGeoLocator.append(startIPN, endIPN, "country", rs.getString("country"));
                        ipGeoLocator.append(startIPN, endIPN, "district", rs.getString("district"));
                        ipGeoLocator.append(startIPN, endIPN, "isp", rs.getString("isp"));
                        num++;
                    }
                    logger.info("init ipGeoLocator successfully with " + num + " records");
                    ipGeoLocator.contructArray();
                } catch (Exception ex) {
                    logger.warn("init ipGeoLocator unsuccessfully for " + ex.getMessage(), ex);
                    ipGeoLocator = null;
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
            ipGeoLocatorLock.unlock();
        }
    }

    private void initIPVipLocator() throws Exception {
        try {
            ipVipLocatorLock.lock();
            if (ipVipLocator == null) {
                ipVipLocator = new RangeSearch();
                ResultSet rs = null;
                try {
                    String sql = "select start_ip_n,end_ip_n,vip_id from dic_vip_ip";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    int num = 0;
                    while (rs.next()) {
                        ipVipLocator.append(rs.getLong("start_ip_n"), rs.getLong("end_ip_n"), rs.getString("vip_id"), rs.getString("vip_id"));
                        num++;
                    }
                    ipVipLocator.contructArray();
                    logger.info("init ipVipLocator successfully with " + num + " records");
                } catch (Exception ex) {
                    logger.warn("init ipVipLocator unsuccessfully for " + ex.getMessage(), ex);
                    ipVipLocator = null;
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
            ipVipLocatorLock.unlock();
        }
    }

    private void initIPLLLocator() throws Exception {
        try {
            ipLLLocatorLock.lock();
            if (ipLLLocator == null) {
                ipLLLocator = new LookupService("GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE);
            }
        } catch (Exception ex) {
            logger.warn("init ipLLLocator unsuccessfully for " + ex.getMessage(), ex);
            ipLLLocator = null;
        } finally {
            ipLLLocatorLock.unlock();
        }
    }

    protected void init0() throws Exception {
        initIPGeoLocator();
        initIPVipLocator();
        initIPLLLocator();
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
                    for (Field2IPMap field2IPMap : field2IPMapSet) {
                        if (field2IPMap.locateMethod.equals("IP2COUNTRY")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("STRING") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                String country = ipGeoLocator.getValue(ipLongValue, "country");
                                record.appendField(country == null ? null : country);
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2DISTRICT")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("STRING") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                String district = ipGeoLocator.getValue(ipLongValue, "district");
                                record.appendField(district == null ? null : district);
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2ISP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("STRING") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                String isp = ipGeoLocator.getValue(ipLongValue, "isp");
                                record.appendField(isp == null ? null : isp);
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2LONTITUDE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("STRING") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                Location location = ipLLLocator.getLocation(ipLongValue);
                                record.appendField(location == null ? null : String.valueOf(location.longitude));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2LATITUDE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("STRING") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                Location location = ipLLLocator.getLocation(ipLongValue);
                                record.appendField(location == null ? null : String.valueOf(location.latitude));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2VIP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("STRING") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                List<String> vipidLst = ipVipLocator.getValues(ipLongValue);
                                String vipidsStr = ",";
                                if (vipidLst != null) {
                                    for (String vipid : vipidLst) {
                                        vipidsStr += vipid + ",";
                                    }
                                }
                                record.appendField(vipidsStr.equals(",") ? null : vipidsStr);
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
            status = SUCCEEDED;
        } catch (Exception ex) {
            ex.printStackTrace();
            status = FAILED;
        } finally {
            try {
                portSet.get(OUT_PORT).write(new DataSet(DataSet.EOS));
            } catch (Exception ex2) {
            }
        }
    }

    @Override
    protected void parseParameters(String pParameters) throws Exception {
        Document document = DocumentHelper.parseText(pParameters);
        Element operatorElt = document.getRootElement();
        Iterator parameterItor = operatorElt.element("parameterlist").elementIterator("parametermap");

        while (parameterItor.hasNext()) {
            Element paraMapElt = (Element) parameterItor.next();
            String srcFieldName = paraMapElt.attributeValue("srcfieldname");
            String srcFieldType = paraMapElt.attributeValue("srcfieldtype");
            String dstFieldName = paraMapElt.attributeValue("dstfieldname");
            String locateMethod = paraMapElt.attributeValue("locatemethod");
            field2IPMapSet.add(new Field2IPMap(srcFieldName, srcFieldType, dstFieldName, locateMethod));
        }
    }

    class Field2IPMap {

        String srcFieldName;
        String srcFieldType;
        String dstFieldName;
        String locateMethod;

        public Field2IPMap(String pSrcFieldName, String pSrcFieldType, String pDstFieldName, String pLocateMethod) {
            srcFieldName = pSrcFieldName;
            srcFieldType = pSrcFieldType;
            dstFieldName = pDstFieldName;
            locateMethod = pLocateMethod;
        }
    }

    public void test(long pIPN) {
        System.out.println(ipGeoLocator.getValue(pIPN, "country"));
        System.out.println(ipGeoLocator.getValue(pIPN, "district"));
        System.out.println(ipGeoLocator.getValue(pIPN, "isp"));
        List<String> vipidLst = ipVipLocator.getValues(pIPN);

        String vipidsStr = ",";
        if (vipidLst != null) {
            for (String vipid : vipidLst) {
                vipidsStr += vipid + ",";
            }
        }
        System.out.println(vipidsStr);
        Location location = ipLLLocator.getLocation(pIPN);
        System.out.println(location.latitude + "," + location.longitude);

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

        IPMapOperator ipMapOperator = new IPMapOperator();
        ipMapOperator.init0();
        ipMapOperator.test(2017968812L);
    }
}
