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

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    private void initIPGeoLocator() throws Exception {
        try {
            ipGeoLocatorLock.lock();
            if (ipGeoLocator == null) {
                ResultSet rs = null;
                try {
                    String sql = "select start_ip_n,end_ip_n,country,district,isp from dic_ip_location";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        ipGeoLocator.append(rs.getLong("start_ip_n"), rs.getLong("end_ip_n"), "country", rs.getString("country"));
                        ipGeoLocator.append(rs.getLong("start_ip_n"), rs.getLong("end_ip_n"), "district", rs.getString("district"));
                        ipGeoLocator.append(rs.getLong("start_ip_n"), rs.getLong("end_ip_n"), "isp", rs.getString("isp"));
                    }
                    ipGeoLocator.contructArray();
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
                ResultSet rs = null;
                try {
                    String sql = "select start_ip_n,end_ip_n,vip_id from dic_vip_ip";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    while (rs.next()) {
                        ipVipLocator.append(rs.getLong("start_ip_n"), rs.getLong("end_ip_n"), rs.getString("vip_id"), rs.getString("vip_id"));
                    }
                    ipVipLocator.contructArray();
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
                                long ipLongValue = field2IPMap.srcFieldType.equals("string") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                record.appendField(ipGeoLocator.getValue(ipLongValue, "country"));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2DISTRICT")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("string") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                record.appendField(ipGeoLocator.getValue(ipLongValue, "district"));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2ISP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("string") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                record.appendField(ipGeoLocator.getValue(ipLongValue, "isp"));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2LONTITUDE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("string") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                Location location = ipLLLocator.getLocation(ipLongValue);
                                record.appendField(location == null ? null : String.valueOf(location.longitude));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2LATITUDE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("string") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                Location location = ipLLLocator.getLocation(ipLongValue);
                                record.appendField(location == null ? null : String.valueOf(location.latitude));
                            }
                        } else if (field2IPMap.locateMethod.equals("IP2VIP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2IPMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                long ipLongValue = field2IPMap.srcFieldType.equals("string") ? IPUtil.IPV4Str2Long(record.getField(field2IPMap.srcFieldName)) : Long.parseLong(record.getField(field2IPMap.srcFieldName));
                                List<String> vipidLst = ipVipLocator.getValues(ipLongValue);
                                String vipidsStr = ",";
                                for (String vipid : vipidLst) {
                                    vipidsStr += vipid + ",";
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
            String srcFieldType = paraMapElt.attributeValue("srcFieldType");
            String dstFieldName = paraMapElt.attributeValue("dstFieldName");
            String locateMethod = paraMapElt.attributeValue("locateMethod");
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
}
