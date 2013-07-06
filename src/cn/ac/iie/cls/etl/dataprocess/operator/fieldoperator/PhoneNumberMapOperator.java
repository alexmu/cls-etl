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
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class PhoneNumberMapOperator extends Operator {

    public static final String IN_PORT = "inport1";
    public static final String OUT_PORT = "outport1";
    public static final String ERROR_PORT = "error1";
    List<Field2PNMap> field2PNMapSet = new ArrayList<Field2PNMap>();
    //
    private static Map<String, RangeSearch> pn2RS = null;
    private static Lock pn2RSLock = new ReentrantLock();
    private static final int PREFIX_LENTH = 3;

    protected void setupPorts() throws Exception {
        setupPort(new Port(Port.INPUT, IN_PORT));
        setupPort(new Port(Port.OUTPUT, OUT_PORT));
        setupPort(new Port(Port.OUTPUT, ERROR_PORT));
    }

    protected void init0() throws Exception {
        try {
            pn2RSLock.lock();
            if (pn2RS == null) {
                pn2RS = new HashMap<String, RangeSearch>();
                ResultSet rs = null;
                try {
                    String sql = "select start_phone_num,end_phone_num,province,district,isp,remark from dic_phone_location";
                    rs = DaoPool.getDao(RuntimeEnv.METADB_CLUSTER).executeQuery(sql);
                    RangeSearch range = null;
                    String pnPrefix = null;
                    while (rs.next()) {
                        pnPrefix = rs.getString("start_phone_num").substring(0, PREFIX_LENTH);
                        range = pn2RS.get(pnPrefix);
                        if (range == null) {
                            range = new RangeSearch();
                            pn2RS.put(pnPrefix, range);
                        }
                        range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "province", rs.getString("province"));
                        range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "district", rs.getString("district"));
                        range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "isp", rs.getString("isp"));
                        range.append(rs.getLong("start_phone_num"), rs.getLong("end_phone_num"), "remark", rs.getString("remark"));
                    }
                    Set pns = pn2RS.keySet();
                    Iterator itr = pns.iterator();
                    while (itr.hasNext()) {
                        pn2RS.get(itr.next()).contructArray();
                    }
                } catch (Exception ex) {
                    pn2RS = null;
                    throw ex;
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
            pn2RSLock.unlock();
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
                    for (Field2PNMap field2PNMap : field2PNMapSet) {
                        if (field2PNMap.locateMethod.equals("PN2DISTRICT")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                String pnStr = record.getField(field2PNMap.srcFieldName);
                                RangeSearch range = pn2RS.get(pnStr.substring(0, PREFIX_LENTH));
                                record.appendField(range == null ? null : range.getValue(Long.parseLong(pnStr), "district"));
                            }
                        } else if (field2PNMap.locateMethod.equals("PN2ISP")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                String pnStr = record.getField(field2PNMap.srcFieldName);
                                RangeSearch range = pn2RS.get(pnStr.substring(0, PREFIX_LENTH));
                                record.appendField(range == null ? null : range.getValue(Long.parseLong(pnStr), "isp"));
                            }
                        } else if (field2PNMap.locateMethod.equals("PN2PROVINCE")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                String pnStr = record.getField(field2PNMap.srcFieldName);
                                RangeSearch range = pn2RS.get(pnStr.substring(0, PREFIX_LENTH));
                                record.appendField(range == null ? null : range.getValue(Long.parseLong(pnStr), "province"));
                            }
                        } else if (field2PNMap.locateMethod.equals("PN2REMARK")) {
                            int dataSetFieldNum = dataSet.getFieldNum();
                            dataSet.putFieldName2Idx(field2PNMap.dstFieldName, dataSetFieldNum);
                            for (int i = 0; i < dataSize; i++) {
                                Record record = dataSet.getRecord(i);
                                String pnStr = record.getField(field2PNMap.srcFieldName);
                                RangeSearch range = pn2RS.get(pnStr.substring(0, PREFIX_LENTH));
                                record.appendField(range == null ? null : range.getValue(Long.parseLong(pnStr), "remark"));
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
            field2PNMapSet.add(new Field2PNMap(srcFieldName, dstFieldName, locateMethod));
        }
    }

    class Field2PNMap {

        String srcFieldName;
        String dstFieldName;
        String locateMethod;

        public Field2PNMap(String pSrcFieldName, String pDstFieldName, String pLocateMethod) {
            this.srcFieldName = pSrcFieldName;
            this.dstFieldName = pDstFieldName;
            this.locateMethod = pLocateMethod;
        }
    }
}
