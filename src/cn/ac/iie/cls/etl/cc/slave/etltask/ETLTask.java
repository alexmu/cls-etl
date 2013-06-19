/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.cc.slave.etltask;

import cn.ac.iie.cls.etl.dataprocess.DataProcessFactory;
import cn.ac.iie.cls.etl.dataprocess.operator.DataProcess;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class ETLTask implements Runnable {

    private String processJobInstanceID = "";
    private String filePath = "";
    DataProcess dataProcess = null;
    Map<String, Map> operatorReportStore = new HashMap<String, Map>();
    static Logger logger = null;

    static {
        PropertyConfigurator.configure("log4j.properties");
        logger = Logger.getLogger(ETLTask.class.getName());
    }

    private ETLTask() {
    }

    public static ETLTask getETLTask(String pETLProcessDescriptor) {
        ETLTask etlTask = new ETLTask();
        try {
            Document processJobDoc = DocumentHelper.parseText(pETLProcessDescriptor);
            Element processJobInstanceElt = processJobDoc.getRootElement();

            Element processJobInstanceIdElt = processJobInstanceElt.element("processJobInstanceId");
            if (processJobInstanceIdElt == null) {
                logger.error("no processJobInstanceId element found in " + pETLProcessDescriptor);
                etlTask = null;
            } else {
                etlTask.processJobInstanceID = processJobInstanceIdElt.getStringValue();
                Element processConfigElt = processJobInstanceElt.element("processConfig");
                if (processConfigElt == null) {
                    logger.error("no processConfig element found in " + pETLProcessDescriptor);
                    etlTask = null;
                } else {
                    List<Element> operatorElts = processConfigElt.element("operator").elements("operator");

                    for (Element operatorElt : operatorElts) {
                        if (operatorElt.attributeValue("class").endsWith("FileInput")) {
                            List<Element> paramElts = operatorElt.elements("parameter");
                            for (Element paramElt : paramElts) {
                                if (paramElt.attributeValue("name").equals("csvFile")) {
                                    etlTask.filePath = paramElt.getStringValue();
                                }
                            }
                            break;
                        }
                    }

                    logger.debug(processConfigElt.element("operator").asXML());
                    DataProcess dataProcess = DataProcessFactory.getDataProcess(processConfigElt.element("operator"));
                    if (dataProcess == null) {
                        logger.error("parse failed");
                        etlTask = null;
                    } else {
                        etlTask.dataProcess = dataProcess;
                        etlTask.dataProcess.setTaskManager(etlTask);
                    }
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            etlTask = null;
        }
        return etlTask;
    }

    public String getID() {
        return processJobInstanceID + "_" + filePath;
    }

    public void run() {
        Thread t = new Thread(dataProcess);
        t.start();
        while (true) {
            try {
                //report port metrics of operator
                Iterator operatorIter = operatorReportStore.entrySet().iterator();
                while (operatorIter.hasNext()) {
                    Map.Entry operatorEntry = (Map.Entry<String, String>) operatorIter.next();
                    String msg = (String) operatorEntry.getKey() + "-->";
                    Iterator portIter = ((Map) operatorEntry.getValue()).entrySet().iterator();
                    while (portIter.hasNext()) {
                        Map.Entry portEntry = (Map.Entry<String, String>) portIter.next();
                        msg += portEntry.getKey() + ":" + portEntry.getValue() + ",";
                    }
                    System.out.println(msg);
                }
                if (!dataProcess.isAlive()) {
                    break;
                }
                Thread.sleep(100);
            } catch (Exception ex) {
            }
        }
        ETLTaskTracker.getETLTaskTracker().removeTask(this);
        try {
            HttpClient httpClient = new DefaultHttpClient();
            HttpPost httppost = new HttpPost("http://192.168.111.129:7060/resources/dataetl/etltaskreport");

            InputStreamEntity reqEntity = new InputStreamEntity(new ByteArrayInputStream(("task exit|succeeded|" + processJobInstanceID + "|" + filePath).getBytes()), -1);
            reqEntity.setContentType("binary/octet-stream");
            reqEntity.setChunked(true);
            httppost.setEntity(reqEntity);
            HttpResponse response = httpClient.execute(httppost);
            httppost.releaseConnection();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void report(String operatorName, Map portMetrics) {
        synchronized (operatorReportStore) {
            operatorReportStore.put(operatorName, portMetrics);
        }
    }
}
