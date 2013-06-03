/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess;

import cn.ac.iie.cls.etl.dataprocess.connector.Connector;
import cn.ac.iie.cls.etl.dataprocess.operator.outputoperator.AlmightyOutputOperator;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.CSVFileInputOperator;
import cn.ac.iie.cls.etl.dataprocess.operator.DataProcess;
import cn.ac.iie.cls.etl.dataprocess.operator.Operator;
import cn.ac.iie.cls.etl.dataprocess.operator.Port;
import cn.ac.iie.cls.etl.dataprocess.operator.filteroperator.RecordFilterOperator;
import cn.ac.iie.cls.etl.dataprocess.operator.inputoperator.XMLFileInputOperator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class DataProcessFactory {

    private static Map<String, Class> operatorClassSet = new HashMap<String, Class>() {
        {
            put("CSVFileInput", CSVFileInputOperator.class);
            put("XMLFileInput", XMLFileInputOperator.class);
            put("Filter", RecordFilterOperator.class);
            put("AlmightyOutput", AlmightyOutputOperator.class);
        }
    };

    public static void getAtomicDestPort(String pDestPort, List<String> pAtomicDestPortList, Map<String, List> pf2tPortPairSet, Map<String, Operator> pOperatorSet) throws Exception {
        String[] portInfo = pDestPort.split("\\.");
        Operator operator = pOperatorSet.get(portInfo[0]);
        if (!(operator instanceof DataProcess)) {
            pAtomicDestPortList.add(pDestPort);
        } else {
            List<String> toPortList = pf2tPortPairSet.get(pDestPort);
            if (toPortList == null) {
                throw new Exception("definition with " + pDestPort + " is incomplete");
            }
            for (String toPort : toPortList) {
                getAtomicDestPort(toPort, pAtomicDestPortList, pf2tPortPairSet, pOperatorSet);
            }
        }
    }

    public static DataProcess getDataProcess(Element pOperatorNode) throws Exception {
        Map<String, List> f2tPortPairSet = new HashMap<String, List>();
        Map<String, List> t2fPortPairSet = new HashMap<String, List>();
        Map<String, Operator> operatorSet = new HashMap<String, Operator>();
        DataProcess dataProcess = (DataProcess) parse(pOperatorNode, f2tPortPairSet, t2fPortPairSet, operatorSet);
        Iterator iter = f2tPortPairSet.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry<String, String>) iter.next();
            String fromPort = (String) entry.getKey();
            String[] fromPortInfo = fromPort.split("\\.");
            Operator fromOperator = operatorSet.get(fromPortInfo[0]);

            if (!(fromOperator instanceof DataProcess)) {
                System.out.println(fromOperator.getName());
                List<String> toPortList = (List<String>) entry.getValue();
                for (String toPort : toPortList) {
                    System.out.println(toPort);
                    List<String> tmpToPortList = new ArrayList<String>();
                    getAtomicDestPort(toPort, tmpToPortList, f2tPortPairSet, operatorSet);
                    for (String tmpToPort : tmpToPortList) {
                        String[] tmpToPortInfo = tmpToPort.split("\\.");
                        Operator tmpToOperator = operatorSet.get(tmpToPortInfo[0]);
                        Connector.createConnector(fromOperator.getPort(fromPortInfo[1]), tmpToOperator.getPort(tmpToPortInfo[1]));
                    }
                }
            }
        }

        iter = t2fPortPairSet.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry<String, String>) iter.next();
            String toPort = (String) entry.getKey();
            String[] toPortInfo = toPort.split("\\.");
            Operator toOperator = operatorSet.get(toPortInfo[0]);
            if (toOperator instanceof DataProcess) {
                List<String> fromPortList = (List<String>) entry.getValue();
                for (String fromPort : fromPortList) {
                    List<String> tmpFromPortList = new ArrayList<String>();
                    getAtomicDestPort(toPort, tmpFromPortList, t2fPortPairSet, operatorSet);
                    for (String tmpFromPort : tmpFromPortList) {
                        String[] tmpFromPortInfo = tmpFromPort.split("\\.");
                        Operator tmpFromOperator = operatorSet.get(tmpFromPortInfo[0]);
                        ((DataProcess) toOperator).addAtomicPort(toPortInfo[1], tmpFromOperator.getPort(tmpFromPortInfo[1]));
                    }
                }
            }
        }
        return dataProcess;
    }

    public static Operator parse(Element pOperatorNode, Map<String, List> pf2tPortPairSet, Map<String, List> pt2fPortPairSet, Map<String, Operator> pOperatorSet) throws Exception {
        String operatorName = pOperatorNode.attributeValue("name");
        String operatorClassName = pOperatorNode.attributeValue("class");
        Operator operator = null;
        if (operatorClassName.equals("Process")) {
            try {
                operator = new DataProcess();
                operator.init(operatorName, null);
                DataProcess dataProcess = (DataProcess) operator;
                Iterator nodeItor = pOperatorNode.elementIterator();
                while (nodeItor.hasNext()) {
                    Element node = (Element) nodeItor.next();
                    if (node.getName().equals("operator")) {
                        Operator subOperator = parse(node, pf2tPortPairSet, pt2fPortPairSet, pOperatorSet);
                        if (subOperator == null) {
                            System.out.println(node.attributeValue("name") + "********");
                        }
                        dataProcess.putSubOperator(subOperator);
                    } else if (node.getName().equals("connect")) {
                        String fromPort = node.attributeValue("from").replaceAll("parent", operatorName);
                        String toPort = node.attributeValue("to").replaceAll("parent", operatorName);

                        List<String> toPortList = pf2tPortPairSet.get(fromPort);
                        if (toPortList == null) {
                            toPortList = new ArrayList<String>();
                            pf2tPortPairSet.put(fromPort, toPortList);
                        }
                        toPortList.add(toPort);

                        List<String> fromPortList = pt2fPortPairSet.get(toPort);
                        if (fromPortList == null) {
                            fromPortList = new ArrayList<String>();
                            pt2fPortPairSet.put(toPort, fromPortList);
                        }
                        fromPortList.add(fromPort);
                    } else if (node.getName().equals("inport")) {
                        Port port = new Port(Port.INPUT, node.attributeValue("name"));
                        dataProcess.setupPort(port);
                    } else if (node.getName().equals("outport")) {
                        Port port = new Port(Port.OUTPUT, node.attributeValue("name"));
                        dataProcess.setupPort(port);
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            Class operatorClass = operatorClassSet.get(operatorClassName);
            if (operatorClass != null) {
                try {
                    operator = (Operator) operatorClass.newInstance();
                    operator.init(operatorName, null);
                } catch (Exception ex) {
                    throw new Exception("initializing operator " + operatorClassName + " is failed for", ex);
                }
            } else {
                throw new Exception("no such operator " + operatorClassName);
            }
        }
        pOperatorSet.put(operatorName, operator);
        return operator;
    }

    public static void main(String[] args) {
        File inputXml = new File("dataprocess-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
//                System.out.println(line);
                dataProcessDescriptor += line;
            }
            System.out.println(dataProcessDescriptor);
            Document document = DocumentHelper.parseText(dataProcessDescriptor);
            Element operatorNode = document.getRootElement();
            getDataProcess(operatorNode);
            System.out.println("parse ok");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
