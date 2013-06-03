/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.taskmanage;

import cn.ac.iie.cls.etl.dataprocess.DataProcessFactory;
import cn.ac.iie.cls.etl.dataprocess.operator.DataProcess;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

/**
 *
 * @author alexmu
 */
public class TaskTracker {

    public static void main(String[] args) {
        File inputXml = new File("dataprocess-specific.xml");
        try {
            String dataProcessDescriptor = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputXml)));
            String line = null;
            while ((line = br.readLine()) != null) {
                dataProcessDescriptor += line;
            }
            System.out.println(dataProcessDescriptor);
            Document document = DocumentHelper.parseText(dataProcessDescriptor);
            Element operatorNode = document.getRootElement();
            DataProcess dataProcess = DataProcessFactory.getDataProcess(operatorNode);
            System.out.println("parse ok");
            Thread taskManager = new Thread(new Task(dataProcess));
            taskManager.start();
//            DataProcess dataProces1 = DataProcessFactory.getDataProcess(operatorNode);
//            System.out.println("parse ok");
//            Thread t1 = new Thread(dataProces1);
//            t1.start();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
