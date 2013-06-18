/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author alexmu
 */
public class Record {

    DataSet dataSet = null;
    List<String> fields = new ArrayList<String>();

    public void appendField(String pField) {
        fields.add(pField);
    }

    public List<String> getAllFields() {
        return this.fields;
    }

    public String getField(int pFieldIdx) {
        return fields.get(pFieldIdx);
    }

    public String getField(String pFieldName) {
        return fields.get(dataSet.getColumnIdx(pFieldName));
    }

    public void setField(String pFieldName, String pNewFieldValue) {
        fields.set(dataSet.getColumnIdx(pFieldName), pNewFieldValue);
    }
}
