/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.dataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author alexmu
 */
public class DataSet {

    public static final int VALID = 0;
    public static final int EOS = -1;
    private int status;
    private Map<String, Integer> fieldName2fieldIdx = new HashMap<String, Integer>();
    private List<Record> records = new ArrayList<Record>();

    public DataSet(int pStatus) {
        status = pStatus;
    }

    public void appendRecord(Record pRecord) {        
        this.records.add(pRecord);
        pRecord.dataSet = this;
    }

    public List<Record> getAllRecords() {
        return this.records;
    }

    public Record getRecord(int pRecordIdx) {
        return this.records.get(pRecordIdx);
    }

    public int size() {
        return this.records.size();
    }

    public int getStatus() {
        return status;
    }

    public boolean isValid() {
        return status == DataSet.VALID;
    }

    public int getColumnIdx(String pFieldName) {
        return fieldName2fieldIdx.get(pFieldName);
    }

    public void putFieldName2Idx(String pColumnName, int pColumnIdx) {
        fieldName2fieldIdx.put(pColumnName, pColumnIdx);
    }
}
