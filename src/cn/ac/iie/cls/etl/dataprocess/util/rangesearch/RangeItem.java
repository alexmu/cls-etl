/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.rangesearch;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author AlexMu
 */
public class RangeItem {

    long start;
    long end;
    Map<String,String> kvs = new HashMap<String,String>();

    public RangeItem(long pStart, long pEnd) {
        this.start = pStart;
        this.end = pEnd;
    }

    public void addValue(String k,String v) {
        kvs.put(k, v);
    }

    public void addValue(Map pMap) {
        kvs.putAll(pMap);
    }
}
