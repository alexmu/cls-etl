/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.rangesearch;

import java.util.List;
import java.util.Map;

/**
 *
 * @author AlexMu
 */
public class ListItem {

    RangeItem ri;
    ListItem next;

    public ListItem(long pStart, long pEnd, String k, String v, ListItem next) {
        this(pStart, pEnd, k, v);
        setNext(next);
    }

    public ListItem(long pStart, long pEnd, Map kvs, ListItem next) {
        this(pStart, pEnd, kvs);
        setNext(next);
    }

    public ListItem(long pStart, long pEnd, String k, String v) {
        this(pStart, pEnd);
        addValue(k, v);
    }

    public ListItem(long pStart, long pEnd, Map kvs) {
        this(pStart, pEnd);
        addValue(kvs);
    }

    public ListItem(long pStart, long pEnd) {
        ri = new RangeItem(pStart, pEnd);
        next = null;
    }

    public void setNext(ListItem pNext) {
        this.next = pNext;
    }

    public void addValue(String k, String v) {
        this.ri.addValue(k, v);
    }

    public void addValue(Map kvs) {
        this.ri.addValue(kvs);
    }
}
