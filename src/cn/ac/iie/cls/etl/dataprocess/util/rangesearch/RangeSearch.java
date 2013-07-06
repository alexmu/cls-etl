/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.util.rangesearch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author AlexMu
 */
public class RangeSearch<T> {

    ListItem listHead = null;
    List<RangeItem> rangesArray = new ArrayList<RangeItem>();

    public void append(long pStart, long pEnd, String k, String v) {
        if (listHead == null) {
            listHead = new ListItem(pStart, pEnd, k, v);
        } else {
            ListItem itemTmpp, itemp1, itemp2, newItemp;
            itemTmpp = itemp1 = listHead;
            while (pStart > itemp1.ri.start) {
                itemTmpp = itemp1;
                itemp1 = itemp1.next; //move next
                if (itemp1 == null) {
                    break;
                }
            }

            if (itemp1 == null) {
                if (pStart < itemTmpp.ri.end) {
                    if (pEnd < itemTmpp.ri.end) {
                        newItemp = new ListItem(pStart, pEnd, itemTmpp.ri.kvs, new ListItem(pEnd + 1, itemTmpp.ri.end, itemTmpp.ri.kvs));
                        newItemp.addValue(k, v);
                        itemTmpp.ri.end = pStart - 1;
                    } else if (pEnd == itemTmpp.ri.end) {
                        newItemp = new ListItem(pStart, pEnd, itemTmpp.ri.kvs);
                        newItemp.addValue(k, v);
                        itemTmpp.ri.end = pStart - 1;
                    } else {
                        newItemp = new ListItem(pStart, itemTmpp.ri.end, itemTmpp.ri.kvs, new ListItem(itemTmpp.ri.end + 1, pEnd, k, v));
                        newItemp.addValue(k, v);
                        itemTmpp.ri.end = pStart - 1;
                    }
                } else if (pStart == itemTmpp.ri.end) {
                    newItemp = new ListItem(itemTmpp.ri.end, itemTmpp.ri.end, itemTmpp.ri.kvs, new ListItem(itemTmpp.ri.end + 1, pEnd, k, v));
                    newItemp.addValue(k, v);
                    itemTmpp.ri.end = pStart - 1;
                } else {
                    newItemp = new ListItem(pStart, pEnd, k, v);
                }
                itemTmpp.next = newItemp;
                return;
            } else {
                if (itemp1 == listHead) {
                    if (pStart < itemp1.ri.start) {
                        if (pEnd < itemp1.ri.start) {
                            listHead = new ListItem(pStart, pEnd, k, v, itemp1);
                            return;
                        } else if (pEnd == itemp1.ri.start) {
                            newItemp = new ListItem(itemp1.ri.start, itemp1.ri.start, itemp1.ri.kvs, itemp1);
                            newItemp.addValue(k, v);
                            itemp1.ri.start++;
                            listHead = new ListItem(pStart, pEnd - 1, k, v, newItemp);
                            return;
                        } else {
                            listHead = new ListItem(pStart, itemp1.ri.start - 1, k, v, itemp1);
                        }
                    }
                } else {
                    if (pStart < itemp1.ri.start) {
                        if (pStart < itemTmpp.ri.end) {
                            if (pEnd < itemTmpp.ri.end) {
                                newItemp = new ListItem(pEnd + 1, itemTmpp.ri.end, itemTmpp.ri.kvs, itemp1);
                                newItemp = new ListItem(pStart, pEnd, itemTmpp.ri.kvs, newItemp);
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else if (pEnd == itemTmpp.ri.end) {
                                newItemp = new ListItem(pStart, itemTmpp.ri.end, itemTmpp.ri.kvs, itemp1);
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else if (pEnd > itemTmpp.ri.end && pEnd < itemp1.ri.start) {
                                newItemp = new ListItem(pStart, itemTmpp.ri.end, itemTmpp.ri.kvs, new ListItem(itemTmpp.ri.end + 1, pEnd, k, v, itemp1));
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else if (pEnd == itemp1.ri.start) {
                                newItemp = new ListItem(itemp1.ri.start, itemp1.ri.start, itemp1.ri.kvs, itemp1);
                                newItemp.addValue(k, v);
                                itemp1.ri.start++;
                                newItemp = new ListItem(itemTmpp.ri.end + 1, newItemp.ri.start - 1, k, v, newItemp);
                                newItemp = new ListItem(pStart, itemTmpp.ri.end, itemTmpp.ri.kvs, newItemp);
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else {
                                newItemp = new ListItem(itemTmpp.ri.end + 1, itemp1.ri.start - 1, k, v, itemp1);
                                newItemp = new ListItem(pStart, itemTmpp.ri.end, itemTmpp.ri.kvs, newItemp);
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                            }
                        } else if (pStart == itemTmpp.ri.end) {
                            if (pEnd == itemTmpp.ri.end) {
                                newItemp = new ListItem(itemTmpp.ri.end, itemTmpp.ri.end, itemTmpp.ri.kvs, itemp1);
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else if (pEnd > itemTmpp.ri.end && pEnd < itemp1.ri.start) {
                                newItemp = new ListItem(itemTmpp.ri.end, itemTmpp.ri.end, itemTmpp.ri.kvs, new ListItem(itemTmpp.ri.end + 1, pEnd, k, v, itemp1));
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else if (pEnd == itemp1.ri.start) {
                                newItemp = new ListItem(itemp1.ri.start, itemp1.ri.start, itemp1.ri.kvs, itemp1);
                                newItemp.addValue(k, v);
                                itemp1.ri.start++;
                                newItemp = new ListItem(itemTmpp.ri.end, itemTmpp.ri.end, itemTmpp.ri.kvs, new ListItem(itemTmpp.ri.end + 1, pEnd - 1, k, v, newItemp));
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                                return;
                            } else {
                                newItemp = new ListItem(itemTmpp.ri.end, itemTmpp.ri.end, itemTmpp.ri.kvs, new ListItem(itemTmpp.ri.end + 1, itemp1.ri.start - 1, k, v, itemp1));
                                newItemp.addValue(k, v);
                                itemTmpp.ri.end = pStart - 1;
                                itemTmpp.next = newItemp;
                            }
                        } else { //pStart> itemTmpp.ri.end
                            if (pEnd < itemp1.ri.start) {
                                if (itemp1 == listHead) {
                                    listHead = new ListItem(pStart, pEnd, k, v, listHead);
                                } else {
                                    itemTmpp.next = new ListItem(pStart, pEnd, k, v, itemp1);
                                }
                                return;
                            } else if (pEnd == itemp1.ri.start) {
                                newItemp = new ListItem(itemp1.ri.start, itemp1.ri.start, itemp1.ri.kvs, itemp1);
                                newItemp.addValue(k, v);
                                itemp1.ri.start++;
                                if (itemp1 == listHead) {
                                    listHead = new ListItem(pStart, pEnd - 1, k, v, newItemp);
                                } else {
                                    itemTmpp.next = new ListItem(pStart, pEnd - 1, k, v, newItemp);
                                }
                                return;
                            } else { //pEnd > itemp1.ri.Start
                                if (itemp1 == listHead) {
                                    listHead = new ListItem(pStart, itemp1.ri.start - 1, k, v, itemp1);
                                } else {
                                    itemTmpp.next = new ListItem(pStart, itemp1.ri.start - 1, k, v, itemp1);
                                }
                            }
                        }
                    } else if (pStart == itemp1.ri.start) {
                        if (pEnd == itemp1.ri.start) {
                            if (itemp1.ri.start == itemp1.ri.end) {
                                itemp1.addValue(k, v);
                            } else {
                                newItemp = new ListItem(itemp1.ri.start, itemp1.ri.start, itemp1.ri.kvs, itemp1);
                                newItemp.addValue(k, v);
                                itemp1.ri.start++;
                                if (itemp1 == listHead) {
                                    listHead = newItemp;
                                } else {
                                    itemTmpp.next = newItemp;
                                }
                            }
                            return;
                        }
                    }
                }//end of else itemp1!=listHead

                itemTmpp = itemp2 = itemp1; //begin with itemp1

                while (pEnd > itemp2.ri.end) {
                    itemTmpp = itemp2;
                    itemp2 = itemp2.next;
                    if (itemp2 == null) {
                        break;
                    }
                }

                if (itemp2 == null) {
                    itemTmpp.next = new ListItem(itemTmpp.ri.end + 1, pEnd, k, v);
                    itemp2 = itemTmpp.next;
                } else {
                    if (itemp2 == itemp1) {
                        if (pEnd < itemp2.ri.end) {
                            itemp2.next = new ListItem(pEnd + 1, itemp2.ri.end, itemp2.ri.kvs, itemp2.next);
                            itemp2.ri.end = pEnd;
                            itemp2.addValue(k, v);
                        } else {
                            itemp2.addValue(k, v);
                        }
                    } else {
                        if (pEnd < itemp2.ri.start) {
                            itemTmpp.next = new ListItem(itemTmpp.ri.end + 1, pEnd, k, v, itemp2);
                        } else if (pEnd == itemp2.ri.start) {
                            newItemp = new ListItem(itemp2.ri.start, itemp2.ri.start, itemp2.ri.kvs, itemp2);
                            newItemp.addValue(k, v);
                            itemp2.ri.start++;
                            if (pEnd - itemTmpp.ri.end > 1) {
                                itemTmpp.next = new ListItem(itemTmpp.ri.end + 1, pEnd - 1, k, v, newItemp);
                            } else {
                                itemTmpp.next = newItemp;
                            }
                        } else {
                            if (pEnd < itemp2.ri.end) {
                                newItemp = new ListItem(itemp2.ri.start, pEnd, itemp2.ri.kvs, itemp2);
                                newItemp.addValue(k, v);
                                itemp2.ri.start = pEnd + 1;
                                if (newItemp.ri.start - itemTmpp.ri.end > 1) {
                                    itemTmpp.next = new ListItem(itemTmpp.ri.end + 1, newItemp.ri.start - 1, k, v, newItemp);
                                } else {
                                    itemTmpp.next = newItemp;
                                }
                            } else {
                                itemp2.addValue(k, v);
                            }
                        }
                        itemp2 = itemTmpp.next;
                    }
                }

                while (true) {
                    if (itemp1 == itemp2) {
                        break;
                    } else {
                        itemp1.addValue(k, v);
                        if (itemp1.next.ri.start - itemp1.ri.end > 1) {
                            itemp1.next = new ListItem(itemp1.ri.end + 1, itemp1.next.ri.start - 1, k, v, itemp1.next);
                            itemp1 = itemp1.next.next;
                        } else {
                            itemp1 = itemp1.next;
                        }
                    }
                }//while true
            }
        }
    }

    public void contructArray() {
        ListItem itemp = listHead;
        while (itemp != null) {
            rangesArray.add(itemp.ri);
            itemp = itemp.next;
        }
    }

    public RangeItem getRangeItem(long p) {
        RangeItem ri;
        int start = 0;
        int end = rangesArray.size() - 1;
        int mid;

        while (start <= end) {
            mid = (start + end) / 2;
            ri = rangesArray.get(mid);
            if (p >= ri.start && p <= ri.end) {
                return ri;
            } else if (p > ri.end) {
                start = mid + 1;
            } else if (p < ri.start) {
                end = mid - 1;
            }
        }

        return null;
    }

    public String getValue(long p, Object key) {
        RangeItem ri = getRangeItem(p);
        if (ri == null) {
            return null;
        } else {
            return ri.kvs.get(key);
        }
    }

    public List getValues(long p) {
        RangeItem ri = getRangeItem(p);
        if (ri == null) {
            return null;
        } else {
            List<String> list = new ArrayList<String>();
            Set vs = ri.kvs.keySet();
            Iterator itr = vs.iterator();
            while (itr.hasNext()) {
                list.add(ri.kvs.get(itr.next()));
            }
            return list;
        }
    }

    public void printList() {
        ListItem itemp = listHead;
        while (itemp != null) {
            System.out.print(itemp.ri.start + "," + itemp.ri.end + ":");
            Map kvs = itemp.ri.kvs;
            Set vs = kvs.keySet();
            Iterator itr = vs.iterator();
            while (itr.hasNext()) {
                String key = (String) itr.next();
                System.out.print(kvs.get(key) + ",");
            }
            System.out.println();
            itemp = itemp.next;
        }

        System.out.println("***************");
    }
}
