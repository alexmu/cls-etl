/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.inputoperator;

import java.util.Comparator;

/**
 *
 * @author alexmu
 */
public class Column implements Comparable {

    public static final int STRING_TYPE = 0;
    public static final int INTEGER_TYPE = 1;
    public static final int FLOAT_TYPE = 2;
    public static final int BOOLEAN_TYPE = 3;
    public static final int TIME_STAMP_TYPE = 4;
    String columnName;
    int columnIdx;
    int columnType;

    public Column(String pColumnName, int pColumnIdx, int pColumnType) {
        columnName = pColumnName;
        columnIdx = pColumnIdx;
        columnType = pColumnType;
    }

    public static int parseType(String pColumnType) throws Exception {
        if (pColumnType.equals("STRING")) {
            return STRING_TYPE;
        } else if (pColumnType.equals("INTEGER")) {
            return INTEGER_TYPE;
        } else if (pColumnType.equals("FLOAT")) {
            return FLOAT_TYPE;
        } else if (pColumnType.equals("BOOLEAN")) {
            return BOOLEAN_TYPE;
        } else if (pColumnType.equals("TIMESTAMP")) {
            return BOOLEAN_TYPE;
        } else {
            throw new Exception("not supported type " + pColumnType);
        }
    }

    @Override
    public int compareTo(Object o) {
        Column anotherColumn = (Column) o;
        if (this.columnIdx > anotherColumn.columnIdx) {
            return 1;
        } else if (this.columnIdx == anotherColumn.columnIdx) {
            return 0;
        } else {
            return -1;
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public int getColumnIdx() {
        return columnIdx;
    }

    public int getColumnType() {
        return columnType;
    }
}
