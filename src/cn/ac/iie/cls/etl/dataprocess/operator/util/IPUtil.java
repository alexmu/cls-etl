/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.ac.iie.cls.etl.dataprocess.operator.util;

/**
 *
 * @author alexmu
 */
public class IPUtil {
    
    public static long IPV4Str2Long(String in) {

        if (in == null || in.equals("")) {
            return -1;
        }

        String str = in.trim();
        //   String str =strin;
        long[] ip = {0, 0, 0, 0};
        int i = 0; //char id
        int j = 0; //seg id
        int m = 1; //cur seg

        char c = str.charAt(0);
        if (c > '9' || c < '0') {
            return -1;
        }
        ip[0] = c - '0';

        for (i = 1; i < str.length(); i++) {
            c = str.charAt(i);

            if (c == '.') {
                if (m < 1 || ip[j] > 255) {
                    return -1;
                }
                j++;
                m = 0;
            } else if (c > '9' || c < '0') {
                return -1;
            } else {
                ip[j] = c - '0' + (ip[j] << 3) + (ip[j] << 1);
                m++;
            }
        }
        if (j < 3 || (j == 3 && m == 0)) {
            return -1;
        }

        long ipl = (ip[0] << 24) | (ip[1] << 16) | (ip[2] << 8) | ip[3];
        return ipl;
    }
    
    public static String IPV4Long2Str(String in) {

        if(in==null||in.equals("")){
            return null;
        }
        
        String ipLongStr = in;
        String ipStr = "";
        try {
            long ipLong = Long.parseLong(ipLongStr);
            long ip4 = ipLong & 0xff;
            long ip3 = (ipLong >> 8) & 0xff;
            long ip2 = (ipLong >> 16) & 0xff;
            long ip1 = (ipLong >> 24) & 0xff;
            ipStr = (String.valueOf(ip1) + "." + ip2 + "." + ip3 + "." + ip4);
        } catch (Exception ex) {
        }
        return ipStr;
    }
}
