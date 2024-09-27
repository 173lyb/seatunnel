package org.apache.seatunnel.connectors.seatunnel.http.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import java.security.MessageDigest;

/**
 * @author Kevin Huang
 *     <p>2019年01月16日 11:58:00
 */
public class Md5Util {
    /**
     * 加密(MD5)
     *
     * @param data 哈希数据
     * @return 返回加密字串（小写）
     */
    public static String md5Lower(String data) {
        return DigestUtils.md5Hex(data);
    }

    /**
     * 加密(MD5)
     *
     * @param data 哈希数据
     * @return 返回加密字串（大写）
     */
    public static String md5Upper(String data) {
        return md5Lower(data).toUpperCase();
    }

    public static String md5(String src, String salt) {
        String toEncode = src + ((salt == null) ? "" : ("{" + salt + "}"));
        char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'a', 'b', 'c', 'd', 'e', 'f'};
        try {
            byte[] btInput = toEncode.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(btInput);
            byte[] md = mdInst.digest();
            int j = md.length;
            char[] str = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            throw new SeaTunnelException("x-token-md5 加密失败", e);
        }
    }
    public static String md5(String s) {
        return md5(s, null);
    }
}
