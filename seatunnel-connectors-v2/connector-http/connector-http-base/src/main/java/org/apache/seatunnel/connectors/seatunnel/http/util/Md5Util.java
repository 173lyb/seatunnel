package org.apache.seatunnel.connectors.seatunnel.http.util;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * @author Kevin Huang
 * <p>
 * 2019年01月16日 11:58:00
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
}
