package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class BodySignBase64Strategy implements EncryptStrategy {

    @Override
    public void encryptBody(String body ,Map<String, Object> bodyMap, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException {
        Map<String, Object> base64Map = new HashMap<>();
        String random = String.valueOf(new Random().nextInt());
        bodyMap.put(NONCE, random);
        String loginid = bodyEncrypt.get(LOGIN_ID);
        String password = bodyEncrypt.get(PASSWORD);
        String token = bodyEncrypt.get(TOKEN);
        //判空
        if (loginid == null || password == null || token == null) {
            throw new SeaTunnelException("loginid,password,token can not be null");
        }
        String nowTimestamp = DateFormatUtils.format(new Date(),"yyyyMMddHHmmss");
        String authInfo = loginid + ":" + password + ":" + nowTimestamp;

        // 使用Java 8内置Base64进行编码
        byte[] authInfoBytes = authInfo.getBytes(StandardCharsets.UTF_8);
        String encodedAuthenticationKey = Base64.getEncoder().encodeToString(authInfoBytes);
        bodyMap.put("authenticationKey",encodedAuthenticationKey);
        bodyMap.put("signature",getSignature(nowTimestamp, random,token));
        //清空bodyMap,转化为base64Map
    }

    @Override
    public void encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {

    }

    @Override
    public void encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {

    }
    public static String getSignature(String timestamp, String nonce, String token) {
        // 1. 将token、timestamp、nonce三个参数进行字典序排序
        String[] arrTmp = {token, timestamp, nonce};
        Arrays.sort(arrTmp);
        StringBuilder sb = new StringBuilder();
        // 2.将三个参数字符串拼接成一个字符串进行sha1加密
        for (int i = 0; i < arrTmp.length; i++) {
            sb.append(arrTmp[i]);
        }
        String expectedSignature = encrypt(sb.toString());
        return expectedSignature;
    }
    /**
     * 将字节数组转换成16进制字符串
     * @return
     */
    private static String encrypt(String strSrc) {
        String strDes = null;
        byte[] bt = strSrc.getBytes();
        MessageDigest digest= null;
        try {
            digest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        digest.update(bt);
        strDes = byte2hex(digest.digest());
        return strDes;
    }

    /**
     * 将字节数组转换成16进制字符串
     */
    private static String byte2hex(byte[] b) {
        StringBuilder sbDes = new StringBuilder();
        String tmp = null;
        for (int i = 0; i < b.length; i++) {
            tmp = (Integer.toHexString(b[i] & 0xFF));
            if (tmp.length() == 1) {
                sbDes.append("0");
            }
            sbDes.append(tmp);
        }
        return sbDes.toString();
    }
}
