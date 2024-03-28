package org.apache.seatunnel.connectors.seatunnel.http;

import com.alibaba.fastjson.JSON;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.seatunnel.common.utils.JsonUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @ClassName FuniRestTemplate
 * @Description 接口请求示例
 * @Author YangFeng
 * @Date 2022/9/15 22:47
 * @Version 1.0
 **/
@Slf4j
public class FuniRestTemplate {
    public static void main(String[] args) {
        String URL="http://10.182.1.26/CCSRegistryCenter/rest";
        String loginid="lqxzsp";//分配的账号
        String password="lqxzsp@12345";//分配的密码
        String token="lqxzsp2024token";//分配的token

        String random = String.valueOf(new Random().nextInt());
        String nowTimestamp = DateFormatUtils.format(new Date(),"yyyyMMddHHmmss");

        Map<String,Object> requestParams=new HashMap<>();
        requestParams.put("apiKey",1524);
        requestParams.put("nonce",random);
        requestParams.put("regioncode","510100");

        // 使用Java 8内置Base64进行编码
        String authInfo = loginid + ":" + password + ":" + nowTimestamp;
        byte[] authInfoBytes = authInfo.getBytes(StandardCharsets.UTF_8);
        String encodedAuthenticationKey = Base64.getEncoder().encodeToString(authInfoBytes);

//        requestParams.put("authenticationKey",new Base64Encoder().encode((loginid + ":" + password + ":" + nowTimestamp).getBytes()));
        requestParams.put("authenticationKey",encodedAuthenticationKey);
        requestParams.put("signature",getSignature(nowTimestamp, random,token));

        //String dataStr="";
        //Map<String,Object> data=JSON.parseObject(dataStr,Map.class);
        Map<String,Object> data=new HashMap<>();
        requestParams.put("data",data);


        log.info(JSON.toJSONString(requestParams));
        String data2 = HttpRequest.post(URL, JsonUtils.toJsonString(requestParams));
//        RestTemplate restTemplate = new RestTemplate(FuniRestTemplate.generateHttpRequestFactory());
//        JSONObject jsonObject = restTemplate.postForObject(URL, requestParams, JSONObject.class);
//        log.info(jsonObject.toString());
        System.out.println(data2);
    }


    /**
     * 获取Signature
     * @param timestamp
     * @param nonce
     * @param token
     * @return java.lang.String
     * @date 15:08 2023/10/23
     * @author YangFeng
     **/
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

    /**
     * 忽略https
     * @return org.springframework.http.client.HttpComponentsClientHttpRequestFactory
     * @date 15:07 2023/10/23
     * @author YangFeng
     **/
//    public static HttpComponentsClientHttpRequestFactory generateHttpRequestFactory() throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
//        TrustStrategy acceptingTrustStrategy = (x509Certificates, authType) -> true;
//        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
//        SSLConnectionSocketFactory connectionSocketFactory = new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());
//
//        HttpClientBuilder httpClientBuilder = HttpClients.custom();
//        httpClientBuilder.setSSLSocketFactory(connectionSocketFactory);
//        CloseableHttpClient httpClient = httpClientBuilder.build();
//        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
//        factory.setHttpClient(httpClient);
//        return factory;
//    }
}