package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt;

import com.hikvision.artemis.sdk.ArtemisHttpUtil;
import com.hikvision.artemis.sdk.config.ArtemisConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.util.EncryptUtil;
import org.apache.seatunnel.connectors.seatunnel.http.util.Md5Util;
import org.apache.seatunnel.connectors.seatunnel.http.util.TimeUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.springframework.util.DigestUtils;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;
import static org.apache.seatunnel.connectors.seatunnel.http.util.SingUtil.sign;

@Slf4j
public class EncryptRequest {



    /**
     * 拦截http请求参数，做MD5加密处理
     *
     * @param params
     * @return Map<String, String>
     */
    public static Map<String, String> SignEncryptRequestParam(Map<String, String> params) throws Exception {
        if (null == params) {
            return params;
        }
        Map<String, String> encryptParams = new HashMap<>(params);

        Iterator<Map.Entry<String, String>> iterator = encryptParams.entrySet().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        String valueKeys = "";
        String signKey = "sign";

        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.contains("MD5")) {
                signKey = key.split("\\->")[0];
                valueKeys = value;
                String[] keys = valueKeys.split("\\+");
                Arrays.sort(keys);

                for (String k : keys) {
                    if (encryptParams.containsKey(k)) {
                        stringBuilder.append(k).append("=").append(encryptParams.get(k)).append("&");
                    }
                }

                iterator.remove();
                if (stringBuilder.length() > 0) {
                    stringBuilder.setLength(stringBuilder.length() - 1);
                }

                stringBuilder.append("d5e1ac45e196e9c00fd5655fe3deed9b");
            }
        }

        String result = stringBuilder.toString();

        if (!result.isEmpty()) {
            String preMd5 = result.toUpperCase();
            try {
                String sign = DigestUtils.md5DigestAsHex(preMd5.getBytes("UTF-8"));
                encryptParams.put(signKey, sign);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new SeaTunnelException("加密Encoding只支持UTF-8");
            }
        }

        return encryptParams;

    }

    public static Map<String, String> encryptRequestParam2(Map<String, String> params, Map<String, String> paramsEncrypt) throws Exception {
        params = SignEncryptRequestParam(params);
        if (MapUtils.isEmpty(paramsEncrypt) || MapUtils.isEmpty(params) ){
            return params;
        }
        String encryptType = paramsEncrypt.get("encrypt_type");
        if ("RSA".equals(encryptType)){
            String account = params.get("account").toString();
            String RSAData = paramsEncrypt.get("RSAData").toString();
            String privateKey = paramsEncrypt.get("privateKey").toString();
            String publicKey = paramsEncrypt.get("publicKey").toString();

            if (StringUtils.isBlank(account) || StringUtils.isBlank(RSAData) || StringUtils.isBlank(privateKey) || StringUtils.isBlank(publicKey)){
                throw new SeaTunnelException("account, RSAData, privateKey, publicKey 不能为空");
            }
            String RSAData2 = EncryptUtil.encryptByPublicKey(RSAData, publicKey);
            String sign = EncryptUtil.signWithMD5("account"+account+"data"+RSAData2);
            params.put(SIGN, sign);
            params.put("data", RSAData2);
        }

        return params;
    }

    /**
     * 加密headers,做sha256加密
     *
     * @param headers
     * @param headerEncrypt
     * @param body
     * @return
     * @throws Exception
     */
    public static Map<String, String> encryptRequestHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws Exception {
        if (MapUtils.isEmpty(headers) || MapUtils.isEmpty(headerEncrypt)) {
            return headers;
        }
        Map<String, String> encryptHeaders = new HashMap<>(headers);
        String encryptType = headerEncrypt.get("encrypt_type");
        if ("SHA256".equals(encryptType)) {
            String accountKey = headerEncrypt.get("accountKey");
            String secretKey = headerEncrypt.get("secretKey");
            if (StringUtils.isBlank(accountKey) || StringUtils.isBlank(secretKey)) {
                throw new SeaTunnelException("accountKey or secretKey is null");
            }
            String jsonBody = StringUtils.isBlank(body) ? "" : body;
            String requestTime = headers.get("requestTime");
            LocalDateTime localDateTime = TimeUtils.parseDateTime(requestTime, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
            long requestTimeStamp = TimeUtils.toEpochMill(localDateTime);
            String signatureBefore = accountKey + secretKey + requestTimeStamp + jsonBody;
            String signature = org.apache.commons.codec.digest.DigestUtils.sha256Hex(signatureBefore);
            encryptHeaders.put("signature", signature);
            encryptHeaders.put("requestTime", requestTimeStamp + "");
            encryptHeaders.put("account", accountKey);
        }

        return encryptHeaders;
    }

    /**
     * sgin-md5加密
     *
     * @param body
     * @param bodyEncrypt
     * @return
     */

    public static String encryptRequestBody(String body,Map<String, String> bodyEncrypt, Map<String, String> params) {
        if (StringUtils.isBlank(body) || MapUtils.isEmpty(bodyEncrypt)) {
            return body;
        }
        String encryptType = bodyEncrypt.get("encrypt_type");
        Map<String, Object> bodyMap = JsonUtils.parseObject(body, new TypeReference<Map<String, Object>>(){});
        if ("SIGN-MD5".equals(encryptType)) {
            String secretKey = bodyEncrypt.get("secretKey");
            if (StringUtils.isNotBlank(secretKey)) {
                String ts = bodyMap.get(TS).toString();
                String cid = bodyMap.get(CID).toString();
                LocalDateTime localDateTime = TimeUtils.parseDateTime(ts, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
                LocalDateTime localDateTimeCid = TimeUtils.parseDateTime(cid, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
                long tsStamp = TimeUtils.toEpochMill(localDateTime);
                long cidStamp = TimeUtils.toEpochMill(localDateTimeCid);
                bodyMap.put(TS, tsStamp + "");
                bodyMap.put(CID, "cid_"+ cidStamp);
                String sign = sign(bodyMap, secretKey);
                bodyMap.put(SIGN, sign);
            }
            else {
                throw new SeaTunnelException("无法找到secretKey");
            }
        }else if ("URL-MD5".equals(encryptType)) {
            String bodyStr = JsonUtils.toJsonString(bodyMap);
            String encryptKey = bodyEncrypt.get("encrypt_key");
            if (StringUtils.isNotBlank(encryptKey)) {
                String sign = Md5Util.md5Upper(bodyStr + encryptKey);
                params.put(SIGN, sign);
            } else {
                throw new SeaTunnelException("无法找到encrypt_key");
            }
            //base64
        }else if ("BASE64".equals(encryptType)) {
            Map<String, Object> base64Map = new HashMap<>();
            String base64Str = Base64.getEncoder().encodeToString(body.getBytes());
            base64Map.put("params", base64Str);
            //秒级时间戳
            base64Map.put("timestamp", System.currentTimeMillis()/1000);
            return JsonUtils.toJsonString(base64Map);
        } else if ("Digital-Twin-MD5".equals(encryptType)) {
            String appKey = bodyMap.get("appKey").toString();
            String appSecret= bodyMap.get("appSecret").toString();
            long timeMillis = System.currentTimeMillis();
            long ts = (timeMillis / 1000) - 70;
            String signature = Md5Util.md5Lower("appKey=" + appKey + "&appSecret=" + appSecret + "&timestamp=" + ts);
            bodyMap.put("signature", signature);
            bodyMap.put("timestamp", ts);
        }
        return JsonUtils.toJsonString(bodyMap);
    }





    /**
     * 拦截海康sdk
     */
    public static HttpResponse handleHikvisionApi(String uri, String body, Map<String, String> querys, String accept, String contentType, Map<String, String> header, Map<String, String> hikvisionApi)  {
        //提取出host和path
        URL url = null;
        Map<String, String> path;
        try {
            url = new URL(uri);
            String hostStr = url.getHost();
            int port = url.getPort();
            String host = port == -1 ? hostStr : hostStr + ":" + port;
            String pathStr = url.getPath();
            String protocol = url.getProtocol();
            log.info("HikvisionApi的host:{},path:{},protocol:{}",host,pathStr,protocol);
            path = new HashMap<String, String>(2) {
                {
                    put(protocol+"://", pathStr);
                }
            };
            ArtemisConfig.host = host;
            ArtemisConfig.appKey = hikvisionApi.get("app_key");
            ArtemisConfig.appSecret = hikvisionApi.get("app_secret");
            log.info("HikvisionApi的appKey:{},appSecret:{}",ArtemisConfig.appKey,ArtemisConfig.appSecret);
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("url格式错误",e);
        } catch (Exception e) {
            throw new SeaTunnelException("提取url的host、path、protocol失败",e);
        }
        String result = ArtemisHttpUtil.doPostStringArtemis(path, body, querys, null, contentType, header);
        //将result封装成httpResponse
        JsonNode resultNode = JsonUtils.parseObject(result);
        HttpResponse httpResponse = new HttpResponse();
        JsonNode msg = resultNode.get("msg");
        if (msg != null) {
            if ("success".equals(msg.asText())) {
                httpResponse.setCode(200);
            }
        }
        httpResponse.setContent(result);
        return httpResponse;
    }
}
