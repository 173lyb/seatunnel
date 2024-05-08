package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.Factory.EncryptStrategyFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.util.DigestUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.ENCRYPT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptRequest.SignEncryptRequestParam;

public class EncryptHandler {
    private final EncryptStrategyFactory factory;

    public EncryptHandler(EncryptStrategyFactory factory) {
        this.factory = factory;
    }
    public  Map<String, String> SignEncryptRequestParam(Map<String, String> params) throws Exception {
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
    public String handleBody(String body, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException {
        if (StringUtils.isBlank(body) || MapUtils.isEmpty(bodyEncrypt)) {
            return body;
        }
        String encryptType = bodyEncrypt.get(ENCRYPT_TYPE);
        Map<String, Object> bodyMap = JsonUtils.parseObject(body, new TypeReference<Map<String, Object>>(){});
        EncryptStrategy strategy = factory.create(encryptType);
        if (strategy != null) {
            strategy.encryptBody(body,bodyMap, bodyEncrypt,params);
            return JsonUtils.toJsonString(bodyMap);
        } else {
            throw new IllegalArgumentException("未知的加密类型: " + encryptType);}
    }

    //handleParams
    public Map<String, String> handleParams( Map<String, String> params, Map<String, String> paramsEncrypt) throws Exception {
        params = SignEncryptRequestParam(params);
        if (MapUtils.isEmpty(paramsEncrypt) || MapUtils.isEmpty(params) ){
            return params;
        }
        String encryptType = paramsEncrypt.get(ENCRYPT_TYPE);
        EncryptStrategy strategy = factory.create(encryptType);
        if (strategy != null) {
            Map<String, String> encryptParams = strategy.encryptParam(params, paramsEncrypt);
            return encryptParams;
        } else {
            throw new IllegalArgumentException("未知的加密类型: " + encryptType);
        }
    }

    //handleHeader
    public Map<String, String> handleHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws Exception {
        if (MapUtils.isEmpty(headers) || MapUtils.isEmpty(headerEncrypt)) {
            return headers;
        }
        String encryptType = headerEncrypt.get(ENCRYPT_TYPE);
        EncryptStrategy strategy = factory.create(encryptType);
        if (strategy != null) {
            Map<String, String> encryptHeader = strategy.encryptHeader(headers, headerEncrypt, body);
            return encryptHeader;
        }else {
            throw new IllegalArgumentException("未知的加密类型: " + encryptType);
        }
    }
}



