package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.EncryptUtil;
import org.apache.seatunnel.connectors.seatunnel.http.util.SecurityUtil;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class ParamsSHA256withRSAStrategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt, Map<String, String> params) throws SeaTunnelException {

    }

    @Override
    public Map<String, String> encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {
        return headers;
    }

    @Override
    public Map<String, String> encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws Exception {
        Map<String, String> encryptParams = JsonUtils.toMap(JsonUtils.toJsonString(params));
        String biz_content = params.get(BIZ_CONTENT);
        String access_key = params.get(ACCESS_KEY);
        String privateKey = paramsEncrypt.get(PRIVATE_KEY);
        String decrypt_biz_content = paramsEncrypt.get(ENCRYPT_BIZ_CONTENT);


        if (StringUtils.isBlank(privateKey) || StringUtils.isBlank(biz_content) || StringUtils.isBlank(access_key) || StringUtils.isBlank(decrypt_biz_content)){
            throw new SeaTunnelException("privateKey, biz_content ,access_key, encrypt_biz_content 不能为空");
        }
        if (Boolean.parseBoolean(decrypt_biz_content)){
            biz_content = SecurityUtil.encrypt(biz_content, access_key);
        }
        //流水号
        String request_id = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 20);
        //请求时间戳
        String timestamp = String.valueOf(System.currentTimeMillis());

        encryptParams.put(BIZ_CONTENT, biz_content);
        encryptParams.put(REQUEST_ID, request_id);
        encryptParams.put(TIMESTAMP, timestamp);
        String source = generateSignSource(encryptParams);
        String sign = sign2(source, privateKey);
        encryptParams.put(SIGN, sign);

        return encryptParams;
    }

    private  String sign2(String source, String keystore) throws Exception {

        PrivateKey privateKey = null;
        String sign = null;
        // Base64解码私钥数据
        byte[] encodedKey = Base64.getDecoder().decode(keystore);
        KeySpec keySpec = new PKCS8EncodedKeySpec(encodedKey);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        privateKey = keyFactory.generatePrivate(keySpec);

        if (privateKey != null) {
            //签名
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(source.getBytes());
            byte[] signed = signature.sign();
            //取base64，得到签名串
            sign = Base64.getEncoder().encodeToString(signed);
        }
        return sign;
    }

    private String generateSignSource(Map params) {
        Set<String> keySet = params.keySet();
        List<String> keys = new ArrayList<String>();
        for (String key : keySet) {
            if (params.get(key) != null && StringUtils.isNotBlank(params.get(key).toString())) {
                keys.add(key);
            }
        }
        Collections.sort(keys);
        StringBuilder builder = new StringBuilder();
        for (int i = 0, size = keys.size(); i < size; i++) {
            String key = keys.get(i);
            Object value = params.get(key);
            builder.append(key);
            builder.append("=");
            builder.append(value);
            //拼接时，不包括最后一个&字符
            if (i != size - 1) {
                builder.append("&");
            }
        }
        return builder.toString();
    }
}
