package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.Md5Util;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.ACCESS_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SIGN_ID;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.X_SignId;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.X_Timestamp;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.X_Token;

public class HeaderXTokenMD5Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt, Map<String, String> params) throws SeaTunnelException {

    }

    @Override
    public Map<String, String> encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {

        Map<String, String> encryptHeaders = JsonUtils.toMap(JsonUtils.toJsonString(headers));

        // 获取并检查 ACCESS_TOKEN 和 SIGN_ID 的值
        String accessToken = headerEncrypt.get(ACCESS_TOKEN);
        if (accessToken == null || accessToken.isEmpty()) {
            throw new SeaTunnelException("Access token is missing or empty");
        }

        String signId = headerEncrypt.get(SIGN_ID);
        if (signId == null || signId.isEmpty()) {
            throw new SeaTunnelException("Sign ID is missing or empty");
        }

        // 其他逻辑保持不变
        Long timestamp = System.currentTimeMillis();
        encryptHeaders.put(X_Timestamp, Long.toString(timestamp));

        String xToken = Md5Util.md5(accessToken + Md5Util.md5(signId + Md5Util.md5(accessToken)).toLowerCase() + timestamp).toLowerCase();
        encryptHeaders.put(X_Token, xToken);

        encryptHeaders.put(X_SignId, signId);

        return encryptHeaders;
    }


    @Override
    public Map<String, String> encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {
        return params;
    }
}
