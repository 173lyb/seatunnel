package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.XZ_SIGNATURE;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.XZ_TIMESTAMP;

public class HeaderWWGSHA256Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(
            String body,
            Map<String, Object> bodyMap,
            Map<String, String> bodyEncrypt,
            Map<String, String> params)
            throws SeaTunnelException {}

    @Override
    public Map<String, String> encryptHeader(
            Map<String, String> headers, Map<String, String> headerEncrypt, String body)
            throws SeaTunnelException {
        String token = headerEncrypt.get(TOKEN);
        Map<String, String> encryptHeaders = JsonUtils.toMap(JsonUtils.toJsonString(headers));
        if (StringUtils.isBlank(token)) {
            throw new SeaTunnelException("token is null");
        }
        Long timestamp = System.currentTimeMillis() / 1000;
        String s = Long.toString(timestamp);
        String signatureBefore = s + token + s;
        String signature = DigestUtils.sha256Hex(signatureBefore);
        encryptHeaders.put(XZ_SIGNATURE, signature);
        encryptHeaders.put(XZ_TIMESTAMP, s);
        return encryptHeaders;
    }

    @Override
    public Map<String, String> encryptParam(
            Map<String, String> params, Map<String, String> paramsEncrypt)
            throws SeaTunnelException {
        return params;
    }
}
