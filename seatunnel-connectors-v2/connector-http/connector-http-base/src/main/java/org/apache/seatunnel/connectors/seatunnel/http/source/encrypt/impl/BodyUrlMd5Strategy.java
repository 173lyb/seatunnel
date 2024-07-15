package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.Md5Util;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.ENCRYPT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SIGN;

@Slf4j
public class BodyUrlMd5Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(
            String body,
            Map<String, Object> bodyMap,
            Map<String, String> bodyEncrypt,
            Map<String, String> params)
            throws SeaTunnelException {
        String bodyStr = JsonUtils.toJsonString(bodyMap);
        String encryptKey = bodyEncrypt.get(ENCRYPT_KEY);
        if (StringUtils.isNotBlank(encryptKey)) {
            String sign = Md5Util.md5Upper(bodyStr + encryptKey);
            log.info("body-url-md5加密获取的sign:{}", sign);
            params.put(SIGN, sign);
        } else {
            throw new SeaTunnelException("无法找到encrypt_key");
        }
    }

    @Override
    public Map<String, String> encryptHeader(
            Map<String, String> headers, Map<String, String> headerEncrypt, String body)
            throws SeaTunnelException {
        return headers;
    }

    @Override
    public Map<String, String> encryptParam(
            Map<String, String> params, Map<String, String> paramsEncrypt)
            throws SeaTunnelException {
        return params;
    }
}
