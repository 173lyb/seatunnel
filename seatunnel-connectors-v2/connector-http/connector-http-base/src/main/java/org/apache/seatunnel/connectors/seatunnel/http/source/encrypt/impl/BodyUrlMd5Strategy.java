package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.Md5Util;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class BodyUrlMd5Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException {
        String bodyStr = JsonUtils.toJsonString(bodyMap);
        String encryptKey = bodyEncrypt.get(ENCRYPT_KEY);
        if (StringUtils.isNotBlank(encryptKey)) {
            String sign = Md5Util.md5Upper(bodyStr + encryptKey);
            params.put(SIGN, sign);
        } else {
            throw new SeaTunnelException("无法找到encrypt_key");
        }
    }

    @Override
    public void encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {

    }

    @Override
    public void encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {

    }
}
