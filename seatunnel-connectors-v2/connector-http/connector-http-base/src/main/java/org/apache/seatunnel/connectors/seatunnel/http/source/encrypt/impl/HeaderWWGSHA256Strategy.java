package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class HeaderWWGSHA256Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt, Map<String, String> params) throws SeaTunnelException {

    }

    @Override
    public void encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {
        String token = headerEncrypt.get(TOKEN);
        if (StringUtils.isBlank(token)){
            throw new SeaTunnelException("token is null");
        }
        Long timestamp =System.currentTimeMillis()/1000;
        String s = Long.toString(timestamp);
        String signatureBefore = s + token + s;
        String signature = DigestUtils.sha256Hex(signatureBefore);
        headers.put(XZ_SIGNATURE, signature);
        headers.put(XZ_TIMESTAMP, s);
    }

    @Override
    public void encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {

    }
}
