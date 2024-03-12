package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.TimeUtils;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class HeaderSHA256Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt, Map<String, String> params) throws SeaTunnelException {

    }

    @Override
    public void encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {
        Map<String, String> encryptHeaders = new HashMap<>(headers);
        String accountKey = headerEncrypt.get(ACCOUNT_KEY);
        String secretKey = headerEncrypt.get(SECRET_KEY);
        if (StringUtils.isBlank(accountKey) || StringUtils.isBlank(secretKey)) {
            throw new SeaTunnelException("accountKey or secretKey is null");
        }
        String jsonBody = StringUtils.isBlank(body) ? "" : body;
        String requestTime = headers.get(REQUEST_TIME);
        LocalDateTime localDateTime = TimeUtils.parseDateTime(requestTime, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
        long requestTimeStamp = TimeUtils.toEpochMill(localDateTime);
        String signatureBefore = accountKey + secretKey + requestTimeStamp + jsonBody;
        String signature = org.apache.commons.codec.digest.DigestUtils.sha256Hex(signatureBefore);
        encryptHeaders.put(SIGNATURE, signature);
        encryptHeaders.put(REQUEST_TIME, requestTimeStamp + "");
        encryptHeaders.put(ACCOUNT, accountKey);
        headers.clear();
        headers.putAll(encryptHeaders);
    }

    @Override
    public void encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {

    }
}
