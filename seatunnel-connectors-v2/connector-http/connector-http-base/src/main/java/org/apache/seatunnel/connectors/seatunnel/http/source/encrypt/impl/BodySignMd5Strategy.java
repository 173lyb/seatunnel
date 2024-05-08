package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.TimeUtils;

import java.time.LocalDateTime;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;
import static org.apache.seatunnel.connectors.seatunnel.http.util.SingUtil.sign;

public class BodySignMd5Strategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body ,Map<String, Object> bodyMap, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException {
        String secretKey = getRequiredValue(bodyEncrypt, SECRET_KEY);
        String ts = getRequiredStringValue(bodyMap, TS);
        String cid = getRequiredStringValue(bodyMap, CID);

        LocalDateTime localDateTime = TimeUtils.parseDateTime(ts, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
        LocalDateTime localDateTimeCid = TimeUtils.parseDateTime(cid, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);

        long tsStamp = TimeUtils.toEpochMill(localDateTime);
        long cidStamp = TimeUtils.toEpochMill(localDateTimeCid);

        bodyMap.put(TS, tsStamp + "");
        bodyMap.put(CID, "cid_" + cidStamp);
        String sign = sign(bodyMap, secretKey);
        bodyMap.put(SIGN, sign);
    }

    @Override
    public Map<String, String> encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {
        return headers;
    }

    @Override
    public Map<String, String> encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {
        return params;
    }

    private String getRequiredStringValue(Map<String, ?> map, String key) throws SeaTunnelException {
        Object value = map.get(key);
        if (value == null || StringUtils.isBlank(value.toString())) {
            throw new SeaTunnelException("无法找到" + key);
        }
        return value.toString();
    }

    private String getRequiredValue(Map<String, String> map, String key) throws SeaTunnelException {
        String value = map.get(key);
        if (StringUtils.isBlank(value)) {
            throw new SeaTunnelException("无法找到" + key);
        }
        return value;
    }
}
