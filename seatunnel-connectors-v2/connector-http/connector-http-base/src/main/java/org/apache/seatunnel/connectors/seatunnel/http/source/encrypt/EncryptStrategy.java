package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt;

import org.apache.seatunnel.common.utils.SeaTunnelException;

import java.util.Map;

public interface EncryptStrategy {
    void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt,Map<String,String> params) throws SeaTunnelException;

    Map<String, String> encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException;

    Map<String, String> encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException;
}
