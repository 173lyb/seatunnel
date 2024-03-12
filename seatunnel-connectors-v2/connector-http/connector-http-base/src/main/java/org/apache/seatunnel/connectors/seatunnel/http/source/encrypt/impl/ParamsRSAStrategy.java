package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.util.EncryptUtil;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class ParamsRSAStrategy implements EncryptStrategy {
    @Override
    public void encryptBody(String body, Map<String, Object> bodyMap, Map<String, String> bodyEncrypt, Map<String, String> params) throws SeaTunnelException {

    }

    @Override
    public void encryptHeader(Map<String, String> headers, Map<String, String> headerEncrypt, String body) throws SeaTunnelException {

    }

    @Override
    public void encryptParam(Map<String, String> params, Map<String, String> paramsEncrypt) throws SeaTunnelException {
        String account = params.get(ACCOUNT).toString();
        String RSAData = paramsEncrypt.get(RSA_DATA).toString();
        String privateKey = paramsEncrypt.get(PRIVATE_KEY).toString();
        String publicKey = paramsEncrypt.get(PUBLIC_KEY).toString();

        if (StringUtils.isBlank(account) || StringUtils.isBlank(RSAData) || StringUtils.isBlank(privateKey) || StringUtils.isBlank(publicKey)){
            throw new SeaTunnelException("account, RSAData, privateKey, publicKey 不能为空");
        }
        String RSAData2 = EncryptUtil.encryptByPublicKey(RSAData, publicKey);
        String sign = EncryptUtil.signWithMD5(ACCOUNT+account+DATA+RSAData2);
        params.put(SIGN, sign);
        params.put(DATA, RSAData2);
    }
}
