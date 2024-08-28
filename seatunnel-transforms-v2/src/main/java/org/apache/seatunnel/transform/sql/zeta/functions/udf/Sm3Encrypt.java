package org.apache.seatunnel.transform.sql.zeta.functions.udf;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import org.apache.commons.lang3.StringUtils;

import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.util.encoders.Hex;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.stream.Collectors;

/** @Author 熊呈 @Mail xiongcheng@xugudb.com @Date 2024/8/9 11:23 */
@AutoService(ZetaUDF.class)
public class Sm3Encrypt implements ZetaUDF {

    @Override
    public String functionName() {
        return "Sm3Encrypt";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> list) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> list) {
        String inputString;
        if (list.size() == 1) {
            inputString = list.get(0).toString();
        } else {
            inputString = list.stream().map(Object::toString).collect(Collectors.joining(","));
        }
        if (StringUtils.isBlank(inputString)) {
            return null;
        }
        return sm3EncryptCore(inputString);
    }

    private String sm3EncryptCore(String input) {
        SM3Digest digest = new SM3Digest();
        byte[] inputBytes = input.getBytes();
        digest.update(inputBytes, 0, inputBytes.length);
        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);
        return Hex.toHexString(hash);
    }
}
