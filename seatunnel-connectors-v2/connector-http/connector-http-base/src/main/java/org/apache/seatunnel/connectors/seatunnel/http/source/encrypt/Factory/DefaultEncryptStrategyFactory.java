package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.Factory;

import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.*;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.*;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.*;

public class DefaultEncryptStrategyFactory implements EncryptStrategyFactory {
    @Override
    public EncryptStrategy create(String encryptType) {
        switch (encryptType) {
            case SIGN_MD5:
                return new BodySignMd5Strategy();
            case URL_MD5:
                return new BodyUrlMd5Strategy();
            case BASE64:
                return new BodyBase64Strategy();
            case DT_SIGNATURE_MD5:
                return new BodyDtSignatureMd5Strategy();
            case SHA256:
                return new HeaderSHA256Strategy();
            case RSA:
                return new ParamsRSAStrategy();
            case BODY_SIGN_BASE64:
                return new BodySignBase64Strategy();
            case HEADER_WWG_SHA256:
                return new HeaderWWGSHA256Strategy();
            case SHA256_WITH_RSA:
                return new ParamsSHA256withRSAStrategy();
            default:
                return null;
        }
    }
}
