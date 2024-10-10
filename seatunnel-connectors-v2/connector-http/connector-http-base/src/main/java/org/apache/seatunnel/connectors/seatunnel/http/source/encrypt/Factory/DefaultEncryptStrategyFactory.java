package org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.Factory;

import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.BodyBase64Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.BodyDtSignatureMd5Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.BodySignBase64Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.BodySignMd5Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.BodyUrlMd5Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.HeaderSHA256Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.HeaderWWGSHA256Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.HeaderXTokenMD5Strategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.ParamsRSAStrategy;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.impl.ParamsSHA256withRSAStrategy;

import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.BASE64;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.BODY_SIGN_BASE64;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.DT_SIGNATURE_MD5;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.HEADER_WWG_SHA256;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.RSA;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SHA256;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SHA256_WITH_RSA;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.SIGN_MD5;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.URL_MD5;
import static org.apache.seatunnel.connectors.seatunnel.http.constants.encryptConstant.X_TOKEN_MD5;

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
            case X_TOKEN_MD5:
                return new HeaderXTokenMD5Strategy();
            default:
                return null;
        }
    }
}
