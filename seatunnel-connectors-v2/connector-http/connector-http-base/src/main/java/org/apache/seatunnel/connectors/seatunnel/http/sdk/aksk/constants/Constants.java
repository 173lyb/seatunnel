package org.apache.seatunnel.connectors.seatunnel.http.sdk.aksk.constants;

/** @author sangfor */
public class Constants {
    public static final String SIG_HEADER_AUTHORIZATION = "Authorization";
    public static final String SIGN_DATE = "sign-date";
    public static final String SDK_CONTENT_TYPE = "sdk-content-type";
    public static final String CONTENT_TYPE = "content-type";
    public static final String SDK_HOST = "sdk-host";
    public static final String HOST = "Host";
    public static final String PAYLOAD_HASH = "Payload-Hash";

    public static final int SIG_ESCAPE_URI = 1;
    public static final int SIG_ESCAPE_QUERY = 2;

    public static final String AUTH_STR =
            "algorithm=HMAC-SHA256, Access=%s, SignedHeaders=%s, Signature=%s";
    public static final String SIGN_STR = "HMAC-SHA256\n%s\n%s";
    public static final String DATE_FORMAT = "yyyyMMdd'T'HHmmss'Z'";

    public static final int AUTH_CODE_PARAMS_NUM = 14;
    public static final String AUTH_CODE_PARAMS = "%s+%s+%s+%s+%s+%s+%s+%s";
}
