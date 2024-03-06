package org.apache.seatunnel.connectors.seatunnel.http.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.util.Assert;

import javax.crypto.Cipher;
import java.io.ByteArrayOutputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;

@Slf4j
public class EncryptUtil {

    // MAX_DECRYPT_BLOCK应等于密钥长度/8（1byte=8bit），所以当密钥位数为2048时，最大解密长度应为256.
    // 128 对应 1024，256对应2048
    private static final int KEYSIZE = 1024;

    // RSA最大加密明文大小
    private static final int MAX_ENCRYPT_BLOCK = 117;

    // RSA最大解密密文大小
//	private static final int MAX_DECRYPT_BLOCK = 128;

    private static final int MAX_DECRYPT_BLOCK = KEYSIZE / 8;

    /**
     * 加密算法RSA
     */
    public static final String KEY_ALGORITHM = "RSA";

    private static final String CHARSET_NAME = "UTF-8";


    /**
     * 生成RSA密文
     * @param json
     * @return
     */
    public static String encryptByPublicKey(String json,String rsaPublicKey){
        Assert.hasText(json,"明文不能为空");
        Assert.hasText(rsaPublicKey,"公钥不能为空");
        String cipherText = "";
        try{
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] decode = decoder.decode(rsaPublicKey);
            PublicKey rsaKey = KeyFactory.getInstance(KEY_ALGORITHM).generatePublic(new X509EncodedKeySpec(decode));
            Cipher cipher = Cipher.getInstance(KEY_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE,rsaKey);
            //RSA加密超过117报错，采用分段加密
            byte[] srcBytes = json.getBytes(CHARSET_NAME);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0; i < srcBytes.length; i += MAX_ENCRYPT_BLOCK) {
                byte[] subBytes = Arrays.copyOfRange(srcBytes, i, i + MAX_ENCRYPT_BLOCK);
                //分段加密后的密文
                byte[] subCipher = cipher.doFinal(subBytes);
                out.write(subCipher,0,subCipher.length);
            }
            byte[] bytes = out.toByteArray();
            Base64.Encoder encoder = Base64.getEncoder();
            cipherText = encoder.encodeToString(bytes);
//            log.info("RSA加密后的密文:" + cipherText);
        }catch (Exception ex){
            log.error("RSA加密异常：" + ex.getMessage());
        }
        return cipherText;
    }

    /**
     * 解密RSA密文
     * @param cipherText
     * @return
     */
    public static String decryptByPrivateKey(String cipherText,String rsaPrivateKey){
        Assert.hasText(cipherText,"密文不能为空");
        Assert.hasText(rsaPrivateKey,"私钥不能为空");
        String json = "";
        try{
            Base64.Decoder privateDecoder = Base64.getDecoder();
            byte[] decodePrivateKey = privateDecoder.decode(rsaPrivateKey);
            PrivateKey privateKey = KeyFactory.getInstance(KEY_ALGORITHM).generatePrivate(new PKCS8EncodedKeySpec(decodePrivateKey));
            Cipher cipher = Cipher.getInstance(KEY_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE,privateKey);
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] cipherBytes = decoder.decode(cipherText);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0; i < cipherBytes.length; i += MAX_DECRYPT_BLOCK) {
                byte[] subBytes = Arrays.copyOfRange(cipherBytes, i, i + MAX_DECRYPT_BLOCK);
                //分段解密
                byte[] subCipher = cipher.doFinal(subBytes);
                out.write(subCipher,0,subCipher.length);
            }
            json = new String(out.toByteArray(),CHARSET_NAME);
//            log.info("RSA解密后的明文:" + json);
        }catch (Exception ex){
            log.error("RSA解密异常：" + ex.getMessage());
        }
        return json;
    }


    /**
     * md5签名
     * @param data
     * @return
     */
    public static String signWithMD5(String data){
        String sign = "";
        try{
            sign = DigestUtils.md5Hex(data.getBytes(CHARSET_NAME)).toUpperCase();
        }catch (Exception ex){
            log.error("MD5签名异常:" + ex.getMessage());
        }
        return sign;
    }

    /**
     * 校验签名
     * @param data
     * @param oldSign 服务器返回签名
     * @return
     */
    public static boolean checkSign(String data,String oldSign){
        String md5Sign = signWithMD5(data);
        return md5Sign.equals(oldSign);
    }
}

