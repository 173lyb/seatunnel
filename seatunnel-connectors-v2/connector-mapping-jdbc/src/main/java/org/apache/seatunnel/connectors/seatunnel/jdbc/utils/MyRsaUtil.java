package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;


import javax.crypto.Cipher;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * RSA加密解密
 *
 * @author ljiong
 **/
public class MyRsaUtil
{
    // Rsa 私钥
    public static String privateKey = "MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAKWklxlWvSBBZLjHaacDNBaucHHWM9IAaFB2lcZ36AcH/Bk0BOYnMIUb0Oip5FuLGqVKIA70YrmsA+LCazNzww+QnLsopJSrcPEFUGxuEZEVUEj2aYsVlk4UGFLh+4mj8B2ZAGvE7/wKSA2ERAw+4SCHyZRyoMNalmB/gM25djBDAgMBAAECgYAHZJIt2lY0k1aYfKX1g0oW3RA9tG65p7UAKlrC8eUUM0IIKe8yCnu65SPsznBXuZyl1eoaYMPrP4co3r6EHF2PSMRMcDoXcBoYZh/vMq8HTK4ulS6EhxLkfqBmXpurdm3BRGlzb8KjUyvWE7AQUyatgi3lLHxp4qodEkv7+a6MeQJBAOg6qj09g+Nc2tj5jCLv8rcvZJD6xLrx8kauOBR0HxoC0ebyb/BYNsyyZ0OtpPGtk4VeEmsU/Q+SgIuyJ+qUA+UCQQC2mRlc9X39xRk3mbbXfur5b9A8BrzDMXsW8RAf4hUFk1mvMOsCAooBWxNu377N24i7wT962QYAcm/6BC/bynEHAkEA1WnMNvlIMfKMP+edDCJceFH6Zm29y1s7Xg8PBGTujBXZVhaoHkTDH3w3/+8c7Oip8F9SJ8wi/2OP9FEl86JQrQJBAIm5SicSRvhcbFvCheVeJi8DhgVwc3mqXZP9ONNDe+WbsT5xYCBA+ARzxGGRQ2ITnrs21AF9pYg6yHjEJbDarYsCQQCjfbuS4HFjhVxwdfEB8+oQiEty9RKJP7NdDQPC1dkkq/IzZxDDoyHzvvIATpwOL56B6Sufb9h2yvcY85I4eYw1";

    //公钥
    public static String publicKey="MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQClpJcZVr0gQWS4x2mnAzQWrnBx1jPSAGhQdpXGd+gHB/wZNATmJzCFG9DoqeRbixqlSiAO9GK5rAPiwmszc8MPkJy7KKSUq3DxBVBsbhGRFVBI9mmLFZZOFBhS4fuJo/AdmQBrxO/8CkgNhEQMPuEgh8mUcqDDWpZgf4DNuXYwQwIDAQAB";
    /**
     * 私钥解密
     *
     * @param
     * @param text 待解密的文本
     * @return 解密后的文本
     */
    public static String decryptByPrivateKey(String text) throws Exception
    {
        return decryptByPrivateKey(privateKey, text);
    }

    /**
     * 公钥解密
     *
     * @param publicKeyString 公钥
     * @param text 待解密的信息
     * @return 解密后的文本
     */
    public static String decryptByPublicKey(String publicKeyString, String text) throws Exception
    {
        X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(publicKeyString));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(x509EncodedKeySpec);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, publicKey);
        byte[] result = cipher.doFinal(Base64.getDecoder().decode(text));
        return new String(result);
    }

    /**
     * 私钥加密
     *
     * @param privateKeyString 私钥
     * @param text 待加密的信息
     * @return 加密后的文本
     */
    public static String encryptByPrivateKey(String privateKeyString, String text) throws Exception
    {
        PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyString));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = keyFactory.generatePrivate(pkcs8EncodedKeySpec);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, privateKey);
        byte[] result = cipher.doFinal(text.getBytes());
        return Base64.getEncoder().encodeToString(result);
    }

    /**
     * 私钥解密
     *
     * @param privateKeyString 私钥
     * @param text 待解密的文本
     * @return 解密后的文本
     */
    public static String decryptByPrivateKey(String privateKeyString, String text) throws Exception
    {
        PKCS8EncodedKeySpec pkcs8EncodedKeySpec5 = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyString));

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = keyFactory.generatePrivate(pkcs8EncodedKeySpec5);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        byte[] result = cipher.doFinal(Base64.getDecoder().decode(text));
        return new String(result);
    }

    /**
     * 公钥加密
     *
     * @param publicKeyString 公钥
     * @param text 待加密的文本
     * @return 加密后的文本
     */
    public static String encryptByPublicKey(String publicKeyString, String text) throws Exception
    {
        X509EncodedKeySpec x509EncodedKeySpec2 = new X509EncodedKeySpec(Base64.getDecoder().decode(publicKeyString));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(x509EncodedKeySpec2);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        byte[] result = cipher.doFinal(text.getBytes());
        return Base64.getEncoder().encodeToString(result);
    }

    /**
     * 构建RSA密钥对
     *
     * @return 生成后的公私钥信息
     */
    public static RsaKeyPair generateKeyPair() throws NoSuchAlgorithmException
    {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(1024);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        RSAPublicKey rsaPublicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey rsaPrivateKey = (RSAPrivateKey) keyPair.getPrivate();
        String publicKeyString = Base64.getEncoder().encodeToString(rsaPublicKey.getEncoded());
        String privateKeyString = Base64.getEncoder().encodeToString(rsaPrivateKey.getEncoded());
        return new RsaKeyPair(publicKeyString, privateKeyString);
    }

    /**
     * RSA密钥对对象
     */
    public static class RsaKeyPair
    {
        private final String publicKey;
        private final String privateKey;

        public RsaKeyPair(String publicKey, String privateKey)
        {
            this.publicKey = publicKey;
            this.privateKey = privateKey;
        }

        public String getPublicKey()
        {
            return publicKey;
        }

        public String getPrivateKey()
        {
            return privateKey;
        }
    }
}
