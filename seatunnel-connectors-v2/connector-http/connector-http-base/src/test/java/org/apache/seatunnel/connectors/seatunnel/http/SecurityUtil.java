package org.apache.seatunnel.connectors.seatunnel.http;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import sun.misc.BASE64Decoder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;


/**
 * @author wangwei
 * @version v1.0.0
 * @description AES加密工具
 * @date 2019-01-14
 */
public class SecurityUtil {


    public static void main(String args[]) throws Exception {

        String data="{\"serialNo\":\"123123\",\"reqTime\":\"20170907143740772\",\"reqBody\":{\"zxbm\":\"C51011\",\"zjhm\":\"510127197612191915\",\"xingming\":\"张三\"}}";
        String key="yinhaitesttestyh";
        System.out.println("加密之后");
        String encryptparam=encrypt(data,key);
        System.out.println(encryptparam);
//        String aa="6d6r2We5bLX8GDM8gPAZE1GGzUTrHtGS8WHRSumjaUSR7KPlpjrRVz2RCs/dcaLh0P\tWjVcrUoyPpYWMaPxBqdoRLpAmwWu/lQkoNnODnsWpsZWilJAz9FJnxsr0XhnXyROXseKrlaQgMF6y61f\tWt8Lc9FkyhgA70xK3yme+A13aWaCraVuOQaKpjeLnDKy57SgDBsBsLmypbaaCeNOQki+L+ygS/fzYAaz\tmFxoWK7Gs=";
//        String aaa=aa.replace("\t","");
        System.out.println("解密之后");
        String desEncryptparam=decrypt("HdvEbS1qoDqdf1NhWZe1WqapDIXxNatsQDmpB/SnILCp+nxiCmWZFwcLp7zrzh0BFgbnC5SM6Ayz6Cb5C96328od+bo7mZYHWiHK4ZKzjBl2/60lK4zCiUcBdPvSyh/oayOsg449JPxX3cNGHWAGhceOQGxNAm71gBG9kBivC9U=",key);
        System.out.println(desEncryptparam);
    }

    public static String encrypt(String data,String key) throws Exception
    {
        try {

            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
            int blockSize = cipher.getBlockSize();

            byte[] dataBytes = data.getBytes("UTF-8");
            int plaintextLength = dataBytes.length;
            if (plaintextLength % blockSize != 0) {
                plaintextLength = plaintextLength + (blockSize - (plaintextLength % blockSize));
            }
            byte[] plaintext = new byte[plaintextLength];
            System.arraycopy(dataBytes, 0, plaintext, 0, dataBytes.length);
            SecretKeySpec keyspec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
            IvParameterSpec ivspec = new IvParameterSpec(key.getBytes("UTF-8"));
            cipher.init(Cipher.ENCRYPT_MODE, keyspec, ivspec);
            byte[] encrypted = cipher.doFinal(plaintext);
            return new sun.misc.BASE64Encoder().encode(encrypted);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public static String decrypt(String data,String key) throws Exception
    {
        try
        {
            byte[] encrypted1 = new BASE64Decoder().decodeBuffer(data);
            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
            SecretKeySpec keyspec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
            IvParameterSpec ivspec = new IvParameterSpec(key.getBytes("UTF-8"));
            cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
            byte[] original = cipher.doFinal(encrypted1);
            String originalString = new String(original,"UTF-8");
//          String str=new String(originalString.getBytes("ISO-8859-1"),"GBK");
            return originalString;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

