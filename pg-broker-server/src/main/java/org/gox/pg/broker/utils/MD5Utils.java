package org.gox.pg.broker.utils;

import lombok.experimental.UtilityClass;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@UtilityClass
public class MD5Utils {

    public static final String MD5 = "MD5";

    public String encrypt(String toEncryptStr) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance(MD5);
        md.update(toEncryptStr.getBytes());
        byte[] digest = md.digest();
        return bytesToHex(digest);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
