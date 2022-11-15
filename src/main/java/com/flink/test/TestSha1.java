package com.flink.test;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;

import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1;

public class TestSha1 {
    public static void main(String[] args) throws Exception {
        File file = new File("D:\\java apps\\mavenLocalRepository\\org\\apache\\flink\\flink-table-api-scala-bridge_2.12\\1.16.0\\flink-table-api-scala-bridge_2.12-1.16.0-sources.jar");
        String hex = new DigestUtils(SHA_1).digestAsHex(file);
        System.out.println(hex);
    }
}
