package org.gox.pg.broker.utils;


import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

class Md5UtilsTest {

    @Test
    void encrypt() throws NoSuchAlgorithmException {
        Assertions.assertThat(MD5Utils.encrypt("test")).isEqualTo("098f6bcd4621d373cade4e832627b4f6");
    }
}