package com.sonicbase.bench;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class TestMisc {
    public static void main(String []args) {
        for (int i = 0; i < 1000000; i++) {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(new byte[]{1, 2, 3}));

        }
    }
}
