package org.example.arrow.io;

import io.netty.util.internal.ThreadLocalRandom;

public class Util {

    public static <T> T pickRandom(T[] options) {
        return options[ThreadLocalRandom.current().nextInt(0, options.length)];
    }
}
