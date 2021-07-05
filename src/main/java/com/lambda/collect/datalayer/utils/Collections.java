package com.lambda.collect.datalayer.utils;

import java.util.List;

public class Collections {
    public static <T> T firstOrNull(List<T> list) {
        if (list.isEmpty()) {
            return null;
        }
        for (T object : list) {
            return object;
        }
        return null;
    }

    public static <T> T lastOrNull(List<T> list) {
        if (list.isEmpty()) {
            return null;
        }
        return list.get(list.size() - 1);
    }

    public static String trim(String value, char charToTrim) {
        if (value != null && value.length() > 0 && value.charAt(value.length() - 1) == charToTrim) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }
}
