package org.engine.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class RowMapperUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Convert CSV row (Map<String, String>) to a typed POJO.
     */
    public static <T> T mapToPojo(Map<String, String> row, Class<T> clazz) {
        try {
            return objectMapper.convertValue(row, clazz);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Failed to map row to " + clazz.getSimpleName(), e);
        }
    }
}
