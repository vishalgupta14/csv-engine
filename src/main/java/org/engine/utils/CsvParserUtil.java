package org.engine.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class CsvParserUtil {

    /**
     * Parses a CSV file into a list of maps (header -> value).
     */
    public static List<Map<String, String>> parseToMap(File csvFile) throws IOException {
        try (Reader reader = new FileReader(csvFile);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            List<Map<String, String>> records = new ArrayList<>();

            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                parser.getHeaderMap().forEach((header, index) -> row.put(header, record.get(header)));
                records.add(row);
            }

            return records;
        }
    }
}
