package org.engine.inmemory.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.engine.utils.CsvParserUtil;
import org.engine.utils.RowMapperUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

public class CsvInMemoryProcessor {
    private static final Logger log = LoggerFactory.getLogger(CsvInMemoryProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final File csvFile;
    private List<Map<String, String>> rows;

    public CsvInMemoryProcessor(File csvFile) {
        this.csvFile = csvFile;
    }

    private List<Map<String, String>> loadRows() {
        if (rows == null) {
            try {
                rows = CsvParserUtil.parseToMap(csvFile);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse CSV", e);
            }
        }
        return rows;
    }

    /**
     * Get stream of raw rows
     */
    public Stream<Map<String, String>> stream() {
        return loadRows().stream();
    }

    /**
     * Map CSV rows to POJOs
     */
    public <T> List<T> mapTo(Class<T> targetType) {
        return loadRows().stream()
                .map(row -> RowMapperUtil.mapToPojo(row, targetType))
                .toList();
    }

    public List<Map<String, String>> toList() {
        return loadRows();
    }

    public List<String> getHeaders() {
        return !loadRows().isEmpty() ? new ArrayList<>(loadRows().get(0).keySet()) : List.of();
    }

    public List<Map<String, String>> limit(int n) {
        return loadRows().stream().limit(n).toList();
    }

    public List<Map<String, String>> skip(int n) {
        return loadRows().stream().skip(n).toList();
    }

    public CsvInMemoryProcessor peekRow() {
        stream().limit(5).forEach(row -> log.info("🔍 Row: {}", row));
        return this;
    }

    public boolean hasRequiredHeaders(String... required) {
        List<String> headers = getHeaders();
        return Arrays.stream(required).allMatch(headers::contains);
    }

    public void writeToCsv(File outputFile) {
        List<Map<String, String>> rows = loadRows();
        if (rows.isEmpty()) return;

        try (FileWriter writer = new FileWriter(outputFile);
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(rows.get(0).keySet().toArray(new String[0])))) {

            for (Map<String, String> row : rows) {
                printer.printRecord(row.values());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write CSV to file: " + outputFile.getName(), e);
        }
    }

    public void writeToCsv(String path) {
        writeToCsv(new File(path));
    }

    /**
     * 🔁 Convert rows to List<JsonNode>
     */
    public List<JsonNode> asJsonList() {
        return loadRows().stream()
                .map(row -> objectMapper.convertValue(row, JsonNode.class))
                .toList();
    }

    /**
     * 💾 Write rows as JSON array to a file
     */
    public void writeJsonToFile(String path) {
        try {
            objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValue(new File(path), loadRows());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write JSON to: " + path, e);
        }
    }

    public void writeJsonLines(String path) {
        try (FileWriter writer = new FileWriter(path)) {
            for (Map<String, String> row : loadRows()) {
                JsonNode jsonNode = objectMapper.convertValue(row, JsonNode.class);
                writer.write(objectMapper.writeValueAsString(jsonNode));
                writer.write(System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write NDJSON to: " + path, e);
        }
    }

    public CsvInMemoryProcessor parseJsonField(String column) {
        List<Map<String, String>> updatedRows = loadRows().stream().map(row -> {
            String jsonStr = row.get(column);
            if (jsonStr != null && jsonStr.trim().startsWith("{")) {
                try {
                    JsonNode parsed = objectMapper.readTree(jsonStr);
                    row.put(column, parsed.toString()); // or: row.put(column, objectMapper.writeValueAsString(parsed));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to parse JSON in column: " + column, e);
                }
            }
            return row;
        }).toList();

        this.rows = updatedRows;
        return this;
    }

    /**
     * 🔍 Infer schema of the CSV (column name → type).
     * Supported types: INTEGER, DOUBLE, STRING
     */
    public Map<String, String> inferSchema(int sampleSize) {
        Map<String, String> schema = new LinkedHashMap<>();
        try (Reader reader = new FileReader(csvFile);
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            List<CSVRecord> records = parser.getRecords();
            List<String> headers = new ArrayList<>(parser.getHeaderMap().keySet());

            if (headers.isEmpty()) {
                throw new IllegalStateException("No headers found in CSV file: " + csvFile.getName());
            }

            for (String header : headers) {
                schema.put(header, "UNKNOWN");
            }

            for (int i = 0; i < Math.min(sampleSize, records.size()); i++) {
                CSVRecord row = records.get(i);
                for (String header : headers) {
                    String value = row.get(header);
                    String currentType = schema.get(header);
                    String detectedType = detectType(value);
                    schema.put(header, mergeTypes(currentType, detectedType));
                }
            }
            return schema;

        } catch (Exception e) {
            throw new RuntimeException("Failed to infer schema from file: " + csvFile.getName(), e);
        }
    }

    /**
     * ✅ Check if required columns exist in the CSV headers
     */
    public void checkRequiredColumns(String... requiredColumns) {
        List<String> headers = getHeaders();
        for (String col : requiredColumns) {
            if (!headers.contains(col)) {
                throw new IllegalArgumentException("Missing required column: " + col);
            }
        }
        log.info("✅ All required columns are present: {}", Arrays.toString(requiredColumns));
    }

    /**
     * ✅ Validate actual schema against expected schema (exact match)
     */
    public boolean validateAgainstSchema(Map<String, String> expectedSchema, int sampleSize) {
        Map<String, String> actual = inferSchema(sampleSize);
        for (Map.Entry<String, String> entry : expectedSchema.entrySet()) {
            String key = entry.getKey();
            String expectedType = entry.getValue();
            String actualType = actual.getOrDefault(key, "MISSING");
            if (!expectedType.equalsIgnoreCase(actualType)) {
                log.error("❌ Schema mismatch on column '{}': expected {}, got {}", key, expectedType, actualType);
                return false;
            }
        }
        log.info("✅ Schema validation passed.");
        return true;
    }

    /**
     * Infer the type of a single cell.
     */
    private String detectType(String value) {
        if (value == null || value.trim().isEmpty()) return "UNKNOWN";
        try {
            Integer.parseInt(value);
            return "INTEGER";
        } catch (Exception ignored) {}
        try {
            Double.parseDouble(value);
            return "DOUBLE";
        } catch (Exception ignored) {}
        return "STRING";
    }

    /**
     * Merge detected type into the schema.
     * Rule: UNKNOWN < INTEGER < DOUBLE < STRING
     */
    private String mergeTypes(String current, String detected) {
        if (current.equals("STRING") || detected.equals("STRING")) return "STRING";
        if (current.equals("DOUBLE") || detected.equals("DOUBLE")) return "DOUBLE";
        if (current.equals("INTEGER") || detected.equals("INTEGER")) return "INTEGER";
        return "UNKNOWN";
    }

}
