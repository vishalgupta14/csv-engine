package org.engine.entity;

import org.engine.db.connection.DatabaseBackend;
import org.engine.db.connection.H2Backend;
import org.engine.db.processor.CsvDbLoader;
import org.engine.inmemory.processor.CsvInMemoryProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;

public class CsvSource {
    private static final Logger log = LoggerFactory.getLogger(CsvSource.class);
    private final File csvFile;
    private final String tableName;

    private CsvSource(File csvFile, String tableName) {
        this.csvFile = csvFile;
        this.tableName = tableName;
    }

    public static CsvSource fromFile(String filePath) {
        File file = new File(filePath);
        String nameWithoutExt = file.getName().replaceFirst("[.][^.]+$", "");
        return new CsvSource(file, nameWithoutExt); // default table name = file name
    }

    public static CsvSource fromFile(String filePath, String tableName) {
        return new CsvSource(new File(filePath), tableName);
    }

    public File getFile() {
        return csvFile;
    }

    public String getTableName() {
        return tableName;
    }

    // For in-memory processing
    public CsvInMemoryProcessor stream() {
        return new CsvInMemoryProcessor(csvFile);
    }

    // For DB-backed operations
    public CsvDbLoader loadToDb() {
        return new CsvDbLoader(csvFile, tableName);
    }

    public CsvDbLoader initDb() {
        try {
            return loadToDb().loadToH2();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize DB from CSV", e);
        }
    }

    public CsvDbLoader autoFallbackToDbIfLarge(long maxMb, DatabaseBackend fallbackDb) throws Exception {
        long sizeInMb = csvFile.length() / (1024 * 1024);
        if (sizeInMb > maxMb) {
            log.warn("📦 File is large ({} MB), using fallback DB: {}", sizeInMb, fallbackDb.getType());
            return new CsvDbLoader(csvFile, csvFile.getName().replace(".csv", ""), fallbackDb).loadToDb();
        } else {
            log.info("⚡ File is small ({} MB), using in-memory H2", sizeInMb);
            return new CsvDbLoader(csvFile, csvFile.getName().replace(".csv", ""), new H2Backend()).loadToDb();
        }
    }

}
