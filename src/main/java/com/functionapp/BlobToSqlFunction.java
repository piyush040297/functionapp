package com.function;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Azure Function that reads a CSV file from Blob Storage and updates Azure SQL Database.
 */
public class BlobToSqlFunction {

    // JDBC connection string from environment variables (uses Managed Identity)
    private static final String SQL_CONNECTION_STRING = System.getenv("AzureSQLConnectionString");

    @FunctionName("processCsvBlobFunction")
    public void run(
            @BlobTrigger(name = "content", path = "democsv/{name}",
                         dataType = "binary", connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename,
            final ExecutionContext context) {

        context.getLogger().info("Processing CSV file: " + filename);

        // Convert byte array to String and remove BOM if present
        String csvContent = removeBOM(new String(content, StandardCharsets.UTF_8));

        // Parse CSV data
        List<String[]> csvData = parseCSV(csvContent);

        if (csvData.isEmpty()) {
            context.getLogger().warning("CSV file is empty or improperly formatted.");
            return;
        }

        // Validate column names before inserting data
        if (!validateColumnName("roll_no", context)) {
            context.getLogger().severe("Column 'roll_no' does not exist in the database.");
            return;
        }

        // Insert data into Azure SQL Database
        insertDataIntoDatabase(csvData, context);
    }

    /**
     * Removes Byte Order Mark (BOM) if present.
     */
    private String removeBOM(String data) {
        return data.startsWith("\uFEFF") ? data.substring(1) : data;
    }

    /**
     * Parses CSV content into a list of String arrays, skipping the header.
     */
    private List<String[]> parseCSV(String csvContent) {
        List<String[]> lines = Arrays.stream(csvContent.split("\n"))
                .map(line -> line.replace("\r", "").split(","))
                .collect(Collectors.toList());

        // Remove header row safely
        if (!lines.isEmpty() && lines.get(0).length > 1) {
            lines.remove(0);
        }

        return lines;
    }

    /**
     * Validates if a column exists in the database.
     */
    private boolean validateColumnName(String columnName, ExecutionContext context) {
        String columnCheckQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                                  "WHERE TABLE_NAME = 'Students' AND COLUMN_NAME = ?";

        try (Connection conn = DriverManager.getConnection(SQL_CONNECTION_STRING);
             PreparedStatement pstmt = conn.prepareStatement(columnCheckQuery)) {

            pstmt.setString(1, columnName);
            ResultSet rs = pstmt.executeQuery();

            return rs.next(); // If result exists, column is valid

        } catch (SQLException e) {
            context.getLogger().severe("Failed to validate column name: " + e.getMessage());
            return false;
        }
    }

    /**
     * Inserts data into Azure SQL Database using MERGE for upsert.
     */
    private void insertDataIntoDatabase(List<String[]> data, ExecutionContext context) {
        if (SQL_CONNECTION_STRING == null || SQL_CONNECTION_STRING.isEmpty()) {
            context.getLogger().severe("AzureSQLConnectionString is missing in environment variables.");
            return;
        }

        // Use MERGE for upsert (SQL Server Compatible)
        String mergeSql = "MERGE INTO Students AS target " +
                          "USING (SELECT ? AS Name, ? AS roll_no) AS source " +
                          "ON target.Name = source.Name " +
                          "WHEN MATCHED THEN " +
                          "UPDATE SET target.roll_no = source.roll_no " +
                          "WHEN NOT MATCHED THEN " +
                          "INSERT (Name, roll_no) VALUES (source.Name, source.roll_no);";

        try (Connection conn = DriverManager.getConnection(SQL_CONNECTION_STRING);
             PreparedStatement pstmt = conn.prepareStatement(mergeSql)) {

            for (String[] row : data) {
                if (row.length < 2) {
                    context.getLogger().warning("Skipping invalid row: " + Arrays.toString(row));
                    continue; // Skip invalid rows
                }

                String name = row[0].trim();
                try {
                    int rollNo = Integer.parseInt(row[1].trim());

                    pstmt.setString(1, name);
                    pstmt.setInt(2, rollNo);
                    pstmt.addBatch();
                } catch (NumberFormatException e) {
                    context.getLogger().warning("Skipping row with invalid number: " + Arrays.toString(row));
                }
            }

            pstmt.executeBatch();
            context.getLogger().info("Database update successful.");

        } catch (SQLException e) {
            context.getLogger().severe("Database connection failed: " + e.getMessage());
        }
    }
}
