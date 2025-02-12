package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.logging.log4j.Logger;

abstract class SQLWriter<T> extends Thread {
    private final Logger logger;
    private Connection conn;


    protected Connection refreshConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            logger.debug("Establishing new database connection.");
            conn = Utils.getDatabaseConnection(logger);
        }
        return conn;
    }


    public SQLWriter(Logger logger) {
        this.logger = logger;
    }

    
    protected abstract void write(Connection conn, List<T> records) throws SQLException;

    protected void writeWithRetry(List<T> records, int maxRetries, long retryDelayMillis) throws SQLException {
        int attempt = 0;
        while (attempt <= maxRetries) {
            Connection connection = null;
            try {
                connection = refreshConnection();
                if (connection == null || records == null || records.isEmpty()) {
                    throw new SQLException("Connection is null or records list is empty.");
                }

                boolean originalAutoCommit = connection.getAutoCommit();
                try {
                    connection.setAutoCommit(false);
                    write(connection, records);
                    connection.commit();
                    // writtenRecordsCounter.addAndGet(records.size()); // @todo

                    return;
                } catch (SQLException e) {
                    try {
                        connection.rollback();
                    } catch (SQLException rollbackEx) {
                        logger.error("Error during transaction rollback: ", rollbackEx);
                    }
                    throw e;
                } finally {
                    connection.setAutoCommit(originalAutoCommit);
                }
            } catch (SQLException e) {
                attempt++;
                if (attempt > maxRetries) {
                    logger.error("Max retries reached. Failed to write records: ", e);
                    throw e;
                }
                logger.warn("Write attempt " + attempt + " failed. Retrying in " + retryDelayMillis + "ms.", e);
                try {
                    Thread.sleep(retryDelayMillis);
                } catch (InterruptedException ie) {
                    logger.error("Retry sleep interrupted: ", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}