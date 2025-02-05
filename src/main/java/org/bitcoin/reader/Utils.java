package org.bitcoin.reader;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.logging.log4j.Logger;
import org.consensusj.bitcoin.jsonrpc.BitcoinClient;

public class Utils {

        public static Connection getDatabaseConnection(Logger logger) throws SQLException{
        Connection connection = null;
        try {
            String url = "jdbc:postgresql://localhost:3004/bitcoin";
            // String url = "jdbc:postgresql://marcus-mini.is-very-nice.org:3004/bitcoin";
            String user = "abc";
            String password = "12345";

            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.error("Database connection error: ", e);
            throw new SQLException("Failed to establish database connection.", e);
        }
        return connection;
    }

        public static Connection refreshDatabaseConnection(Connection conn, Logger logger) throws SQLException {
        try {
            if (conn == null || conn.isClosed()) {
                logger.debug("Refreshing database connection.");
                conn = Utils.getDatabaseConnection(logger);
            }
        } catch (SQLException e) {
            logger.error("Error checking or refreshing database connection: ", e);
            throw e;
        }
        return conn;
    }

public static BitcoinClient createBitcoinClient(Logger logger) {
    BitcoinClient btcCore = null;
    try {
        btcCore = new BitcoinClient(
            // new URI("http://marcus-mini.is-very-nice.org:3003"),
            new URI("http://localhost:3003"),
            "bitcoinrpc",
            "12345"
        );
    } catch (Exception e) {
        logger.error("Error creating BitcoinClient: ", e);
    }
    return btcCore;
}
    
}
