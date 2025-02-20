// src/main/java/org/bitcoin/reader/AdHocBalanceProcessor.java
// Start of Selection
package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import org.apache.logging.log4j.Logger;
import org.bitcoinj.core.Transaction;

public class AdHocBalanceProcessor {

    private static final String SELECT_UNPROCESSED_TX = "SELECT txid, block_number FROM unprocessed_transactions_for_balance";
    private static final String INSERT_BALANCE = "INSERT INTO balance_java (address, txid, block_number, balance) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING";

    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(AdHocBalanceProcessor.class);


    public AdHocBalanceProcessor() {
        // this.logger = logger;
    }

    protected List<BalanceRecord> read(Connection conn) throws Exception {
        List<BalanceRecord> allBalanceRecords = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(SELECT_UNPROCESSED_TX);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                String txid = rs.getString("txid");
                int blockNumber = rs.getInt("block_number");

                try {
                    Transaction transaction = Utils.getTransaction(conn, txid);
                    List<BalanceRecord> balanceRecords = Utils.getBalanceRecords(conn, transaction, blockNumber, logger);
                    allBalanceRecords.addAll(balanceRecords);
                } catch (Exception e) {
                    logger.error("Error processing transaction " + txid, e);
                }
            }
        } catch (SQLException e) {
            logger.error("Error reading unprocessed transactions", e);
            throw e;
        }
        return allBalanceRecords;
    }

    protected void write(Connection conn, List<BalanceRecord> records) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(INSERT_BALANCE)) {
            for (BalanceRecord record : records) {
                pstmt.setString(1, record.getAddress());
                pstmt.setString(2, record.getTxid());
                pstmt.setInt(3, record.getBlockNumber());
                pstmt.setFloat(4, record.getBalance());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        } catch (SQLException e) {
            logger.error("Error writing balance records", e);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        AdHocBalanceProcessor adHoc = new AdHocBalanceProcessor();
        try (Connection conn = Utils.getDatabaseConnection(logger)) {
            List<BalanceRecord> records = adHoc.read(conn);
            
            adHoc.write(conn, records);

        } catch (SQLException e) {
            logger.error("Failed to create database connection", e);
            System.exit(1);
        }
        // adHoc.read()


    }
}
