package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.bitcoinj.core.Transaction;



public class Balance {

    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(BitcoinBlockChainLoader.class);


    public static int getHighestBlock(Connection conn) throws SQLException {
        int highestBlock = 1;
        String sql = "SELECT COALESCE(MAX(block_number), 1) FROM balance_java";

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                highestBlock = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("Error fetching highest block from balance_java: ", e);
            throw e;
        }

        return highestBlock;
    }

    public static void writeBalances(Connection conn, List<BalanceRecord> balanceList, Logger logger) throws SQLException{
        for (BalanceRecord balance : balanceList) {
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO balance_java (address, txid, block_number, balance) VALUES (?, ?, ?, ?) ON CONFLICT (address, txid, block_number) DO NOTHING")) {
                pstmt.setString(1, balance.getAddress());
                pstmt.setString(2, balance.getTxid());
                pstmt.setInt(3, balance.getBlockNumber());
                pstmt.setFloat(4, balance.getBalance());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                logger.error("Error writing balance record to database: ", e);
                throw e;
            }
        }
    }

    public static void processBlock(int blockNumber, Logger logger) {
        try (Connection conn = Utils.getDatabaseConnection(logger)) {
            if (conn != null) {
                List<Transaction> transactions = Utils.getTransactions(conn, blockNumber);
                for (Transaction transaction : transactions) {
                    try {
                        List<BalanceRecord> balances = Utils.getBalanceRecords(conn, transaction, blockNumber, logger);
                        writeBalances(conn, balances, logger);
                    } catch (Exception e) {
                        logger.error("Error processing transaction: ", e);
                        try (PreparedStatement pstmt = conn.prepareStatement(
                                "INSERT INTO unprocessed_transactions_for_balance (txid, block_number) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
                            pstmt.setString(1, transaction.getTxId().toString());
                            pstmt.setInt(2, blockNumber);
                            pstmt.executeUpdate();
                        } catch (SQLException ex) {
                            logger.error("Error writing unprocessed transaction to database: ", ex);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Error connecting to the database: ", e);
        }
    }

    public static void main(String[] args) {

        // AtomicInteger currentBlockNumber = new AtomicInteger(getHighestBlock(conn));

    }
}
