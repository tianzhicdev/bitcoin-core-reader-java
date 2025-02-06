package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.bitcoinj.core.Transaction;

public class Balance extends AbstractRWProcessor<BalanceRecord> {

    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(Balance.class);

    public Balance() throws SQLException {
        super(logger, "balance_java");
    }

    @Override
    protected List<BalanceRecord> read(int blockNumber) throws Exception {
        try (Connection conn = refreshDatabaseConnection()) {
            List<Transaction> transactions = Utils.getTransactions(conn, blockNumber);
            List<BalanceRecord> allBalanceRecords = new ArrayList<BalanceRecord>();
            for (Transaction transaction : transactions) {
                try {
                    List<BalanceRecord> balanceRecords = Utils.getBalanceRecords(conn, transaction, blockNumber, logger);
                    allBalanceRecords.addAll(balanceRecords);
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
            return allBalanceRecords;
        } catch (SQLException e) {
            logger.error("Error reading balance records for block " + blockNumber + ": ", e);
            throw e;
        }
    }

    @Override
    protected void write(List<BalanceRecord> balanceList) throws SQLException {
        try (Connection conn = refreshDatabaseConnection()) {
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
    }

    public static void main(String[] args) throws Exception {
        int readerThreads = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        int writerThreads = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int queueSize = args.length > 2 ? Integer.parseInt(args[2]) : 100;
        int minBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 10;
        int maxBatchSize = args.length > 4 ? Integer.parseInt(args[4]) : 20;

        Balance processor = new Balance();
        processor.execute(readerThreads, writerThreads, queueSize, minBatchSize, maxBatchSize);
    }
}
