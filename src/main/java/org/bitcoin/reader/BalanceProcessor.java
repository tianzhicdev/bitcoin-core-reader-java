package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.bitcoinj.core.Transaction;

public class BalanceProcessor extends AbstractRWProcessor<BalanceRecord> {

    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(BalanceProcessor.class);

    public BalanceProcessor() throws SQLException {
        super(logger, "balance_java");
    }

    // @Override
    // protected int getHighestBlockNumber(Connection conn, String tableName) throws SQLException {
    //     return 678901;
    // }

    @Override
    protected List<BalanceRecord> read(Connection conn, int fromBlockNumber, int toBlockNumber) throws Exception {
        List<BalanceRecord> allBalanceRecords = new ArrayList<>();
        for (int blockNumber = fromBlockNumber; blockNumber < toBlockNumber; blockNumber++) {
            try {
                List<Transaction> transactions;
                while (true) {
                    transactions = Utils.getTransactions(conn, blockNumber);
                    if (!transactions.isEmpty()) {
                        break;
                    }
                    logger.warn("No transactions found for block " + blockNumber + ". Retrying in 1 minute.");
                    Thread.sleep(60000); // Wait for 1 minute before retrying
                }
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
            } catch (SQLException e) {
                logger.error("Error reading balance records for block " + blockNumber + ": ", e);
                throw e;
            } catch (InterruptedException e) {
                logger.error("Thread interrupted while waiting to retry: ", e);
                Thread.currentThread().interrupt(); // Restore interrupted status
            }
        }
        return allBalanceRecords;
    }

    @Override
    protected void write(Connection conn, List<BalanceRecord> balanceList) throws SQLException {
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

    public static void main(String[] args) throws Exception {
        int readerThreads = args.length > 0 ? Integer.parseInt(args[0]) : 1; // Number of BlockReader threads
        int writerThreads = args.length > 1 ? Integer.parseInt(args[1]) : 1;  // Number of DBWriter threads
        int queueSize = args.length > 2 ? Integer.parseInt(args[2]) : 100; // Queue size for transactionQueue
        int readBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 1; // Read batch size for BlockReader
        int minBatchSize = args.length > 4 ? Integer.parseInt(args[4]) : 20; // Smallest size for DBWriter
        int maxBatchSize = args.length > 5 ? Integer.parseInt(args[5]) : 10; // Max batch size for DBWriter

        BalanceProcessor processor = new BalanceProcessor();
        processor.execute(readerThreads, writerThreads, queueSize, readBatchSize, minBatchSize, maxBatchSize);
    }
}
