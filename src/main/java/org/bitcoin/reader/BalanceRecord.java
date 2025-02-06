package org.bitcoin.reader;

public class BalanceRecord {
    private String address;
    private String txid;
    private int blockNumber;
    private long balance;

    public BalanceRecord(String address, String txid, int blockNumber, long balance) {
        this.address = address;
        this.txid = txid;
        this.blockNumber = blockNumber;
        this.balance = balance;
    }

    public String getAddress() {
        return address;
    }

    public String getTxid() {
        return txid;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public float getBalance() {
        return balance;
    }
}

