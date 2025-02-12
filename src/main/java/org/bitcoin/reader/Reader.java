package org.bitcoin.reader;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.Logger;



abstract class Reader<T> extends Thread {
    
    protected final Logger logger;

    public Reader(Logger logger) {
        this.logger = logger;
    }
    
    protected abstract List<T> read(int fromBlockNumber, int toBlockNumber) throws Exception;
}
