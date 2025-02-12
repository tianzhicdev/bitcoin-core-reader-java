package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.logging.log4j.Logger;

abstract class SQLReader<T> extends Reader<T>{

    Connection conn;
    protected Connection refreshConnection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            logger.debug("Establishing new database connection.");
            conn = Utils.getDatabaseConnection(logger);
        }
        return conn;
    }


    public SQLReader(Logger logger) {
        super(logger);
        //TODO Auto-generated constructor stub
    }
    
}