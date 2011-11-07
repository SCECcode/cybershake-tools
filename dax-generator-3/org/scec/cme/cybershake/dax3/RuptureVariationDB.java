package org.scec.cme.cybershake.dax3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class RuptureVariationDB {
	private Connection conn;
	private PreparedStatement ps;
	private int numInBatch;
	private final int BATCH_SIZE = 1000;
	
	public RuptureVariationDB(String siteName, int runID) {
		try {
			Class.forName("org.sqlite.JDBC");
			conn = DriverManager.getConnection("jdbc:sqlite:" + siteName + ".db");
			Statement stat = conn.createStatement();
			String tableName = siteName.toLowerCase() + "_" + runID + "_RuptureVariations";
			stat.executeUpdate("drop table if exists " + tableName);
			String createTableStatement = "create table " + tableName +
				"(SourceID INTEGER NOT NULL, RuptureID INTEGER NOT NULL, RupVarID INTEGER NOT NULL" +
				" SubWorkflow INTEGER NOT NULL, PRIMARY KEY (SourceID, RuptureID, RupVarID)";
			stat.executeUpdate(createTableStatement);
			String prepStat = "insert into " + tableName + " values (?, ?, ?, ?);";
			ps = conn.prepareStatement(prepStat);
			conn.setAutoCommit(false);
			numInBatch = 0;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	public void addMapping(int sourceID, int ruptureID, int rupVarID, int daxNum) {
		try {
			ps.setString(0, sourceID + "");
			ps.setString(1, ruptureID + "");
			ps.setString(2, rupVarID + "");
			ps.setString(3, daxNum + "");
			ps.addBatch();
			numInBatch++;
			if (numInBatch>BATCH_SIZE) {
				ps.executeBatch();
				numInBatch = 0;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void finish() {
		try {
			ps.executeBatch();
			conn.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
}
