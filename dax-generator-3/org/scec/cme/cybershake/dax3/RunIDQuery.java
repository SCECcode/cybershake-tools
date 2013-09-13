package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.opensha.commons.data.siteData.impl.WillsMap2006;
import org.opensha.commons.geo.Location;

public class RunIDQuery {
	private int runID;
	private int erfID;
	private int siteID = -1;
	private String siteName;
	private int sgtVarID;
	private String sgtString;
	private int ruptVarScenID;
	private int velModelID;
	private String velModelString;
	private double cutoffDist;
	private double lat;
	private double lon;
	private double vs30 = -1.0;
	private double erfSpacing = 1.0;

	private DBConnect dbc;

	private final static int RWG_SGT_ID = 5;
	private final static int AWP_SGT_ID = 6;
	private final static int RWG_REVISED_SGT_ID = 7;
	
	private final int CVMS_VEL_ID = 1;
	private final int CVMH_11_2_VEL_ID = 2;
	private final int HALFSPACE_VEL_ID = 3;
	private final int CVMH_11_9_VEL_ID = 4;
	
	private final String HOSTNAME = "focal.usc.edu";
	private final String DB_NAME = "CyberShake";
	private final String USER = "cybershk_ro";
	private final String PASS = "CyberShake2007";
	
	public RunIDQuery(int runID, boolean hf) {
		this.runID = runID;
		dbc = new DBConnect(HOSTNAME, DB_NAME, USER, PASS);
		populateRunIDInfo();
		populateSiteInfo();
		if (hf) {
			retrieveVs30();
		}
		dbc.closeConnection();
	}

	private void retrieveVs30() {
		try {
			WillsMap2006 wills = new WillsMap2006("/home/scec-02/cybershk/runs/dax-generator/wills2006.bin"); // you'll use the data file constructor here
			Location loc = new Location(lat, lon);
			vs30 = wills.getValue(loc);
			//write to DB
			String pass = null;
			try {
				BufferedReader br = new BufferedReader(new FileReader("focal.txt"));
				pass = br.readLine().trim();
				br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			if (pass==null) {
				System.err.println("Couldn't find a write password for db " + DB_NAME);
				System.exit(3);
			}
			DBConnect dbc = new DBConnect(HOSTNAME, DB_NAME, "cybershk", pass);
			String update = "update CyberShake_Runs set Vs30=" + vs30 + ", Vs30_Source=\"CGS/Wills Site Classification Map (2006)\" where Run_ID=" + runID;
			dbc.insertData(update);
			dbc.closeConnection();
		} catch (IOException ex) {
			System.err.println("Unable to retrieve Vs30 for site " + this.siteName + ".");
			ex.printStackTrace();
			System.exit(-1);
		}
	}

	private void populateSiteInfo() {
		try {
			String query;
			if (siteID==-1) {
				populateRunIDInfo();
			}
			query = "SELECT S.CS_Short_Name, R.Cutoff_Dist FROM CyberShake_Sites S, CyberShake_Site_Regions R WHERE R.CS_Site_ID=S.CS_Site_ID and S.CS_Site_ID=" + siteID + " and R.ERF_ID=" + erfID;
			ResultSet res = dbc.selectData(query);
			res.first();
			if (res.getRow()==0) {
				System.err.println("Couldn't find a site name for site ID " + siteID);
				System.exit(3);
			}
			siteName = res.getString("S.CS_Short_Name");
			cutoffDist = res.getDouble("R.Cutoff_Dist");
			res.next();
			if (!res.isAfterLast()) {
			    System.err.println("More than one Site_ID matched Site_ID "  + siteID + ", aborting.");
			    System.exit(3);
			}
    		res.close();
    		
    		query = "select CS_Site_Lat, CS_Site_Lon " +
			"from CyberShake_Sites " +
			"where CS_Short_Name=\"" + siteName + "\" ";
    		res = dbc.selectData(query);
			res.first();
			if (res.getRow()==0) {
				System.err.println("Couldn't find lat/lon for site " + siteName + ".");
				System.exit(1);
			}
			lat = res.getDouble("CS_Site_Lat");
			lon = res.getDouble("CS_Site_Lon");
			res.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(2);
		}
	}

	private void populateRunIDInfo() {
		try {
			String query = "SELECT Site_ID, ERF_ID, SGT_Variation_ID, Rup_Var_Scenario_ID, Velocity_Model_ID FROM CyberShake_Runs WHERE Run_ID=" + runID;
			ResultSet res = dbc.selectData(query);
			res.first();
    		if (res.getRow()==0) {
    			System.err.println("Couldn't find run " + runID + ".");
    			System.exit(1);
    		}
    		siteID = res.getInt("Site_ID");
    		erfID = res.getInt("ERF_ID");
    		sgtVarID = res.getInt("SGT_Variation_ID");
    		if (sgtVarID==RWG_SGT_ID) {
    			sgtString = "rwg";
    		} else if (sgtVarID==AWP_SGT_ID) {
    			sgtString = "awp";
    		} else if (sgtVarID==RWG_REVISED_SGT_ID) {
    			sgtString = "rwg3.0.3";
    		} else {
    			System.err.println("SGT variation ID " + sgtVarID + " can't be converted to a string representation.");
    			System.exit(2);
    		}
    		ruptVarScenID = res.getInt("Rup_Var_Scenario_ID");
    		velModelID = res.getInt("Velocity_Model_ID");
    		if (velModelID==CVMS_VEL_ID) {
    			velModelString = "cvms";
    		} else if (velModelID==CVMH_11_2_VEL_ID) {
    			velModelString = "cvmh11.2";
    		} else if (velModelID==CVMH_11_9_VEL_ID) {
    			velModelString = "cvmh";
    		} else if (velModelID==HALFSPACE_VEL_ID) {
    			velModelString = "1D";
    		} else {
    			System.err.println("Velocity model ID " + velModelID + " can't be converted to a string representation.");
    			System.exit(3);
    		}

    		res.next();
    		if (!res.isAfterLast()) {
    			System.err.println("More than one Run_ID matched Run_ID "  + runID + ", aborting.");
    			System.exit(4);
    		}
    		
    		res.close();
    		
    		//Do 2nd query to get rupture surface spacing from ERF_ID
    		query = "SELECT ERF_Attr_Value from ERF_Metadata where ERF_ID=" + erfID + " and ERF_Attr_Name='Rupture Surface Resolution'";
    		res = dbc.selectData(query);
    		if (res.getRow()==0) {
    			System.err.println("Couldn't find Rupture Surface Resolution for ERF_ID " + erfID + ".");
    			System.exit(1);
    		}
    		erfSpacing = res.getFloat("ERF_Attr_Value");
    		res.close();
    		
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public int getRunID() {
		return runID;
	}

	public int getErfID() {
		return erfID;
	}

	public int getSiteID() {
		return siteID;
	}

	public String getSiteName() {
		return siteName;
	}

	public int getSgtVarID() {
		return sgtVarID;
	}

	public int getRuptVarScenID() {
		return ruptVarScenID;
	}

	public double getCutoffDist() {
		return cutoffDist;
	}

	public double getLat() {
		return lat;
	}

	public double getLon() {
		return lon;
	}

	public double getVs30() {
		return vs30;
	}

	public int getVelModelID() {
		return velModelID;
	}

	public String getSgtString() {
		return sgtString;
	}

	public String getVelModelString() {
		return velModelString;
	}

	public double getErfSpacing() {
		return erfSpacing;
	}

}
