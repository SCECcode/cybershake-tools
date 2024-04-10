package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.opensha.commons.data.siteData.impl.ThompsonVs30_2020;
import org.opensha.commons.data.siteData.impl.WillsMap2006;
import org.opensha.commons.data.siteData.impl.WaldAllenGlobalVs30;
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
	private double low_frequency_cutoff = 0.5;
	private double max_frequency = 0.5;
	private int numComponents = 2;
	private double sourceFrequency;
	private String host;
	private enum Vs30_Source { Wills2006, Thompson2020, WaldAllenTopoSlope };
	private Vs30_Source vs30Source = Vs30_Source.Thompson2020;
	
	private DBConnect dbc;

	private final static int RWG_SGT_ID = 5;
	private final static int AWP_SGT_ID = 6;
	private final static int RWG_REVISED_SGT_ID = 7;
	private final static int AWP_GPU_SGT_ID = 8;
	
	private final int CVMS_VEL_ID = 1;
	private final int CVMH_11_2_VEL_ID = 2;
	private final int HALFSPACE_VEL_ID = 3;
	private final int CVMH_11_9_VEL_ID = 4;
	private final int CVMSI_4_26_VEL_ID = 5;
	private final int SCEC_1D_ID = 6;
	private final int CVMH_11_9_NO_GTL_ID = 7;
	private final int BBP_1D_ID = 8;
	private final int CCA_1D_ID = 9;
	private final int CCA_ID = 10;
	private final int USGS_ID = 11;
	private final int STUDY_18_8_ID = 12;
	private final int STUDY_22_12_ID = 13;
	private final int SFCVM_ID = 14;
	
	private static String DEFAULT_HOSTNAME = "moment.usc.edu";
	private final String DB_NAME = "CyberShake";
	private final String USER = "cybershk_ro";
	private final String PASS = "CyberShake2007";
    private final String passFile = "/home/shock/scottcal/runs/config/moment.txt";
	
	public RunIDQuery(int runID) {
		this(runID, DEFAULT_HOSTNAME);
	}
	
	public RunIDQuery(int runID, String host) {
		this.runID = runID;
		System.out.println("Connecting to host " + host);
		this.host = host;
		dbc = new DBConnect(this.host, DB_NAME, USER, PASS);
		populateRunIDInfo();
		populateSiteInfo();
		retrieveVs30();
		dbc.closeConnection();
	}

	private void retrieveVs30() {
		System.out.println("Determining Vs30.");
		//Connect to database
		String pass = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(passFile));
			pass = br.readLine().trim();
			br.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		if (pass==null) {
			System.err.println("Couldn't find a write password for db " + DB_NAME);
			System.exit(3);
		}
		DBConnect dbc = new DBConnect(host, DB_NAME, "cybershk", pass);
		//See if we have a Vs30 for this run in the DB already -- if so, don't change it
		String query = "select Target_Vs30 from CyberShake_Runs where Run_ID=" + runID;
		ResultSet rs = dbc.selectData(query);
		try {
			rs.first();
			if (rs.getRow()==0) {
				//No hits
				System.err.println("Can't find run ID " + runID + " in the database on " + host + ", aborting.");
				dbc.closeConnection();
				System.exit(2);
			}
			double vs30 = rs.getDouble("Target_Vs30");
			//Null in SQL row is cast to 0
			if (vs30>0) {
				System.out.println("Found Vs30 value of " + vs30 + " already present in DB, using that.");
				dbc.closeConnection();
				this.vs30 = vs30;
				return;
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
			dbc.closeConnection();
			System.exit(3);
		}
		
		if (vs30Source==Vs30_Source.Wills2006) {
			try {
				WillsMap2006 wills = new WillsMap2006("/home/scec-02/cybershk/runs/dax-generator/wills2006.bin"); // you'll use the data file constructor here
				Location loc = new Location(lat, lon);
				vs30 = wills.getValue(loc);
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(2);
			}
		} else if (vs30Source==Vs30_Source.Thompson2020) {
			try {
				ThompsonVs30_2020 thompson = new ThompsonVs30_2020();
				Location loc = new Location(lat, lon);
				vs30 = thompson.getValue(loc);
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(2);
			}
			//If not in Thompson (2020), default to topo slope
			if (Double.isNaN(vs30)) {
				try {
					WaldAllenGlobalVs30 waldallen = new WaldAllenGlobalVs30();
					Location loc = new Location(lat, lon);
	                vs30 = waldallen.getValue(loc);
					vs30Source=Vs30_Source.WaldAllenTopoSlope;
				} catch (IOException ex) {
                	ex.printStackTrace();
                	System.exit(2);
            	}
			}			
		} else {
			System.err.println("Unsure where to get Vs30 value from, aborting.");
			System.exit(1);
		}
		String sourceString = null;
		if (vs30Source==Vs30_Source.Wills2006) {
			sourceString = "Wills (2006)";
		} else if (vs30Source==Vs30_Source.Thompson2020) {
			sourceString = "Thompson et al. (2020)";
		} else if (vs30Source==Vs30_Source.WaldAllenTopoSlope) {
			sourceString = "Topographic Slope (Wald & Allen 2008)";
		}
		String update = "update CyberShake_Runs set Target_Vs30=" + vs30 + ", Vs30_Source=\"" + sourceString + "\" where Run_ID=" + runID;
		dbc.insertData(update);
		dbc.closeConnection();
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
			String query = "SELECT Site_ID, ERF_ID, SGT_Variation_ID, Rup_Var_Scenario_ID, Velocity_Model_ID, " +
					"Low_Frequency_Cutoff, Max_Frequency, SGT_Source_Filter_Frequency " +
					"FROM CyberShake_Runs WHERE Run_ID=" + runID;
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
    		} else if (sgtVarID==AWP_GPU_SGT_ID) {
    			sgtString = "awp_gpu";
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
    		//Taking out halfspace for now, since is really 0D model 
//    		} else if (velModelID==HALFSPACE_VEL_ID) {
//    			velModelString = "1d";
    		} else if (velModelID==CVMSI_4_26_VEL_ID) {
    			velModelString = "cvmsi";
    		} else if (velModelID==SCEC_1D_ID) {
    			velModelString = "scec1d";
    		} else if (velModelID==CVMH_11_9_NO_GTL_ID) {
    			velModelString = "cvmh_nogtl";
    		} else if (velModelID==BBP_1D_ID) {
    			velModelString = "bbp1d";
    		} else if (velModelID==CCA_1D_ID) {
    			velModelString = "cca1d";
    		} else if (velModelID==CCA_ID) {
    			//If using CCA, use CVM-S4.26 for regions outside of CCA, and USGS Bay Area for outside of that
    			velModelString = "cca,usgs,cvmsi";
    		} else if (velModelID==USGS_ID) {
    			//If using USGS Bay Area, use CCA, then CVM-S4.26.M01
    			velModelString = "usgs,cca,cvmsi";
    		} else if (velModelID==STUDY_18_8_ID) {
    			//For Study 18.8, we're using CCA, then USGS Bay Area, then CVM-S4.26.M01
    			velModelString = "cca,usgs,cvmsi";
    		} else if (velModelID==STUDY_22_12_ID) {
    			velModelString = "cvmsi";
    		} else if (velModelID==SFCVM_ID) {
    			velModelString = "sfcvm";
    		} else {
    			System.err.println("Velocity model ID " + velModelID + " can't be converted to a string representation.");
    			System.exit(3);
    		}

    		low_frequency_cutoff = res.getDouble("Low_Frequency_Cutoff");
    		max_frequency = res.getDouble("Max_Frequency");
    		sourceFrequency = res.getDouble("SGT_Source_Filter_Frequency");
    		
    		res.next();
    		if (!res.isAfterLast()) {
    			System.err.println("More than one Run_ID matched Run_ID "  + runID + ", aborting.");
    			System.exit(4);
    		}
    		
    		res.close();
    		
    		//Do 2nd query to get rupture surface spacing from ERF_ID
    		query = "SELECT ERF_Attr_Value from ERF_Metadata where ERF_ID=" + erfID + " and ERF_Attr_Name='Rupture Surface Resolution'";
    		res = dbc.selectData(query);
			res.first();
    		if (res.getRow()==0) {
    			System.err.println("Couldn't find Rupture Surface Resolution for ERF_ID " + erfID + ".");
    			System.err.println("Query: " + query);
    			System.exit(1);
    		}
    		erfSpacing = res.getFloat("ERF_Attr_Value");
    		res.close();
    		
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Return true if we are using an AWP code
	public boolean isAWPSGT() {
		return sgtString.contains("awp");
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

	public String getLowFrequencyCutoffString() {
		return String.format("%.1f", low_frequency_cutoff);
	}
	
	public double getLowFrequencyCutoff() {
		return low_frequency_cutoff;
	}

	public double getMax_frequency() {
		return max_frequency;
	}

	public void setMax_frequency(double max_frequency) {
		this.max_frequency = max_frequency;
	}

	public int getNumComponents() {
		return numComponents;
	}

	public void setNumComponents(int numComponents) {
		this.numComponents = numComponents;
	}

	public double getSourceFrequency() {
		return sourceFrequency;
	}

	public String getHost() {
		return host;
	}
	
	//To override Vs30 from Wills or Thompson
	public void setVs30(double vs30) {
		//write to DB
		String pass = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(passFile));
			pass = br.readLine().trim();
			br.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		if (pass==null) {
			System.err.println("Couldn't find a write password for db " + DB_NAME);
			System.exit(3);
		}
		DBConnect dbc = new DBConnect(host, DB_NAME, "cybershk", pass);
		String sourceString = "User-provided";
		String update = "update CyberShake_Runs set Target_Vs30=" + vs30 + ", Vs30_Source=\"" + sourceString + "\" where Run_ID=" + runID;
		dbc.insertData(update);
		dbc.closeConnection();
	}

}
