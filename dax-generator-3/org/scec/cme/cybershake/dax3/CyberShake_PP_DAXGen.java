package org.scec.cme.cybershake.dax3;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.DAX;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;


public class CyberShake_PP_DAXGen {

	//Constants
    protected final static String DAX_FILENAME_PREFIX = "CyberShake_";
    protected final static String DAX_FILENAME_EXTENSION = ".dax";
	protected final static String NAMESPACE="scec";
	protected final static String VERSION="1.0";
	
	private final static String SEISMOGRAM_FILENAME_PREFIX = "Seismogram_";
    private final static String SEISMOGRAM_FILENAME_EXTENSION = ".grm";
    private final static String PEAKVALS_FILENAME_PREFIX = "PeakVals_";
    private final static String PEAKVALS_FILENAME_EXTENSION = ".bsa";
	
	//Job names
    private final static String UPDATERUN_NAME = "UpdateRun";
    private final static String CYBERSHAKE_NOTIFY_NAME = "CyberShakeNotify";
    private final static String CHECK_SGT_NAME = "CheckSgt";
    private final static String ZIP_SEIS_NAME = "ZipSeismograms";
    private final static String ZIP_PSA_NAME = "ZipPeakSA";
    private final static String EXTRACT_SGT_NAME = "extract_sgt";
    private final static String SEISMOGRAM_SYNTHESIS_NAME = "seismogram_synthesis";
    private final static String PEAK_VAL_CALC_NAME = "PeakValCalc_Okaya";
	
    //Simulation parameters
    private final static String NUMTIMESTEPS = "3000";
    private final static String SIMULATION_TIMESKIP = "0.1";
    private final static String SPECTRA_PERIOD1 = "all";
    private final static String FILTER_HIGHHZ = "5.0";
    
	//Database
    private final static String DB_SERVER = "focal.usc.edu";
    private final static String DB = "CyberShake";
    private final static String USER = "cybershk_ro";
    private final static String PASS = "CyberShake2007";
    private static DBConnect dbc;
    
    //Instance variables
    private PP_DAXParameters params;
    private RunIDQuery riq;
	
    public static void main(String[] args) {
	//Command-line options
        Options cmd_opts = new Options();
        Option partition = OptionBuilder.withArgName("num_partitions").hasArg().withDescription("Number of partitions to create.").create("p");
        Option priorities = new Option("r", "use priorities");
        Option replicate_sgts = OptionBuilder.withArgName("num_sgts").hasArg().withDescription("Number of times to replicated SGT files, >=1, <=50").create("rs");
        Option sort_ruptures = new Option("s", "sort ruptures by descending size");
        cmd_opts.addOption(partition);
        cmd_opts.addOption(priorities);
        cmd_opts.addOption(replicate_sgts);
        cmd_opts.addOption(sort_ruptures);
        CyberShake_PP_DAXGen daxGen = new CyberShake_PP_DAXGen();
        PP_DAXParameters pp_params = new PP_DAXParameters();
        String usageString = "Usage: CyberShakeRob <runID> <PP directory> [-p num_subDAXes] [-r] [-rs num_repl] [-s]";
        CommandLineParser parser = new GnuParser();
        if (args.length<1) {
            System.out.println(usageString);
            System.exit(1);
        }
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(2);
        }
        int runID = Integer.parseInt(args[0]);
        String directory = args[1];
        pp_params.setPPDirectory(directory);
        if (line.hasOption("p")) {
            pp_params.setNumOfDAXes(Integer.parseInt(line.getOptionValue("p")));
        }
        if (line.hasOption("r")) {
    		pp_params.setUsePriorities(true);
        }
        if (line.hasOption("rs")) {
            pp_params.setSgtReplication(Integer.parseInt(line.getOptionValue("rs")));
        }
        if (line.hasOption("s")) {
        	pp_params.setSortRuptures(true);
        }
        daxGen.makeDAX(runID, pp_params);
	}


	private void makeDAX(int runID, PP_DAXParameters params) {
		try {
			this.params = params;
			//Get parameters from DB and calculate number of variations
			ResultSet ruptureSet = getParameters(runID);
		
			ADAG topLevelDax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName(), 0, 1);
			
			// Add DAX for checking SGT files
			ADAG preDAX = makePreDAX(riq.getRunID(), riq.getSiteName());
			String preDAXFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_pre" + DAX_FILENAME_EXTENSION;
			preDAX.writeToFile(preDAXFile);

			//The arguments here are daxname, the file that the DAX was written to.
			//This file is also the LFN of the daxfile.  We need to create an LFN, PFN association
			//so that the topLevelDax can find it when we plan.
			DAX preD = new DAX("preDAX", preDAXFile);
			preD.addArgument("--force");
			//Add the dax to the top-level dax like a job
			topLevelDax.addDAX(preD);
			//Create a file object.
			File preDFile = new File(preDAXFile);
			preDFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + preDAXFile, "local");
			topLevelDax.addFile(preDFile);
			
			ruptureSet.first();
		
			int sourceIndex, rupIndex;
			int count = 0;

			int currDax = 0;
			ADAG dax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax, currDax, params.getNumOfDAXes());
  	    
			Job[] zipJobs = addZipJobs(dax, currDax);
  	    
			int numVarsInDAX = 0;
  	    
			while (!ruptureSet.isAfterLast()) {
				++count;
				if (count%100==0) {
					System.out.println("Added " + count + " ruptures.");
					System.gc();
				}
  	    	  	    	
				sourceIndex = ruptureSet.getInt("Source_ID");
				rupIndex = ruptureSet.getInt("Rupture_ID");

				//get variations from the DB
				//need them to figure out if we need a new DAX
				ResultSet variationsSet = getVariations(sourceIndex, rupIndex);
				variationsSet.last();
				
				int numVars = variationsSet.getRow();
				if (numVarsInDAX + numVars < params.getMaxVarsPerDAX()) {
					numVarsInDAX += numVars;
				} else {
					//Create new dax
					System.out.println(numVarsInDAX + " vars in dax " + currDax);
					numVarsInDAX = numVars;
					String daxFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax + DAX_FILENAME_EXTENSION;
					dax.writeToFile(daxFile);
					//Add to topLevelDax
					DAX jDax = new DAX("dax_" + currDax, daxFile);
					jDax.addArgument("--cluster horizontal");
					//Makes sure it doesn't prune workflow elements
					jDax.addArgument("--force");
					//Force stage-out of zip files
					jDax.addArgument("--output shock");
					topLevelDax.addDAX(jDax);
					topLevelDax.addDependency(preD, jDax);
					File jDaxFile = new File(daxFile);
					jDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + daxFile, "local");
					topLevelDax.addFile(jDaxFile);

					
					currDax++;
					dax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax, currDax, params.getNumOfDAXes());
					//create new set of zip jobs
					zipJobs = addZipJobs(dax, currDax);
				}
  	    	
				//Insert extraction job
				Job extractJob = createExtractJob(sourceIndex, rupIndex, variationsSet.getString("Rup_Var_LFN"), count, currDax);
				dax.addJob(extractJob);
			
				variationsSet.first();
			
				int rupvarcount = 0;
				//Iterate over variations
				while (!variationsSet.isAfterLast()) {
					//create and add seismogram synthesis
					Job seismoJob = createSeismogramJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
					dax.addJob(seismoJob);
					//create and add PSA
					Job psaJob = createPSAJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
					dax.addJob(psaJob);
					//set up dependencies
			    	dax.addDependency(extractJob, seismoJob);
			    	dax.addDependency(seismoJob, psaJob);
			    	//make the zip jobs appropriate children
			    	dax.addDependency(seismoJob, zipJobs[0]);
			    	dax.addDependency(psaJob, zipJobs[1]);
				    	
			    	// Attach notification job to end of workflow after zip jobs
			    	if (currDax % params.getNotifyGroupSize()== 0) {
			    		Job notifyJob = addNotify(dax, riq.getSiteName(), "DAX", currDax, params.getNumOfDAXes());
			    		dax.addDependency(zipJobs[0], notifyJob);
			    		dax.addDependency(zipJobs[1], notifyJob);
			    	}
			   		rupvarcount++;
			    	variationsSet.next();
				}
				if (numVarsInDAX > params.getNumVarsPerDAX()) {
					//Create new dax
					System.out.println(numVarsInDAX + " vars in dax " + currDax);
					numVarsInDAX = 0;
					String daxFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax + DAX_FILENAME_EXTENSION;
					dax.writeToFile(daxFile);
					//Add to topLevelDax
					DAX jDax = new DAX("dax_" + currDax, daxFile);
					jDax.addArgument("--cluster horizontal");
					//Makes sure it doesn't prune workflow elements
					jDax.addArgument("--force");
					//Force stage-out of zip files
					jDax.addArgument("--output shock");
					topLevelDax.addDAX(jDax);
					topLevelDax.addDependency(preD, jDax);
					File jDaxFile = new File(daxFile);
					jDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + daxFile, "local");
					topLevelDax.addFile(jDaxFile);
					currDax++;
					dax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax, currDax, params.getNumOfDAXes());
					//create new set of zip jobs
					zipJobs = addZipJobs(dax, currDax);
				}
				
				ruptureSet.next();
			}
			//write leftover jobs to dax
			System.out.println(numVarsInDAX + " vars in dax " + currDax);
			String daxFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax + DAX_FILENAME_EXTENSION;
			dax.writeToFile(daxFile);
			//Add to topLevelDax
			DAX jDax = new DAX("dax_" + currDax, daxFile);
			jDax.addArgument("--cluster horizontal");
			jDax.addArgument("--force");
			jDax.addArgument("--output shock");
			topLevelDax.addDAX(jDax);
			topLevelDax.addDependency(preD, jDax);
			File jDaxFile = new File(daxFile);
			jDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + daxFile, "local");
			topLevelDax.addFile(jDaxFile);

			
			// Add DAX for DB insertion/curve generation
            ADAG dbProductsDAX = genDBProductsDAX(currDax+1);
			String dbDAXFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_DB_Products" + DAX_FILENAME_EXTENSION;
			dbProductsDAX.writeToFile(dbDAXFile);
			DAX dbDax = new DAX("dbDax", dbDAXFile);
			dbDax.addArgument("--force");
			dbDax.addArgument("--sites shock,opensha,ranger");
			topLevelDax.addDAX(dbDax);
			for (int i=0; i<=currDax; i++) {
				topLevelDax.addDependency("dax_" + i, "dbDax");
			}
			File dbDaxFile = new File(dbDAXFile);
			dbDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + dbDAXFile, "local");
			topLevelDax.addFile(dbDaxFile);

			
            // Final notifications
            ADAG postDAX = makePostDAX();
			String postDAXFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_post" + DAX_FILENAME_EXTENSION;
			postDAX.writeToFile(postDAXFile);
			DAX postD = new DAX("postDax", postDAXFile);
			postD.addArgument("--force");
			topLevelDax.addDAX(postD);
			topLevelDax.addDependency(dbDax, postD);
			File postDFile = new File(postDAXFile);
			postDFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + postDAXFile, "local");
			topLevelDax.addFile(postDFile);

			String topLevelDaxName = DAX_FILENAME_PREFIX + riq.getSiteName() + DAX_FILENAME_EXTENSION;
			topLevelDax.writeToFile(topLevelDaxName);
			
		} catch (SQLException ex) {
			ex.printStackTrace();
			System.exit(1);
		}
	}


	private ADAG makePostDAX() {
     	try {
    	    ADAG postDax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_post", 0, 1);
    	    
    	    // Create update run state job
    	    Job updateJob = addUpdate(postDax, riq.getRunID(), "PP_START", "PP_END");
    	    postDax.addJob(updateJob);
    	    
          	return postDax;
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	return null;
	}

	private ADAG genDBProductsDAX(int numSubDAXes) {
		CyberShake_DB_DAXGen gen = new CyberShake_DB_DAXGen(riq, ".", numSubDAXes);
		ADAG dax = gen.makeDAX();
			
		return dax;		
	}


	private ResultSet getParameters(int runID) {
		//Populate RunID object
    		riq = new RunIDQuery(runID);
		dbc = new DBConnect(DB_SERVER, DB, USER, PASS);

		String stationName = riq.getSiteName();
      		ResultSet ruptureSet = getRuptures(stationName);
      		int numOfVariations = getNumOfVariations(ruptureSet);
      		params.setNumVarsPerDAX(numOfVariations/params.getNumOfDAXes());
      	
      		return ruptureSet;
	}
	
	private ResultSet getRuptures(String stationName) {
		String query =  "select R.Source_ID, R.Rupture_ID " +
			"from CyberShake_Site_Ruptures R, CyberShake_Sites S " +
			"where S.CS_Short_Name=\"" + stationName + "\" " +
			"and R.CS_Site_ID=S.CS_Site_ID " +
			"and R.ERF_ID=" + riq.getErfID() + " order by R.Source_ID, R.Rupture_ID";
		if (params.isSortRuptures()) {
			//Sort on reverse # of points
			query = "select R.Source_ID, R.Rupture_ID " +
			"from CyberShake_Site_Ruptures SR, CyberShake_Sites S, Ruptures R " +
			"where S.CS_Short_Name=\"" + stationName + "\" " +
			"and SR.CS_Site_ID=S.CS_Site_ID " +
			"and SR.ERF_ID=" + riq.getErfID() + " " +
			"and SR.ERF_ID=R.ERF_ID " + 
			"and SR.Source_ID=R.Source_ID " +
			"and SR.Rupture_ID=R.Rupture_ID " +
			"order by R.Num_Points desc";
		}

		ResultSet rs = dbc.selectData(query);
		try {
			rs.first();
		 	if (rs.getRow()==0) {
	      	    System.err.println("No ruptures found for site " + stationName + ".");
	      	    System.exit(1);
	      	}
		} catch (SQLException e) {
			e.printStackTrace();
		}
      	return rs;
	}
	
	private int getNumOfVariations(ResultSet ruptureSet) {
        try {
            ruptureSet.first();
            int totNum = 0;
            String part1 = "select count(*) " +
                "from Rupture_Variations " +
	            "where Rupture_Variations.Source_ID = ";
            String part2 = " and Rupture_Variations.Rupture_ID = ";
            String part3 = " and Rupture_Variations.ERF_ID = " + riq.getErfID();
            String part4 = " and Rupture_Variations.Rup_Var_Scenario_ID = " + riq.getRuptVarScenID();
            String query;
            int count = 0;
            while (!ruptureSet.isAfterLast()) {
                count++;
                if (count%100==0) System.out.println("Counted " + count + " ruptures.");
                query = part1 + ruptureSet.getInt("Source_ID") + part2 + ruptureSet.getInt("Rupture_ID") + part3 + part4;
                ResultSet varNum = dbc.selectData(query);
                varNum.first();
                totNum += varNum.getInt("count(*)");
                varNum.close();
                ruptureSet.next();
            }
            return totNum;
        } catch (SQLException se) {
            se.printStackTrace();
        }
        return -1;
    }
	
    private ADAG makePreDAX(int runid, String stationName) {
      	try {
      		ADAG preDax = new ADAG(DAX_FILENAME_PREFIX + stationName + "_pre", 0, 1);
	    
      		// Create update run state job
      		Job updateJob = addUpdate(preDax, runid, "PP_INIT", "PP_START");
	    
      		// Create CheckSGT jobs for X, Y components
      		Job checkSgtXJob = addCheck(preDax, stationName, "x");
      		Job checkSgtYJob = addCheck(preDax, stationName, "y");
	    
      		// Create Notify job
      		Job notifyJob = addNotify(preDax, stationName, CHECK_SGT_NAME, 0, 0);
	    
      		/// Make md5 check jobs children of update job
      		preDax.addDependency(updateJob, checkSgtXJob);
      		preDax.addDependency(updateJob, checkSgtYJob);

      		// Make notify job child of the two md5 check jobs
      		preDax.addDependency(checkSgtXJob, notifyJob);
      		preDax.addDependency(checkSgtYJob, notifyJob);
	    
      		if (params.getSgtReplication()>1) { //add replication job
      			Job[] replicateSGTs = addReplicate(preDax, stationName, params.getSgtReplication());
	    	
      			for (Job j: replicateSGTs) {
      				//replicate jobs are children of md5check jobs
      				preDax.addDependency(checkSgtXJob, j);
      				preDax.addDependency(checkSgtYJob, j);
      				//notify job is child of replicate jobs
      				preDax.addDependency(j, notifyJob);
      			}
      		}
	    
      		// Save the DAX
      		String daxFile = DAX_FILENAME_PREFIX + stationName + "_pre" + DAX_FILENAME_EXTENSION;
      		preDax.writeToFile(daxFile);
          	return preDax;
      	} catch (Exception e) {
      		e.printStackTrace();
      		System.exit(-1);
      		return null;
      	}

    }
    
    private Job addUpdate(ADAG dax, int runid, String from_state, String to_state) {
    	String id = UPDATERUN_NAME + "_" + to_state;
    	Job updateJob = new Job(id, NAMESPACE, UPDATERUN_NAME, VERSION);
    	
    	updateJob.addArgument(runid + "");
    	updateJob.addArgument(from_state);
    	updateJob.addArgument(to_state);
    	
    	updateJob.addProfile("globus", "maxWallTime", "5");
    	
    	dax.addJob(updateJob);
    	return updateJob;
    }
    
    private Job addCheck(ADAG dax, String site, String component) {
		String id = CHECK_SGT_NAME + "_" + site + "_" + component;
		Job checkJob = new Job(id, NAMESPACE, CHECK_SGT_NAME, VERSION);

		File sgtFile = new File(site + "_f" + component + "_" + riq.getRunID() + ".sgt");
		File sgtmd5File = new File(site + "_f" + component + "_" + riq.getRunID() + ".sgt.md5");
		
		checkJob.addArgument(sgtFile);
		checkJob.addArgument(sgtmd5File);

		//Need a local copy to be inserted into RLS;  trying this
		checkJob.uses(sgtFile, File.LINK.INOUT);
		checkJob.uses(sgtmd5File, File.LINK.INOUT);

		sgtFile.setRegister(true);
		sgtmd5File.setRegister(true);
		
		// Force a local copy to be inserted in RLS
		/*Filename sgtout = new Filename(sgt, LFN.OUTPUT);
		Filename sgtmd5out = new Filename(sgtmd5, LFN.OUTPUT);
		sgtout.setTransfer(LFN.XFER_NOT);
		sgtmd5out.setTransfer(LFN.XFER_NOT);
		sgtout.setRegister(true);
		sgtmd5out.setRegister(true);
		checkJob.addUses(sgtout);
		checkJob.addUses(sgtmd5out);*/
		
		dax.addJob(checkJob);
		
		return checkJob;
	}
    
    private Job addNotify(ADAG dax, String site, String stage, int daxnum, int maxdax) {
    	String id = CYBERSHAKE_NOTIFY_NAME + "_" + site + "_" + stage + "_" + daxnum;
    	Job notifyJob = new Job(id, NAMESPACE, CYBERSHAKE_NOTIFY_NAME, VERSION);
    		
    	notifyJob.addArgument(riq.getRunID() + "");
    	notifyJob.addArgument("PP");
    	notifyJob.addArgument(stage);
    	notifyJob.addArgument(daxnum + "");
    	notifyJob.addArgument(maxdax + "");
    		
    	dax.addJob(notifyJob);
    	return notifyJob;
    }
 
    
    private Job[] addReplicate(ADAG dax, String site, int sgtReps) {
    	String id = "SGT_Replicate_" + site + "_";
    	
    	File sgtXIn = new File(site + "_fx_" + riq.getRunID() + ".sgt");
    	File sgtYIn = new File(site + "_fy_" + riq.getRunID() + ".sgt");

    	Job[] replicateJobs = new Job[sgtReps];
    	//one job for each copy
    	for (int i=0; i<sgtReps; i++) {
        	replicateJobs[i] = new Job(id+i, NAMESPACE, "SGT_Replicate", VERSION);
    		
        	replicateJobs[i].addArgument(sgtXIn);
        	replicateJobs[i].addArgument(sgtYIn);

        	replicateJobs[i].uses(sgtXIn, File.LINK.INPUT);
        	replicateJobs[i].uses(sgtYIn, File.LINK.INPUT);
    		
    		File xFile = new File(sgtXIn + "." + i);
    		File yFile = new File(sgtYIn + "." + i);
    		
    		xFile.setTransfer(File.TRANSFER.FALSE);
    		yFile.setTransfer(File.TRANSFER.FALSE);
    		
    		xFile.setRegister(true);
    		yFile.setRegister(true);

    		replicateJobs[i].addArgument(xFile);
    		replicateJobs[i].addArgument(yFile);
    		
    		replicateJobs[i].uses(xFile, File.LINK.OUTPUT);
    		replicateJobs[i].uses(yFile, File.LINK.OUTPUT);
    		
        	dax.addJob(replicateJobs[i]);    		
    	}
    	    	
    	return replicateJobs;
    }
    
    private Job[] addZipJobs(ADAG dax, int daxValue) {
    	String id4 = "ZipSeis_" + daxValue;
    	Job zipSeisJob = new Job(id4, NAMESPACE, ZIP_SEIS_NAME, VERSION);
    	dax.addJob(zipSeisJob);
    	File zipSeisFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + daxValue + "_seismograms.zip");
    	zipSeisFile.setTransfer(File.TRANSFER.TRUE);
    	zipSeisFile.setRegister(true);
    	
    	zipSeisJob.addArgument(".");
    	zipSeisJob.addArgument(zipSeisFile);
    	zipSeisJob.uses(zipSeisFile, File.LINK.OUTPUT);
    	
    	String id5 = "ZipPSA_" + daxValue;
    	Job zipPSAJob = new Job(id5, NAMESPACE, ZIP_PSA_NAME, VERSION);
    	dax.addJob(zipPSAJob);
    	File zipPSAFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + daxValue + "_PSA.zip");
    	zipPSAFile.setTransfer(File.TRANSFER.TRUE);
    	zipPSAFile.setRegister(true);
    	zipPSAJob.addArgument(".");
    	zipPSAJob.addArgument(zipPSAFile);
    	zipPSAJob.uses(zipPSAFile, File.LINK.OUTPUT);

    	if (params.isUsePriorities()) {
    		zipSeisFile.addProfile("condor", "priority", params.getNumOfDAXes()-daxValue + "");
    		zipPSAFile.addProfile("condor", "priority", params.getNumOfDAXes()-daxValue + "");
    	}
    	
    	return new Job[]{zipSeisJob, zipPSAJob};
    }
    
	private ResultSet getVariations(int sourceIndex, int rupIndex) {
		String query = "select Rup_Var_LFN " +
			"from Rupture_Variations " +
			"where Rup_Var_Scenario_ID=" + riq.getRuptVarScenID() +
			" and ERF_ID=" + riq.getErfID() +
			" and Source_ID=" + sourceIndex +
			" and Rupture_ID=" + rupIndex +
			" order by Rup_Var_ID";
		ResultSet rs = dbc.selectData(query);
		try {
			rs.first();
		 	if (rs.getRow()==0) {
	      	    System.err.println("No variations found for source " + sourceIndex + ", rupture " + rupIndex + ", ERF " + riq.getErfID() + ", rupt var scenario " + riq.getRuptVarScenID() + ", exiting.");
	      	    System.exit(1);
	      	}
		} catch (SQLException e) {
			e.printStackTrace();
		}
      	return rs;
	}
	

	private Job createExtractJob(int sourceIndex, int rupIndex, String rupVarLFN, int rupCount, int currDax) {
        /**
     	* Add the sgt extraction job
     	*
     	*/
        String id1 = "ID1_" + sourceIndex+"_"+rupIndex;
        Job job1 = new Job(id1, NAMESPACE, EXTRACT_SGT_NAME, VERSION);

        job1.addArgument("stat="+riq.getSiteName());
        job1.addArgument("slon="+riq.getLon());
        job1.addArgument("slat="+riq.getLat());
        job1.addArgument("extract_sgt=1");

        String sgtx=riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgt";
        String sgty=riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgt";
             
        if (params.getSgtReplication()>1) {
        	sgtx = sgtx + "." + params.getCurrentSGTRep();
            sgty = sgty + "." + params.getCurrentSGTRep();
            params.incrementSGTRep();
         }
             
         File sgtxFile = new File(sgtx);
         File sgtyFile = new File(sgty);
             
         File rupsgtxFile = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
         rupsgtxFile.setTransfer(File.TRANSFER.FALSE);
         rupsgtxFile.setRegister(false);
         File rupsgtyFile = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
         rupsgtyFile.setTransfer(File.TRANSFER.FALSE);
         rupsgtyFile.setRegister(false);
         
         File rupVarFile = new File(rupVarLFN);
         
         job1.addArgument("rupmodfile=" + rupVarFile.getName());
         job1.addArgument("sgt_xfile="+ sgtxFile.getName());
         job1.addArgument("sgt_yfile=" + sgtyFile.getName());
         job1.addArgument("extract_sgt_xfile=" + rupsgtxFile.getName());
         job1.addArgument("extract_sgt_yfile=" + rupsgtyFile.getName());

         job1.uses(rupVarFile,File.LINK.INPUT);
         job1.uses(sgtxFile,File.LINK.INPUT);
         job1.uses(sgtyFile,File.LINK.INPUT);
         job1.uses(rupsgtxFile, File.LINK.OUTPUT);
         job1.uses(rupsgtyFile, File.LINK.OUTPUT);

         job1.addProfile("globus", "maxWallTime", "2");
         job1.addProfile("pegasus", "group", "" + rupCount);
         job1.addProfile("pegasus", "label", "" + currDax);
         job1.addProfile("dagman", "CATEGORY", "extract-jobs");
         
         if (params.isUsePriorities()) {
         	job1.addProfile("condor", "priority", params.getNumOfDAXes()-currDax + "");
         }

         return job1;
	}
	


	private Job createSeismogramJob(int sourceIndex, int rupIndex, int rupvarcount, String rupVarLFN, int count, int currDax) {
		String id2 = "ID2_" + sourceIndex+"_"+rupIndex+"_"+rupvarcount;
		Job job2= new Job(id2, NAMESPACE, SEISMOGRAM_SYNTHESIS_NAME,VERSION);
                 
		File seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + 
			riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
			"_"+ rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);                            
		
		File rupVarFile = new File(rupVarLFN);
		
		job2.addArgument("stat="+riq.getSiteName());
		job2.addArgument("slon="+riq.getLon());
		job2.addArgument("slat="+riq.getLat());
		job2.addArgument("extract_sgt=0");
		job2.addArgument("outputBinary=1");
		job2.addArgument("mergeOutput=1");
		job2.addArgument("ntout="+NUMTIMESTEPS);
         
		File rupsgtx = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
		File rupsgty = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		
		job2.addArgument("rupmodfile=" + rupVarFile.getName());
		job2.addArgument("sgt_xfile=" + rupsgtx.getName());
		job2.addArgument("sgt_yfile=" + rupsgty.getName());
     	job2.addArgument("seis_file=" + seisFile.getName());

     	//Must set flags BEFORE 'uses' call, because uses makes a clone
		seisFile.setRegister(false);
		seisFile.setTransfer(File.TRANSFER.FALSE);
     	
     	job2.uses(rupVarFile,File.LINK.INPUT);     
     	job2.uses(rupsgtx,File.LINK.INPUT);
		job2.uses(rupsgty,File.LINK.INPUT);
		job2.uses(seisFile, File.LINK.OUTPUT);

		job2.addProfile("globus", "maxWallTime", "2");
     	job2.addProfile("pegasus", "group", "" + count);
        job2.addProfile("pegasus", "label", "" + currDax);
     
        if (params.isUsePriorities()) {
         	job2.addProfile("condor", "priority", params.getNumOfDAXes()-currDax + "");
        }
		return job2;
	}

	
	private Job createPSAJob(int sourceIndex, int rupIndex, int rupvarcount, String rupVarLFN, int count, int currDax) {
   		File seisFile = new File(SEISMOGRAM_FILENAME_PREFIX +
   			riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
   			"_"+rupvarcount+  SEISMOGRAM_FILENAME_EXTENSION);  
		
    	File peakValsFile = new File(PEAKVALS_FILENAME_PREFIX +
    			riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
    			"_"+rupvarcount+ PEAKVALS_FILENAME_EXTENSION);
    
    	// make a new job for extracting peak values(may need differentiation or integration
    	// to convert between IMT values for 1 spectral period
    	String id3 = "ID3_" + count + "_" + rupvarcount;
    	Job job3 = new Job(id3, NAMESPACE, PEAK_VAL_CALC_NAME,VERSION);
    
    	job3.addArgument("simulation_out_pointsX=2"); //2 b/c 2 components
    	job3.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job3.addArgument("simulation_out_timesamples="+NUMTIMESTEPS);// numTimeSteps
    	job3.addArgument("simulation_out_timeskip="+ SIMULATION_TIMESKIP); //dt
    	job3.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job3.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job3.addArgument("surfseis_rspectra_output_type=aa");
    	job3.addArgument("surfseis_rspectra_period=" + SPECTRA_PERIOD1);
    	job3.addArgument("surfseis_rspectra_apply_filter_highHZ="+FILTER_HIGHHZ);
    	job3.addArgument("surfseis_rspectra_apply_byteswap=no");
    	job3.addArgument("in=" + seisFile.getName());
    	job3.addArgument("out=" + peakValsFile.getName());

        seisFile.setRegister(false);
        seisFile.setTransfer(File.TRANSFER.FALSE);
        job3.uses(seisFile, File.LINK.INPUT);
    	
        peakValsFile.setRegister(false);
        peakValsFile.setTransfer(File.TRANSFER.FALSE);
    	job3.uses(peakValsFile, File.LINK.OUTPUT);
    	
    	job3.addProfile("globus", "maxWallTime", "1");
    	job3.addProfile("pegasus", "group", "" + count);
        job3.addProfile("pegasus", "label", "" + currDax);
        
        if (params.isUsePriorities()) {
        	job3.addProfile("condor", "priority", params.getNumOfDAXes()-currDax + "");
        }
        
        return job3;
	}
}
