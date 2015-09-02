package org.scec.cme.cybershake.dax3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.DAX;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.File.TRANSFER;
import edu.isi.pegasus.planner.dax.Job;
import edu.isi.pegasus.planner.dax.File.LINK;

/* This class creates a workflow which will generate stochastic high-frequency CyberShake results,
 * combined with a previous low-frequency run to generate broadband results.
 */

public class CyberShake_Stochastic_DAXGen {
	private RunIDQuery riq;
	private Stochastic_DAXParameters sParams;
	
	//Constants
	public final String DAX_FILENAME_PREFIX = "CyberShake_Stoch";
	public final String DAX_FILENAME_EXTENSION = ".dax";
	private final String NAMESPACE = "scec";
	private final String VERSION = "1.0";
	private final String VM_FILENAME = "LA_Basin_BBP_14.3.0";
	private final static String SEISMOGRAM_FILENAME_PREFIX = "Seismogram_";
    private final static String SEISMOGRAM_FILENAME_EXTENSION = ".grm";
    private final static double DX = 2.0;
    private final static double DY = 2.0;
    private final static double DT = 0.025;
	private final static double PSA_FILTER = 20.0;
	private final static String PSA_FILENAME_PREFIX = "PeakVals_";
    private final static String PSA_FILENAME_EXTENSION = ".bsa";
	private final static String ROTD_FILENAME_PREFIX = "RotD_";
    private final static String ROTD_FILENAME_EXTENSION = ".rotd";
	
	//Transformation names
    private final static String LOCAL_VM_NAME = "Local_VM";
    private final static String CREATE_DIRS_NAME = "Create_Dirs";
    private final static String HF_SYNTH_NAME = "HF_Synth";
    private final static String MERGE_IM_NAME = "Merge_IM";
    private final static String UPDATERUN_NAME = "UpdateRun";
	
	//DB constants
    private final static String DB_SERVER = "focal.usc.edu";
    private final static String DB = "CyberShake";
    private final static String USER = "cybershk_ro";
    private final static String PASS = "CyberShake2007";
	
	public CyberShake_Stochastic_DAXGen(int runID, Stochastic_DAXParameters hp) {
		riq = new RunIDQuery(runID);
		sParams = hp;
	}

	
	private static int parseCommandLine(String[] args, Stochastic_DAXParameters sParams) {
		String usageString = "CyberShake_HF_DAXGen <runID> <directory>";
		Options cmd_opts = new Options();
		
		Option help = new Option("h", "help", false, "Print help for CyberShake_HF_DAXGen");
		Option mergeFrequency = OptionBuilder.withArgName("merge_frequency").hasArg().withDescription("Frequency at which to merge the LF and HF seismograms.").create("mf");
//		Option lfRunID = OptionBuilder.withArgName("lf_run_id").hasArg().withDescription("Run ID of low-frequency run to use (required).").create("lr");
		Option noRotd = new Option("nr", "no-rotd", false, "Omit RotD calculations.");
		Option noSiteResponse = new Option("nsr", "no-site-response", false, "Omit site response calculation.");
		Option debug = new Option("d", "debug", false, "Debug flag.");
		
		cmd_opts.addOption(help);
		cmd_opts.addOption(mergeFrequency);
//		cmd_opts.addOption(lfRunID);
		cmd_opts.addOption(noRotd);
		cmd_opts.addOption(noSiteResponse);
		cmd_opts.addOption(debug);
		
		CommandLineParser parser = new GnuParser();
        if (args.length<=1) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            return -1;
        }
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            return -2;
        }
        int runID = Integer.parseInt(args[0]);
        String directory = args[1];
        System.out.println("directory: " + directory);
        int lfRunID = Integer.parseInt(args[2]);
        sParams.setLfRunID(lfRunID);
        
        if (line.hasOption(help.getOpt())) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            return -1;
        }

        //Required option
//        if (line.hasOption(lfRunID.getOpt())) {
//        	sParams.setLfRunID(Integer.parseInt(line.getOptionValue(lfRunID.getOpt())));
//        } else {
//        	System.err.println("Low-frequency run ID is required.");
//        	return -3;
//        }
        
        if (line.hasOption(mergeFrequency.getOpt())) {
        	sParams.setMergeFrequency(Double.parseDouble(line.getOptionValue(mergeFrequency.getOpt())));
        }
        
        if (line.hasOption(noRotd.getOpt())) {
        	sParams.setRunRotd(false);
        }
        
        if (line.hasOption(noSiteResponse.getOpt())) {
        	sParams.setRunSiteResponse(false);
        }
        
        if (line.hasOption(debug.getOpt())) {
        	sParams.setDebug(true);
        }
                
        return runID;
	}
	
	private ResultSet getRuptures() {
		DBConnect dbc = new DBConnect(DB_SERVER, DB, USER, PASS);
		String query = "select R.Source_ID, R.Rupture_ID, R.Num_Points, R.Mag, count(*) " +
				"from CyberShake_Site_Ruptures SR, CyberShake_Sites S, Ruptures R, Rupture_Variations V " +
				"where S.CS_Short_Name=\"" + riq.getSiteName() + "\" " +
				"and SR.CS_Site_ID=S.CS_Site_ID " +
				"and SR.ERF_ID=" + riq.getErfID() + " " +
				"and SR.ERF_ID=R.ERF_ID " + 
				"and V.ERF_ID=R.ERF_ID " + 
				"and SR.Source_ID=R.Source_ID " +
				"and V.Source_ID=R.Source_ID " +
				"and SR.Rupture_ID=R.Rupture_ID " +
				"and V.Rupture_ID=R.Rupture_ID " +
				"and V.Rup_Var_Scenario_ID=" + riq.getRuptVarScenID() + " " +
				"group by V.Source_ID, V.Rupture_ID " +
				"order by R.Num_Points desc";
		System.out.println("Query: " + query);

		ResultSet rs = dbc.selectData(query);
		try {
			rs.first();
		 	if (rs.getRow()==0) {
	      	    System.err.println("No ruptures found for site " + riq.getSiteName() + ".");
	      	    System.exit(1);
	      	}
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(2);
		}
      	return rs;
	}
	
	//Creates 1 job per ruptures
	private Job createHFSynthJob(int sourceID, int ruptureID, int numRupVars, int numPoints, String localVMFilename) {
		String id = "HF_Synth_" + sourceID + "_" + ruptureID;

		Job job = new Job(id, NAMESPACE, HF_SYNTH_NAME, "2.0");

		String dirPrefix = "" + sourceID;
		
		job.addArgument("stat=" + riq.getSiteName());
		job.addArgument("slat=" + riq.getLat());
		job.addArgument("slon=" + riq.getLon());
		
		File rupGeomFile = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceID + "_" + ruptureID + ".txt");
		rupGeomFile.setRegister(false);
		rupGeomFile.setTransfer(TRANSFER.TRUE);
		
		job.addArgument("rup_geom_file=" + rupGeomFile.getName());
		job.uses(rupGeomFile, File.LINK.INPUT); 
		
		job.addArgument("source_id=" + sourceID);
		job.addArgument("rupture_id=" + ruptureID);
		job.addArgument("num_rup_vars=" + numRupVars);

		File seisFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION);
		seisFile.setRegister(false);
		seisFile.setTransfer(TRANSFER.TRUE);
		job.uses(seisFile, File.LINK.OUTPUT);

		job.addArgument("outfile=" + seisFile.getName());

		job.addArgument("dx=" + DX);
		job.addArgument("dy=" + DY);

		job.addArgument("tlen=" + sParams.getTlen());
		job.addArgument("dt=" + DT);
		int doSiteResponse = 1;
		if (!sParams.isRunSiteResponse()) {
			doSiteResponse = 0;
		}
		job.addArgument("do_site_response=" + doSiteResponse);
		int debug = 0;
		if (sParams.isDebug()) {
			debug = 1;
		}
		job.addArgument("debug=" + debug);

		File localVMFile = new File(localVMFilename);
		localVMFile.setRegister(false);
		localVMFile.setTransfer(TRANSFER.TRUE);
		job.uses(localVMFile, LINK.INPUT);
		
		job.addArgument("vmod=" + localVMFile.getName());

		job.addProfile("pegasus", "priority", "" + numPoints);
		
		//Memory usage is the size of the SRF + the size of the output, basically.
		double srfMem = 14.8 * Math.log10(numPoints) * Math.pow(numPoints, 1.14) / (1024.0*1024.0);
		//Largest RV at dt=0.1 is 75 mb, so cap if larger
		srfMem = Math.min(srfMem, 80.0);
		//Adjust rv mem based on dt, since that affects the dt of the SRF too
		srfMem *= 0.1/DT;
		double outputMem = sParams.getTlen()/DT * 2.0 * 4.0 / (1024.0 * 1024.0);
		int memUsage = (int)(Math.ceil(1.1*(outputMem + srfMem)));

		job.addProfile("pegasus", "pmc_request_memory", "" + memUsage);

		return job;
	}
	
	private Job createLocalVMJob(String vmName, String localVMName) {
		String id = "Create_Local_VM";
		Job job = new Job(id, NAMESPACE, LOCAL_VM_NAME, VERSION);
		
		File vmIn = new File(vmName);
		File vmOut = new File(localVMName);
		
		job.addArgument(vmIn);
		job.addArgument(vmOut);
		
		vmIn.setRegister(false);
		vmOut.setRegister(false);
		
		job.uses(vmIn, LINK.INPUT);
		job.uses(vmOut, LINK.OUTPUT);
        
        return job;
	}
	
	private Job createDirsJob(String inputFile) {
		String id = "Create_Dirs";
		Job job = new Job(id, NAMESPACE, CREATE_DIRS_NAME, VERSION);
		
		File dirsList = new File(inputFile);
		dirsList.setTransfer(TRANSFER.TRUE);
		dirsList.setRegister(false);
		job.uses(dirsList, LINK.INPUT);
		job.addArgument(dirsList);
		
		return job;
	}
	
	private Job createMergeIMJob(int sourceID, int ruptureID, int numRupVars, int numPoints) {
		String id = "Merge_IM_" + sourceID + "_" + ruptureID;

		Job job = new Job(id, NAMESPACE, MERGE_IM_NAME, VERSION);
		
		String dirPrefix = "" + sourceID;
		
		String lfSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + "_" + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
				"_" + sourceID + "_" + ruptureID + SEISMOGRAM_FILENAME_EXTENSION;
		File lfSeisFile = new File(lfSeisName);
		lfSeisFile.setTransfer(TRANSFER.TRUE);
		lfSeisFile.setRegister(true);
		job.uses(lfSeisFile, LINK.INPUT);
		job.addArgument("lf_seis=" + lfSeisFile.getName());
		
		String hfSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + "_" + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION;
		File hfSeisFile = new File(hfSeisName);
		hfSeisFile.setTransfer(TRANSFER.TRUE);
		job.uses(hfSeisFile, LINK.INPUT);
		job.addArgument("hf_seis=" + hfSeisFile.getName());
		
		String mergedSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX +  "_" + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_bb" + SEISMOGRAM_FILENAME_EXTENSION;
		File mergedSeisFile = new File(mergedSeisName);
		mergedSeisFile.setTransfer(TRANSFER.TRUE);
		mergedSeisFile.setRegister(true);
		job.uses(mergedSeisFile, LINK.OUTPUT);
		job.addArgument("seis_out=" + mergedSeisFile.getName());
		
		job.addArgument("freq=" + sParams.getMergeFrequencyString());
		job.addArgument("comps=2");
		job.addArgument("num_rup_vars=" + numRupVars);
		
		//PSA args
		double psaFilter = PSA_FILTER;
		if (psaFilter<2.0*riq.getFrequency()) {
			psaFilter = 2.0*riq.getFrequency();
		}
		
       	job.addArgument("simulation_out_pointsX=2"); //2 b/c 2 components
    	job.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job.addArgument("simulation_out_timesamples="+((int)((sParams.getTlen()/DT + 0.5))));// numTimeSteps
    	job.addArgument("simulation_out_timeskip="+ DT); //dt
    	job.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job.addArgument("surfseis_rspectra_output_type=aa");
    	job.addArgument("surfseis_rspectra_period=all");
    	job.addArgument("surfseis_rspectra_apply_filter_highHZ="+psaFilter);
    	job.addArgument("surfseis_rspectra_apply_byteswap=no");
    	
    	String psaFilename = dirPrefix + java.io.File.separator + PSA_FILENAME_PREFIX + "_" + riq.getSiteName() + "_" + riq.getRunID() + 
    			"_" + sourceID + "_" + ruptureID + "_bb" + PSA_FILENAME_EXTENSION;
    	File psaFile = new File(psaFilename);
    	psaFile.setRegister(true);
    	psaFile.setTransfer(TRANSFER.TRUE);
    	job.uses(psaFile, LINK.OUTPUT);    	
    	job.addArgument("out=" + psaFile.getName());
    	
    	//RotD
		if (sParams.isRunRotd()) {
			job.addArgument("run_rotd=1");
			String rotDFilename = dirPrefix + java.io.File.separator + ROTD_FILENAME_PREFIX + "_" + riq.getSiteName() + "_" + riq.getRunID() + 
	    			"_" + sourceID + "_" + ruptureID + "_bb" + PSA_FILENAME_EXTENSION;
			File rotDFile = new File(rotDFilename);
			rotDFile.setRegister(true);
			rotDFile.setTransfer(TRANSFER.TRUE);
			job.uses(rotDFile, LINK.OUTPUT);
			job.addArgument("rotd_out=" + rotDFile.getName());
		} else {
			job.addArgument("run_rotd=0");
		}
		
		return job;
	}
	
	private void createJobs(ADAG dax, ResultSet ruptureSet) {
		try {
			String localVMFilename = VM_FILENAME + ".local";
			Job localVMJob = createLocalVMJob(VM_FILENAME, localVMFilename);
			dax.addJob(localVMJob);
			
			HashSet<String> dirNames = new HashSet<String>();
			String dirsInputFilename = "dirs_list.txt";
			Job dirsJob = createDirsJob(dirsInputFilename);
			dax.addJob(dirsJob);
			
			int index = 0;
			
			while (!ruptureSet.isAfterLast()) {
				if ((index+1)%1000==0) {
					System.out.println((index+1) + " ruptures processed.");
				}
				int sourceID = ruptureSet.getInt("R.Source_ID");
				int ruptureID = ruptureSet.getInt("R.Rupture_ID");
				int numPoints = ruptureSet.getInt("R.Num_Points");
				int numRupVars = ruptureSet.getInt("count(*)");
			
				dirNames.add("" + sourceID);
				
				Job hfSynthJob = createHFSynthJob(sourceID, ruptureID, numRupVars, numPoints, localVMFilename);
				dax.addJob(hfSynthJob);
				dax.addDependency(localVMJob, hfSynthJob);
				dax.addDependency(dirsJob, hfSynthJob);
				
				Job mergeIMJob = createMergeIMJob(sourceID, ruptureID, numRupVars, numPoints);
				dax.addJob(mergeIMJob);
				dax.addDependency(hfSynthJob, mergeIMJob);
				
				ruptureSet.next();
				index++;
			}
			
			java.io.File dirsInputFile = new java.io.File(sParams.getDirectory() + java.io.File.separator + dirsInputFilename);
			String fullPath = dirsInputFile.getCanonicalPath();
			BufferedWriter bw = new BufferedWriter(new FileWriter(fullPath));
			for (String s: dirNames) {
				bw.write(s + "\n");
			}
			bw.flush();
			bw.close();
			
			edu.isi.pegasus.planner.dax.File dirsPegasusFile = new File(dirsInputFilename);
			dirsPegasusFile.addPhysicalFile("file://" + fullPath);
			dax.addFile(dirsPegasusFile);
		} catch (SQLException se) {
			se.printStackTrace();
			System.exit(3);
		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(4);
		}
		
	}

    private Job addUpdate(int runid, String from_state, String to_state) {
    	String id = UPDATERUN_NAME + "_" + to_state;
    	Job updateJob = new Job(id, NAMESPACE, UPDATERUN_NAME, VERSION);
    	
    	updateJob.addArgument(runid + "");
    	updateJob.addArgument(from_state);
    	updateJob.addArgument(to_state);
    	
    	updateJob.addProfile("globus", "maxWallTime", "5");
    	
    	return updateJob;
    }

	private void makeDax() {
		//Start with some consistency checking
        if (!sParams.getLowFreqRIQ().getSiteName().equals(riq.getSiteName())) {
        	System.err.println("High and low frequency site names " + riq.getSiteName() + " and " + sParams.getLowFreqRIQ().getSiteName() + " don't agree, aborting.");
        	System.exit(2);
        }
        if (sParams.getLowFreqRIQ().getFrequency()<sParams.getMergeFrequency()) {
        	System.err.println("Low frequency stopped at " + sParams.getLowFreqRIQ().getFrequency() + "Hz, so can't merge at " + sParams.getMergeFrequency() + ", aborting.");
        	System.exit(3);
        }
        if (sParams.getLowFreqRIQ().getErfID()!=riq.getErfID()) {
        	System.err.println("ERF ID " + sParams.getLowFreqRIQ().getErfID() + " used for low freq, ID " + riq.getErfID() + " used for high freq, aborting.");
        	System.exit(4);
        }
        if (sParams.getLowFreqRIQ().getRuptVarScenID()!=riq.getRuptVarScenID()) {
        	System.err.println("Rupture Variation Scenario ID " + sParams.getLowFreqRIQ().getRuptVarScenID() + " used for low freq, ID " + riq.getRuptVarScenID() + " used for high freq, aborting.");
        	System.exit(5);
        }
		
		if (sParams.getLowFreqRIQ().getFrequency()>=1.0) {
			//@1 Hz we use seismograms which are 400s in length
			sParams.setTlen(400.0);
		}
        
		ADAG stochADAG = new ADAG(DAX_FILENAME_PREFIX + "_Stoch_" + riq.getSiteName() + DAX_FILENAME_EXTENSION);
		
		ResultSet ruptures = getRuptures();
		createJobs(stochADAG, ruptures);

		String stochDAXFilename = DAX_FILENAME_PREFIX + "_Stoch_" + riq.getSiteName() + DAX_FILENAME_EXTENSION;
		stochADAG.writeToFile(stochDAXFilename);
		
		CyberShake_DB_DAXGen dbDaxGen = new CyberShake_DB_DAXGen(riq, 1, true, riq.getFrequency(), false);
		ADAG dbADAG = dbDaxGen.makeDAX();
		String dbDAXFilename = DAX_FILENAME_PREFIX + riq.getSiteName() + "_DB_Products" + DAX_FILENAME_EXTENSION;
		dbADAG.writeToFile(dbDAXFilename);
		
		ADAG topDAX = new ADAG(DAX_FILENAME_PREFIX + "_" + riq.getSiteName() + DAX_FILENAME_EXTENSION);
	
		// Create update run state job
	    Job updateJob = addUpdate(riq.getRunID(), "PP_START", "PP_END");
		
		DAX stochDAX = new DAX("stochDax", stochDAXFilename);
		stochDAX.addArgument("--force");
		stochDAX.addArgument("--q");
		stochDAX.addArgument("--output shock");
		File stochDaxFile = new File(stochDAXFilename);
		stochDaxFile.addPhysicalFile("file://" + sParams.getDirectory() + "/" + stochDAXFilename, "local");
		
		DAX dbDAX = new DAX("stochDax", dbDAXFilename);
		dbDAX.addArgument("--force");
		dbDAX.addArgument("--q");
		File dbDAXFile = new File(dbDAXFilename);
		dbDAXFile.addPhysicalFile("file://" + sParams.getDirectory() + "/" + dbDAXFilename, "local");
	    
		topDAX.addDAX(stochDAX);
		topDAX.addFile(stochDaxFile);
		topDAX.addDAX(dbDAX);
		topDAX.addFile(dbDAXFile);
		topDAX.addDependency(stochDAX, dbDAX);
		topDAX.addDependency(dbDAX, updateJob);
	    
		String topDaxFilename = DAX_FILENAME_PREFIX + "_" + riq.getSiteName() + DAX_FILENAME_EXTENSION;
		File topDaxFile = new File(topDaxFilename);
		topDaxFile.addPhysicalFile("file://" + sParams.getDirectory() + "/" + topDaxFilename, "local");
		topDAX.writeToFile(topDaxFilename);
	}
	
	
	public static void main(String[] args) {
		Stochastic_DAXParameters hfParams = new Stochastic_DAXParameters();
		int runID = parseCommandLine(args, hfParams);
		if (runID<0) {
			System.exit(runID);
		}
		
		CyberShake_Stochastic_DAXGen hfDax = new CyberShake_Stochastic_DAXGen(runID, hfParams);
		hfDax.makeDax();
	}
}
