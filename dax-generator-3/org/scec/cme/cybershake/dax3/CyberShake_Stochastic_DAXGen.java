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
	public final String DAX_FILENAME_PREFIX = "CyberShake_Stoch_";
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
    private final static String DURATION_FILENAME_PREFIX = "Duration_";
    private final static String DURATION_FILENAME_EXTENSION = ".dur";
	
	//Transformation names
    private final static String GET_VELOCITY_INFO = "Velocity_Info";
    private final static String GEN_STOCH_DAX = "GenStochDax";
    private final static String LOCAL_VM_NAME = "Local_VM";
    private final static String CREATE_DIRS_NAME = "Create_Dirs";
    private final static String HF_SYNTH_NAME = "HF_Synth";
    private final static String COMBINE_NAME = "Combine_HF_Synth";
    private final static String LF_SITE_RESPONSE_NAME = "LF_Site_Response";
    private final static String MERGE_IM_NAME = "MergeIM";
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
		String usageString = "CyberShake_Stochastic_DAXGen <runID> <directory>";
		Options cmd_opts = new Options();
		
		Option help = new Option("h", "help", false, "Print help for CyberShake_HF_DAXGen");
		Option mergeFrequency = OptionBuilder.withArgName("merge_frequency").hasArg().withDescription("Frequency at which to merge the LF and HF seismograms.").create("mf");
//		Option lfRunID = OptionBuilder.withArgName("lf_run_id").hasArg().withDescription("Run ID of low-frequency run to use (required).").create("lr");
		Option noRotd = new Option("nr", "no-rotd", false, "Omit RotD calculations.");
		Option noSiteResponse = new Option("nsr", "no-site-response", false, "Omit site response calculation.");
		Option noLFSiteResponse = new Option("nls", "no-low-site-response", false, "Omit site response calculation for low-frequency seismograms.");
		Option noDuration = new Option("nd", "no-duration", false, "Omit duration calculations.");
//		Option vs30 = OptionBuilder.withArgName("vs30").hasArg().withDescription("Vs30 value to use for site response.").create("v");
		Option debug = new Option("d", "debug", false, "Debug flag.");
		
		cmd_opts.addOption(help);
		cmd_opts.addOption(mergeFrequency);
//		cmd_opts.addOption(lfRunID);
		cmd_opts.addOption(noRotd);
		cmd_opts.addOption(noSiteResponse);
		cmd_opts.addOption(noLFSiteResponse);
		cmd_opts.addOption(noDuration);
//		cmd_opts.addOption(vs30);
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
        sParams.setDirectory(directory);
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
        
        if (line.hasOption(noLFSiteResponse.getOpt())) {
        	sParams.setRunLFSiteResponse(false);
        }
        
        if (line.hasOption(noDuration.getOpt())) {
        	sParams.setRunDuration(false);
        }
        
//        if (line.hasOption(vs30.getOpt())) {
//        	sParams.setVs30(Double.parseDouble(line.getOptionValue(vs30.getOpt())));
//        }
        
        if (line.hasOption(debug.getOpt())) {
        	sParams.setDebug(true);
        }
                
        return runID;
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
    
    private Job getVelInfo() {
    	String id = GET_VELOCITY_INFO;
    	Job velocityJob = new Job(id, NAMESPACE, GET_VELOCITY_INFO, VERSION);
    	
    	velocityJob.addArgument("" + riq.getLon());
    	velocityJob.addArgument("" + riq.getLat());
    	velocityJob.addArgument(riq.getVelModelString());
    	int gridSpacing = (int)(100.0/riq.getLowFrequencyCutoff());
    	velocityJob.addArgument(gridSpacing + "");
    	
    	File velocityInfoFile = new File("velocity_info_" + riq.getSiteName() + ".txt");
    	sParams.setVelocityInfoFile(velocityInfoFile.getName());
    	velocityInfoFile.setRegister(false);
    	//Set transfer to false, otherwise Pegasus thinks we want to archive it and looks in the RC regex for it
    	velocityInfoFile.setTransfer(TRANSFER.FALSE);

    	velocityJob.addArgument(velocityInfoFile.getName());
    	velocityJob.uses(velocityInfoFile, LINK.OUTPUT);
    	
    	return velocityJob;
    }

	private Job genStochDAX(File daxFile) {
		//Runs a job which creates a DAX for running the SGT jobs
		String id = GEN_STOCH_DAX + "_" + riq.getSiteName();
		Job genStochDAXJob = new Job(id, NAMESPACE, GEN_STOCH_DAX, VERSION);

		File velocityFile = new File("velocity_info_" + riq.getSiteName() + ".txt");
		
		genStochDAXJob.addArgument("-r " + riq.getRunID());
		genStochDAXJob.addArgument("-mf " + sParams.getMergeFrequencyString());
		genStochDAXJob.addArgument("-lr " + sParams.getLfRunID());
		if (!sParams.isRunRotd()) {
			genStochDAXJob.addArgument("-nr");
		}
		if (!sParams.isRunSiteResponse()) {
			genStochDAXJob.addArgument("-nsr");
		}
		if (!sParams.isRunLFSiteResponse()) {
			genStochDAXJob.addArgument("-nls");
		}
		if (!sParams.isRunDuration()) {
			genStochDAXJob.addArgument("-nd");
		}
		if (sParams.isDebug()) {
			genStochDAXJob.addArgument("-d");
		}

		genStochDAXJob.addArgument("-o " + daxFile.getName());
		genStochDAXJob.addArgument("-v " + velocityFile.getName());
		
		genStochDAXJob.uses(velocityFile, LINK.INPUT);
		genStochDAXJob.uses(daxFile, LINK.OUTPUT);
		
		daxFile.setRegister(false);
		daxFile.setTransfer(TRANSFER.TRUE);
		
		genStochDAXJob.addProfile("hints", "executionPool", "local");
		
		return genStochDAXJob;
	}
    
	private void makeDax() {
		//Start with some consistency checking
        if (!sParams.getLowFreqRIQ().getSiteName().equals(riq.getSiteName())) {
        	System.err.println("High and low frequency site names " + riq.getSiteName() + " and " + sParams.getLowFreqRIQ().getSiteName() + " don't agree, aborting.");
        	System.exit(2);
        }
        if (sParams.getLowFreqRIQ().getLowFrequencyCutoff()<sParams.getMergeFrequency()) {
        	System.err.println("Low frequency stopped at " + sParams.getLowFreqRIQ().getLowFrequencyCutoff() + "Hz, so can't merge at " + sParams.getMergeFrequency() + ", aborting.");
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
		
		if (sParams.getLowFreqRIQ().getLowFrequencyCutoff()>=1.0) {
			//@1 Hz we use seismograms which are 400s in length
			sParams.setTlen(400.0);
		}
		
		ADAG topDAX = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_top" + DAX_FILENAME_EXTENSION);
		
		// Create update run state job
	    Job updateJob = addUpdate(riq.getRunID(), "PP_START", "PP_END");
	    topDAX.addJob(updateJob);
	    
	    // Create job to query UCVM to get velocity info for setting vref
	    Job velocityJob = getVelInfo();
	    topDAX.addJob(velocityJob);

	    File genStochDaxFile = new File(DAX_FILENAME_PREFIX + riq.getSiteName() + DAX_FILENAME_EXTENSION);
	    genStochDaxFile.setRegister(false);
	    genStochDaxFile.setTransfer(TRANSFER.FALSE);
		Job genStochDAX = genStochDAX(genStochDaxFile);
		topDAX.addJob(genStochDAX);
		//To pick up velocity file as a dependency
		topDAX.addDependency(velocityJob, genStochDAX);
		
		DAX stochDAX = new DAX("Sub_Stoch_" + riq.getSiteName(), genStochDaxFile.getName());
		stochDAX.addArgument("--cluster label");
		stochDAX.addArgument("--force");
		stochDAX.addArgument("--q");
		stochDAX.addArgument("--output shock");

		stochDAX.addArgument("--basename Sub_Stoch_" + riq.getSiteName());
		topDAX.addDAX(stochDAX);
		topDAX.addDependency(genStochDAX, stochDAX);
		
		CyberShake_DB_DAXGen dbDaxGen = new CyberShake_DB_DAXGen(riq, 1, true, riq.getLowFrequencyCutoff(), false, sParams.isRunRotd(), sParams.isRunDuration(), sParams.getVelocityInfoFile());
		ADAG dbADAG = dbDaxGen.makeDAX();
		String dbDAXFilename = DAX_FILENAME_PREFIX + riq.getSiteName() + "_DB_Products" + DAX_FILENAME_EXTENSION;
		dbADAG.writeToFile(dbDAXFilename);
				
		DAX dbDAX = new DAX("dbDax", dbDAXFilename);
		dbDAX.addArgument("--force");
		dbDAX.addArgument("--q");
		//Track the velocity file so it gets staged
		File velocityFile = new File(sParams.getVelocityInfoFile());
		velocityFile.setRegister(false);
		velocityFile.setTransfer(TRANSFER.FALSE);
		dbDAX.uses(velocityFile, LINK.INPUT);
		File dbDAXFile = new File(dbDAXFilename);
		dbDAXFile.addPhysicalFile("file://" + sParams.getDirectory() + "/" + dbDAXFilename, "local");
	    
		topDAX.addDAX(dbDAX);
		topDAX.addFile(dbDAXFile);
		topDAX.addDependency(stochDAX, dbDAX);
		topDAX.addDependency(dbDAX, updateJob);
		topDAX.addDependency(velocityJob, dbDAX);
	    
		String topDaxFilename = DAX_FILENAME_PREFIX + riq.getSiteName() + "_top" + DAX_FILENAME_EXTENSION;
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
