package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;
import edu.isi.pegasus.planner.dax.File.LINK;
import edu.isi.pegasus.planner.dax.File.TRANSFER;

public class CyberShake_Sub_Stoch_DAXGen {
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
    
    private final static int VS30 = 0;
    private final static int VS5H = 1;
    private final static int VSD5H = 2;
	
	private RunIDQuery riq;
	private Stochastic_DAXParameters sParams;
    
	public CyberShake_Sub_Stoch_DAXGen(int runID, Stochastic_DAXParameters hp) {
		riq = new RunIDQuery(runID);
		sParams = hp;
	}
    
	public static void main(String[] args) {
		String usageString = "CyberShake_Sub_Stochastic_DAXGen <runID> <directory>";
		Options cmd_opts = new Options();
	
		Option help = new Option("h", "help", false, "Print help for CyberShake_HF_DAXGen");
		Option runID = OptionBuilder.withArgName("run_id").hasArg().withDescription("Stochastic simulation run ID").create("r");
		Option lfRunID = OptionBuilder.withArgName("lf_run_id").hasArg().withDescription("Low-frequency simulation run ID").create("lr");
		Option mergeFrequency = OptionBuilder.withArgName("merge_frequency").hasArg().withDescription("Frequency at which to merge the LF and HF seismograms.").create("mf");
		Option noRotd = new Option("nr", "no-rotd", false, "Omit RotD calculations.");
		Option noSiteResponse = new Option("nsr", "no-site-response", false, "Omit site response calculation.");
		Option noLFSiteResponse = new Option("nls", "no-low-site-response", false, "Omit site response calculation for low-frequency seismograms.");
		Option noDuration = new Option("nd", "no-duration", false, "Omit duration calculations.");
		Option velocityFile = OptionBuilder.withArgName("vs30").hasArg().withDescription("Velocity file with Vs0, Vs30, VsD.").create("v");
		Option outputDAX = OptionBuilder.withArgName("output").hasArg().withDescription("output DAX filename").create("o");
		Option debug = new Option("d", "debug", false, "Debug flag.");
		
		cmd_opts.addOption(help);
		cmd_opts.addOption(runID);
		cmd_opts.addOption(lfRunID);
		cmd_opts.addOption(mergeFrequency);
		cmd_opts.addOption(noRotd);
		cmd_opts.addOption(noSiteResponse);
		cmd_opts.addOption(noLFSiteResponse);
		cmd_opts.addOption(noDuration);
		cmd_opts.addOption(velocityFile);
		cmd_opts.addOption(outputDAX);
		cmd_opts.addOption(debug);
		
		CommandLineParser parser = new GnuParser();
        if (args.length<=1) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            System.exit(-1);
        }
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(-2);
        }
        
        
        if (line.hasOption(help.getOpt())) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            System.exit(-1);
        }
        
        Stochastic_DAXParameters sParams = new Stochastic_DAXParameters();
        if (!line.hasOption(runID.getOpt())) {
        	System.err.println("Must specify runID.");
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	System.exit(-2);
        }
        int run_id = Integer.parseInt(line.getOptionValue(runID.getOpt()));
        
        
        if (!line.hasOption(lfRunID.getOpt())) {
        	System.err.println("Must specify low frequency runID.");
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	System.exit(-2);
        }
        int lf_run_id = Integer.parseInt(line.getOptionValue(lfRunID.getOpt()));
        sParams.setLfRunID(lf_run_id);

        if (!line.hasOption(velocityFile.getOpt())) {
        	System.err.println("Must specify velocity file.");
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	System.exit(-2);
        }
        String velocityFilename = line.getOptionValue(velocityFile.getOpt());

        if (!line.hasOption(outputDAX.getOpt())) {
        	System.err.println("Must specify output DAX file.");
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	System.exit(-2);
        }
        String daxFilename = line.getOptionValue(outputDAX.getOpt());
        
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
                
        if (line.hasOption(debug.getOpt())) {
        	sParams.setDebug(true);
        }
        
        sParams.setDirectory(".");
        CyberShake_Sub_Stoch_DAXGen ssd = new CyberShake_Sub_Stoch_DAXGen(run_id, sParams);
        ADAG stochADAG = new ADAG(daxFilename);
        ResultSet ruptureSet = ssd.getRuptures();
        ssd.createJobs(stochADAG, ruptureSet, velocityFilename);
        
        stochADAG.writeToFile(daxFilename);
	}
	
	private ResultSet getRuptures() {
		DBConnect dbc = new DBConnect(DB_SERVER, DB, USER, PASS);
		String query = "select R.Source_ID, R.Rupture_ID, R.Num_Points, R.Mag, count(*), R.Num_Rows, R.Num_Columns " +
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
	
	//Creates multiple tasks per rupture to keep job runtime low; adds job to combine output files
	private Job createHFSynthJob(int sourceID, int ruptureID, int numRupVars, int numPoints, int numRows, int numCols, String localVMFilename,
			Job localVMJob, Job dirsJob, ADAG dax, double[] velocityArray) {
		//Figure out how many tasks we need
		long varsTimesPoints = numRupVars * numPoints;
		int numTasks = (int)(Math.ceil(varsTimesPoints/10000000.0));
		int numRupVarsPerTask = (int)(Math.ceil(((double)numRupVars)/((double)numTasks)));
		
		Job combineJob = null;
		if (numTasks>1) {
			combineJob = new Job("Combine_HF_Synth_" + sourceID + "_" + ruptureID, NAMESPACE, COMBINE_NAME, "1.0");
		}
		
		String dirPrefix = "" + sourceID;
		
		for (int i=0; i<numTasks; i++) {
			int startingRupVar = i*numRupVarsPerTask;
			int endingRupVar = Math.min((i+1)*numRupVarsPerTask, numRupVars);
			int numRupVarsThisTask = endingRupVar - startingRupVar;
			
			String id = "HF_Synth_" + sourceID + "_" + ruptureID;
			if (numTasks>1) {
				id += "_t" + i;
			}
			
			Job job = new Job(id, NAMESPACE, HF_SYNTH_NAME, "2.0");
		
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
			job.addArgument("num_rup_vars=" + numRupVarsThisTask);

			File seisFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION);
			if (numTasks>1) {
				seisFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
						"_" + sourceID + "_" + ruptureID + "_hf_t" + i + SEISMOGRAM_FILENAME_EXTENSION);
				//Construct rupture variation string
				StringBuffer rup_var_string = new StringBuffer("");
				for (int j=startingRupVar; j<endingRupVar; j++) {
					rup_var_string.append("(" + j + "," + j + "," + 0 + ")");
					if (j<endingRupVar-1) {
						rup_var_string.append(";");
					}
				}
				job.addArgument("rup_vars=" + rup_var_string.toString());
			}
			seisFile.setRegister(false);
			seisFile.setTransfer(TRANSFER.FALSE);
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
			if (sParams.isRunSiteResponse()) {
				job.addArgument("vs30=" + velocityArray[VS30]);
			}
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

			job.addProfile("pegasus", "pmc_priority", "" + numPoints);
		
			//Memory usage is the size of the SRF, the size of the output, and the size of some mysterious arrays in srf2stoch.
			double srfMem = 14.8 * Math.log10(numPoints) * Math.pow(numPoints, 1.14) / (1024.0*1024.0);
			//Largest RV at dt=0.1 is 75 mb, so cap if larger
			srfMem = Math.min(srfMem, 80.0);
			//Adjust rv mem based on dt, since that affects the dt of the SRF too
			srfMem *= 0.1/DT;
			double outputMem = sParams.getTlen()/DT * 2.0 * 4.0 / (1024.0 * 1024.0);
			//slipfile memory:  3 x NP x NQ x LV x sizeof(float)
			double slipfileMem = 3.0*100.0*600.0*1000.0*4.0 / (1024.0*1024.0);
//			//Determine memory usage for srf2stoch buffers
//			int nstk = numCols;
//			int ndip = numRows;
//			int nx = (int)(nstk*0.2/2.0+0.5);
//			int ny = (int)(ndip*0.2/2.0+0.5);
//			int nxdiv = 1;
//			while ((nstk*nxdiv)%nx!=0) {
//				nxdiv++;
//			}
//			int nydiv = 1;
//			while ((ndip*nydiv)%ny!=0) {
//				nydiv++;
//			}
//			double srf2stochBuffers = 3.0*nxdiv*nydiv*nstk*ndip*4.0/(1024.0*1024.0);
//			int memUsage = (int)(Math.ceil(1.1*(outputMem + srfMem + slipfileMem+srf2stochBuffers)));
			//srf2stoch_lite: 
			int nstk = numCols;
			int ndip = numRows;
			double srf2stochBuffers = (4.0*nstk*ndip*4.0)/(1024.0*1024.0);
			int memUsage = (int)(Math.ceil(1.1*(outputMem + srfMem + slipfileMem+srf2stochBuffers)));
			
			job.addProfile("pegasus", "pmc_request_memory", "" + memUsage);
			job.addProfile("pegasus", "label", "pmc");
			
			dax.addJob(job);
			dax.addDependency(localVMJob, job);
			dax.addDependency(dirsJob, job);
			if (numTasks==1) {
				return job;
			} else {
				File combineSeisFile = new File(seisFile.getName());
				combineJob.addArgument(combineSeisFile.getName());
				combineSeisFile.setRegister(false);
				combineSeisFile.setTransfer(TRANSFER.FALSE);
				combineJob.uses(combineSeisFile, LINK.INPUT);
				
				dax.addDependency(job, combineJob);
			}
		}
		File combineSeisOutFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION);
		combineSeisOutFile.setRegister(false);
		combineSeisOutFile.setTransfer(TRANSFER.FALSE);
		combineJob.addArgument(combineSeisOutFile.getName());
		combineJob.uses(combineSeisOutFile, LINK.OUTPUT);
		
		combineJob.addProfile("pegasus", "label", "pmc");
		
		return combineJob;
	}
	
	private Job createLFSiteResponseJob(int sourceID, int ruptureID, int numRupVars, int num_points, double[] velocityArray) {
		String id = "LF_Site_Response_" + sourceID + "_" + ruptureID;

		Job job = new Job(id, NAMESPACE, LF_SITE_RESPONSE_NAME, "1.0");

		File seis_in = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
				"_" + sourceID + "_" + ruptureID + SEISMOGRAM_FILENAME_EXTENSION);
		
		seis_in.setRegister(false);
		seis_in.setTransfer(TRANSFER.TRUE);
		job.addArgument("seis_in=" + seis_in.getName());
		job.uses(seis_in, LINK.INPUT);
		
		String dir_prefix = "" + sourceID;
		
		File seis_out = new File(dir_prefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
				"_" + sourceID + "_" + ruptureID + "_site_response" + SEISMOGRAM_FILENAME_EXTENSION);

		seis_out.setRegister(false);
		seis_out.setTransfer(TRANSFER.FALSE);
		job.addArgument("seis_out=" + seis_out.getName());
		job.uses(seis_out, LINK.OUTPUT);
		
		job.addArgument("slat=" + riq.getLat());
		job.addArgument("slon=" + riq.getLon());
		//Use the cb2014 module when using CyberShake (3D) deterministic seismograms
		job.addArgument("module=cb2014");
		//Need to set both Vs30 and Vref
		job.addArgument("vs30=" + velocityArray[VS30]);
		//Vref = Vsite * Vs30 / VsD
		double vref = velocityArray[VS30] * velocityArray[VSD5H] / velocityArray[VS5H];
		//Round vref to nearest 0.1
		vref = ((double)((int)(vref*10.0)))/10.0;
		job.addArgument("vref=" + vref);
		
		job.addProfile("pegasus", "label", "pmc");
		//We only process 1 rupture variation at a time, so very small memory requirements
		job.addProfile("pegasus", "pmc_request_memory", "1");
		//Let's run the HF jobs first
		job.addProfile("pegasus", "pmc_priority", "1");
		
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
		vmIn.setTransfer(TRANSFER.TRUE);
		vmOut.setRegister(false);
		vmOut.setTransfer(TRANSFER.FALSE);
		
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
		
		if (sParams.isRunLFSiteResponse()) {
			String lfSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
					"_" + sourceID + "_" + ruptureID + "_site_response" + SEISMOGRAM_FILENAME_EXTENSION;
			File lfSeisFile = new File(lfSeisName);
			lfSeisFile.setTransfer(TRANSFER.FALSE);
			lfSeisFile.setRegister(false);
			job.uses(lfSeisFile, LINK.INOUT);
			job.addArgument("lf_seis=" + lfSeisFile.getName());
		} else {
			String lfSeisName = SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
					"_" + sourceID + "_" + ruptureID + SEISMOGRAM_FILENAME_EXTENSION;
			File lfSeisFile = new File(lfSeisName);
			lfSeisFile.setTransfer(TRANSFER.TRUE);
			lfSeisFile.setRegister(false);
			job.uses(lfSeisFile, LINK.INOUT);
			job.addArgument("lf_seis=" + lfSeisFile.getName());
		}
		
		String hfSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION;
		File hfSeisFile = new File(hfSeisName);
		hfSeisFile.setTransfer(TRANSFER.FALSE);
		hfSeisFile.setRegister(false);
		job.uses(hfSeisFile, LINK.INPUT);
		job.addArgument("hf_seis=" + hfSeisFile.getName());
		
		String mergedSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
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
		if (psaFilter<2.0*riq.getLowFrequencyCutoff()) {
			psaFilter = 2.0*riq.getLowFrequencyCutoff();
		}
		
		int nt = (int)((sParams.getTlen()/DT + 0.5));
		
       	job.addArgument("simulation_out_pointsX=2"); //2 b/c 2 components
    	job.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job.addArgument("simulation_out_timesamples="+nt);// numTimeSteps
    	job.addArgument("simulation_out_timeskip="+ DT); //dt
    	job.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job.addArgument("surfseis_rspectra_output_type=aa");
    	job.addArgument("surfseis_rspectra_period=all");
    	job.addArgument("surfseis_rspectra_apply_filter_highHZ="+psaFilter);
    	job.addArgument("surfseis_rspectra_apply_byteswap=no");
    	
    	String psaFilename = dirPrefix + java.io.File.separator + PSA_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + 
    			"_" + sourceID + "_" + ruptureID + "_bb" + PSA_FILENAME_EXTENSION;
    	File psaFile = new File(psaFilename);
    	psaFile.setRegister(true);
    	psaFile.setTransfer(TRANSFER.TRUE);
    	job.uses(psaFile, LINK.OUTPUT);    	
    	job.addArgument("out=" + psaFile.getName());
    	
    	//RotD
		if (sParams.isRunRotd()) {
			job.addArgument("run_rotd=1");
			String rotDFilename = dirPrefix + java.io.File.separator + ROTD_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + 
	    			"_" + sourceID + "_" + ruptureID + "_bb" + ROTD_FILENAME_EXTENSION;
			File rotDFile = new File(rotDFilename);
			rotDFile.setRegister(true);
			rotDFile.setTransfer(TRANSFER.TRUE);
			job.uses(rotDFile, LINK.OUTPUT);
			job.addArgument("rotd_out=" + rotDFile.getName());
		} else {
			job.addArgument("run_rotd=0");
		}
		
		//Duration
		if (sParams.isRunDuration()) {
			job.addArgument("run_duration=1");
			String durationFilename = dirPrefix + java.io.File.separator + DURATION_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + 
    			"_" + sourceID + "_" + ruptureID + "_bb" + DURATION_FILENAME_EXTENSION;
			File durFile = new File(durationFilename);
			durFile.setRegister(true);
			durFile.setTransfer(TRANSFER.TRUE);
			job.uses(durFile, LINK.OUTPUT);
			job.addArgument("duration_out=" + durFile.getName());
		} else {
			job.addArgument("run_duration=0");
		}
		
		job.addProfile("pegasus", "label", "pmc");
		//Must read in both rupture files at once
		double lfMem = numRupVars * 2.0 * nt;
		double hfMem = numRupVars * 2.0 * nt;
		double totMem = (int)Math.ceil(1.1*(lfMem + hfMem)/(1024.0*1024.0));
		job.addProfile("pegasus", "pmc_request_memory", "" + totMem);
		
		return job;
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
	
    private double[] processVelocityFile(String velocityFilename) {
    	try {
    		BufferedReader br = new BufferedReader(new FileReader(velocityFilename));
    		/*File format is
    		 * Vs at site = <Vs at surface>
    		 * Vs30 = <Vs30>
    		 * Vs<D> = <VsD>
    		 */
    		double vs30 = Double.parseDouble(br.readLine().split("=")[1]);
    		double vs5H = Double.parseDouble(br.readLine().split("=")[1]);
    		double vsD5H = Double.parseDouble(br.readLine().split("=")[1]);
    		System.out.println("vs30=" + vs30 + ", vs5H=" + vs5H + ", vsD5H=" + vsD5H);
    		br.close();
    		double[] retArray = new double[3];
    		retArray[VS30] = vs30;
    		retArray[VS5H] = vs5H;
    		retArray[VSD5H] = vsD5H;
    		return retArray;
    	} catch (IOException ie) {
    		ie.printStackTrace();
    		System.exit(1);
    	}
    	return null;
    }
    
	private void createJobs(ADAG dax, ResultSet ruptureSet, String velocityFilename) {
		double[] vsArray = processVelocityFile(velocityFilename);
		try {
      		// Create update run state job
      		Job updateJob = addUpdate(riq.getRunID(), "PP_INIT", "PP_START");
      		dax.addJob(updateJob);
			
			String localVMFilename = VM_FILENAME + ".local";
			Job localVMJob = createLocalVMJob(VM_FILENAME, localVMFilename);
			dax.addJob(localVMJob);
			dax.addDependency(updateJob, localVMJob);
			
			HashSet<String> dirNames = new HashSet<String>();
			String dirsInputFilename = "dirs_list.txt";
			Job dirsJob = createDirsJob(dirsInputFilename);
			dax.addJob(dirsJob);
			dax.addDependency(updateJob, dirsJob);
			
			int index = 0;
			
			while (!ruptureSet.isAfterLast()) {
				if ((index+1)%1000==0) {
					System.out.println((index+1) + " ruptures processed.");
				}
				int sourceID = ruptureSet.getInt("R.Source_ID");
				int ruptureID = ruptureSet.getInt("R.Rupture_ID");
				int numPoints = ruptureSet.getInt("R.Num_Points");
				int numRupVars = ruptureSet.getInt("count(*)");
				int numRows = ruptureSet.getInt("R.Num_Rows");
				int numCols = ruptureSet.getInt("R.Num_Columns");
			
				dirNames.add("" + sourceID);
				
				//Handle dependences in the method, because we might be using multiple tasks per rupture
				Job hfSynthJob = createHFSynthJob(sourceID, ruptureID, numRupVars, numPoints, numRows, numCols, localVMFilename, localVMJob, dirsJob, dax, vsArray);
				dax.addJob(hfSynthJob);
				
				Job mergeIMJob = createMergeIMJob(sourceID, ruptureID, numRupVars, numPoints);
				dax.addJob(mergeIMJob);
				dax.addDependency(hfSynthJob, mergeIMJob);

				if (sParams.isRunLFSiteResponse()) {
					Job lfSiteResponseJob = createLFSiteResponseJob(sourceID, ruptureID, numRupVars, numPoints, vsArray);
					dax.addJob(lfSiteResponseJob);
					dax.addDependency(dirsJob, lfSiteResponseJob);
					dax.addDependency(lfSiteResponseJob, mergeIMJob);
				}
				
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
}
