package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.DAX;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;
import edu.isi.pegasus.planner.dax.File.LINK;
import edu.isi.pegasus.planner.dax.File.TRANSFER;


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
	private final static String COMBINED_SEISMOGRAM_FILENAME_PREFIX = "Seismogram_";
    private final static String COMBINED_SEISMOGRAM_FILENAME_EXTENSION = ".grm";
    private final static String COMBINED_PEAKVALS_FILENAME_PREFIX = "PeakVals_";
    private final static String COMBINED_PEAKVALS_FILENAME_EXTENSION = ".bsa";
    
//  private final static String TMP_FS = "/lustre/scratch/tera3d/tmp";
    private final static String TMP_FS = "/dev/shm";
    private final static String FD_PATH = "/proc/self/fd";
    private final static String SEISMOGRAM_ENV_VAR = "GRM";
    private final static String PEAKVALS_ENV_VAR = "PSA";
    //Output directory for Study 13.4
//    private final static String OUTPUT_DIR = "/home/scec-02/tera3d/CyberShake2007/data/PPFiles";
    private final static String OUTPUT_DIR = "/home/scec-04/tera3d/CyberShake/data/PPFiles";
	
	//Job names
    private final static String UPDATERUN_NAME = "UpdateRun";
    private final static String CYBERSHAKE_NOTIFY_NAME = "CyberShakeNotify";
    private final static String CHECK_SGT_NAME = "CheckSgt";
    private final static String ZIP_SEIS_NAME = "ZipSeismograms";
    private final static String ZIP_PSA_NAME = "ZipPeakSA";
    private final static String ZIP_COMBINED_NAME = "ZipCombined";
    private final static String EXTRACT_SGT_NAME = "extract_sgt";
    private final static String SEISMOGRAM_SYNTHESIS_NAME = "seismogram_synthesis";
    private final static String PEAK_VAL_CALC_NAME = "PeakValCalc_Okaya";
    private final static String SEIS_PSA_NAME = "Seis_PSA";
    private final static String SEIS_PSA_MEMCACHED_NAME = "Seis_PSA_memcached";
    private final static String SEIS_PSA_HEADER_NAME = "Seis_PSA_header";
    private final static String SEIS_PSA_HIGH_MEM_NODES_NAME = "Seis_PSA_high_mem";
    private final static String SEIS_PSA_LARGE_MEM_NAME = "Seis_PSA_large_mem";
    private final static String SEIS_PSA_MULTI_NAME = "Seis_PSA_multi";
    private final static String LOCAL_VM_NAME = "Local_VM";
    private final static String STOCH_NAME = "srf2stoch";
    private final static String HIGH_FREQ_NAME = "HighFrequency";
    private final static String MERGE_NAME = "MergeFrequency"; 
    private final static String HF_SYNTH_NAME = "HF_Synth";
    private final static String MERGE_PSA_NAME = "MergePSA";
    private final static String EXTRACT_SGT_MPI_NAME = "Extract_SGT_MPI";
    private final static String EXTRACT_SGT_MPI_AWP_NAME = "Extract_SGT_MPI_AWP";
	
    //Simulation parameters
    private static String NUMTIMESTEPS = "3000";
    private static String LF_TIMESTEP = "0.1";
    private final static String SPECTRA_PERIOD1 = "all";
    private final static String FILTER_HIGHHZ = "5.0";
    private static String SEIS_LENGTH = "300.0";
    private final static String HF_DT = "0.025";

    private final static int LARGE_MEM_BUF = 2*1024;
    
	//Database
    private final static String DB_SERVER = "focal.usc.edu";
    private final static String DB = "CyberShake";
    private final static String USER = "cybershk_ro";
    private final static String PASS = "CyberShake2007";
    private final static String pass_file = "focal.txt";
    private static DBConnect dbc;
     
    //SQLite DB
    private RuptureVariationDB sqlDB = null;
    
    //Instance variables
    private PP_DAXParameters params;
    private RunIDQuery riq;
    private String localVMFilename;
    private Job localVMJob = null;
    private CyberShake_Workflow_Container wfContainer = null;

	
    //Class for load balancing
    private class RuptureEntry {
    	int sourceID;
    	int ruptureID;
    	int numRupPoints;
    	
    	public RuptureEntry(int s, int r, int n) {
    		sourceID = s;
    		ruptureID = r;
    		numRupPoints = n;
    	}
    }
      
    public static void main(String[] args) {
    	CyberShake_Workflow_Container cont = subMain(args, true);
    	
    	//Create top-level DAX, set up dependencies, and populate with sub-workflows
    	String siteName = cont.getRIQ().getSiteName();
    	
    	ADAG topLevelDax = new ADAG(DAX_FILENAME_PREFIX + siteName, 0, 1);
    	
    	//PRE workflow
    	String preDAXFilename = cont.getPreWorkflow();
    	DAX preD = new DAX("preDAX", preDAXFilename);
		preD.addArgument("--force");
		preD.addArgument("-q");
		//Add the dax to the top-level dax like a job
		topLevelDax.addDAX(preD);
		//Create a file object
		File preDFile = new File(preDAXFilename);
		preDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + preDAXFilename, "local");
		topLevelDax.addFile(preDFile);
		
		//subWfs
		ArrayList<String> subWfs = cont.getSubWorkflows();
		for (int i=0; i<subWfs.size(); i++) {
			String filename = subWfs.get(i);
			DAX jDax = new DAX("dax_" + i, filename);
			if (cont.getParams().isMPICluster()) {
				jDax.addArgument("--cluster label");
			} else {
				jDax.addArgument("--cluster horizontal");
			}
			//Makes sure it doesn't prune workflow elements
			jDax.addArgument("--force");
			jDax.addArgument("-q");
			//Force stage-out of zip files
			jDax.addArgument("--output shock");
			jDax.addArgument("--output-dir " + OUTPUT_DIR + "/" + siteName + "/" + cont.getRIQ().getRunID());
			jDax.addProfile("dagman", "category", "subwf");
			topLevelDax.addDAX(jDax);
			topLevelDax.addDependency(preD, jDax);
			File jDaxFile = new File(filename);
			jDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + filename, "local");
			topLevelDax.addFile(jDaxFile);
		}
		
		//DB
		String dbDAXFile = cont.getDBWorkflow();
		DAX dbDax = new DAX("dbDax", dbDAXFile);
		dbDax.addArgument("--force");
		dbDax.addArgument("-q");
		topLevelDax.addDAX(dbDax);
		for (int i=0; i<subWfs.size(); i++) {
			topLevelDax.addDependency("dax_" + i, "dbDax");
		}	
		File dbDaxFile = new File(dbDAXFile);
		dbDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + dbDAXFile, "local");
		topLevelDax.addFile(dbDaxFile);
		
		//Post
		String postDAXFile = cont.getPostWorkflow();
		DAX postD = new DAX("postDax", postDAXFile);
		postD.addArgument("--force");
		postD.addArgument("-q");
		topLevelDax.addDAX(postD);
		if (cont.getParams().getInsert()) {
			topLevelDax.addDependency(dbDax, postD);
		} else {
			for (int i=0; i<subWfs.size(); i++) {
				topLevelDax.addDependency("dax_" + i, "postDax");
			}	
		}
		File postDFile = new File(postDAXFile);
		postDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + postDAXFile, "local");
		topLevelDax.addFile(postDFile);

		String topLevelDaxName = DAX_FILENAME_PREFIX + siteName + DAX_FILENAME_EXTENSION;
		topLevelDax.writeToFile(topLevelDaxName);
		
    }
    
    public static CyberShake_Workflow_Container subMain(String[] args, boolean writeDAX) {
    	PP_DAXParameters pp_params = new PP_DAXParameters();
    	int runID = parseCommandLine(args, pp_params);
    	
        CyberShake_PP_DAXGen daxGen = new CyberShake_PP_DAXGen();
        return daxGen.makeDAX(runID, pp_params, writeDAX);
    }
    
    public static int parseCommandLine(String[] args, PP_DAXParameters pp_params) {
    	//Command-line options
    	String usageString = "CyberShake_PP_DAXGen <runID> <directory> ";
    	
        Options cmd_opts = new Options();
        Option help = new Option("h", "help", false, "Print help for CyberShake_PP_DAXGen");
        Option partition = OptionBuilder.withArgName("partitions").hasArg().withDescription("Number of partitions to create.").create("p");
        Option no_insert = new Option("ni", "no-insert", false, "Don't insert ruptures into database (used for testing)");
        Option no_seisPSA = new Option("ns", "no-seispsa", false, "Use separate executables for both synthesis and PSA");
        Option no_hf_synth = new Option("nh", "no-hf-synth", false, "Use separate executables for high-frequency srf2stoch and hfsim, rather than hfsynth");
        Option no_merge_psa = new Option("nm", "no-mergepsa", false, "Use separate executables for merging broadband seismograms and PSA, rather than mergePSA");
        Option high_frequency = OptionBuilder.withArgName("high-frequency").hasOptionalArg().withDescription("Lower-bound frequency cutoff for stochastic high-frequency seismograms (default 1.0), required for high frequency run").create("hf");
        Option sqlIndex = new Option("q", "sql", false, "Create sqlite file containing (source, rupture, rv) to sub workflow mapping");
        Option awp = new Option("a", "awp", false, "Use AWP SGTs");
        Option no_mpi_cluster = new Option("nc", "no-mpi-cluster", false, "Do not use pegasus-mpi-cluster");
        Option zip = new Option("z", "zip", false, "Zip seismogram and PSA files before transferring.");
        Option separate_zip = new Option("s", "separate-zip", false, "Run zip jobs as separate seismogram and PSA zip jobs.");
        Option no_dir_hierarchy = new Option("nh", "no-hierarchy", false, "Use directory hierarchy on compute resource for seismograms and PSA files.");
        Option file_forward = new Option("ff", "file-forward", false, "Use file-forwarding option.  Requires PMC.");
        Option no_forward = new Option("nf", "no-forward", false, "Use no forwarding.");
        Option serial_extract = new Option("se", "serial-extract", false, "Use serial version of extraction code rather than extract SGT MPI.");
        Option global_extract_sgt_mpi = new Option("ge", "global-extract-mpi", false, "Use 1 extract SGT MPI job, run as part of pre workflow.");
        Option frequency = OptionBuilder.withArgName("frequency").hasArg().withDescription("Maximum frequency for deterministic simulation.").create("f");
        Option large_mem = new Option("lm", "large-mem", false, "Use version of SeisPSA which can handle ruptures with large numbers of points.");
        Option multi_rv = OptionBuilder.withArgName("factor").hasArg().withDescription("Use SeisPSA version which supports multiple synthesis tasks per invocation; number of seis_psa jobs per invocation.").create("mr");
        Option source_forward = new Option("sf", "source-forward", false, "Aggregate files at the source level instead of the default rupture level.");
        cmd_opts.addOption(help);
        cmd_opts.addOption(partition);
        cmd_opts.addOption(no_insert);
        cmd_opts.addOption(no_hf_synth);
        cmd_opts.addOption(no_merge_psa);
        cmd_opts.addOption(high_frequency);
        cmd_opts.addOption(sqlIndex);
        cmd_opts.addOption(awp);
        cmd_opts.addOption(no_mpi_cluster);
        cmd_opts.addOption(no_dir_hierarchy);
        cmd_opts.addOption(serial_extract);
        cmd_opts.addOption(global_extract_sgt_mpi);
        cmd_opts.addOption(frequency);
        cmd_opts.addOption(large_mem);
        OptionGroup forwardingGroup = new OptionGroup();
        forwardingGroup.addOption(file_forward);
        forwardingGroup.addOption(no_forward);
        forwardingGroup.addOption(source_forward);
        cmd_opts.addOptionGroup(forwardingGroup);
        cmd_opts.addOption(no_seisPSA);
        cmd_opts.addOption(zip);
        cmd_opts.addOption(separate_zip);
        cmd_opts.addOption(multi_rv);

        CommandLineParser parser = new GnuParser();
        if (args.length<1) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            System.exit(1);
        }
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (AlreadySelectedException ase) {
        	System.err.println("Only 1 of file-forward, no-forward may be selected.");
        	System.exit(3);
        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(2);
        }
        if (line.hasOption(help.getOpt())) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            System.exit(1);
        }
        
        int runID = Integer.parseInt(args[0]);
        String directory = args[1];
        pp_params.setPPDirectory(directory);
        if (line.hasOption(partition.getOpt())) {
            pp_params.setNumOfDAXes(Integer.parseInt(line.getOptionValue("p")));
        }
        if (line.hasOption(no_insert.getOpt())) {
        	pp_params.setInsert(false);
        }
        if (line.hasOption(no_seisPSA.getOpt())) {
        	pp_params.setSeisPSA(false);
        }
        if (line.hasOption(high_frequency.getOpt())) {
        	if (pp_params.isSeisPSA()) {
        		System.out.println("Can't use seisPSA with high-frequency, since we calculate PSA after merging.");
        		System.exit(3);
        	}
        	pp_params.setHighFrequency(true);
        	if (line.getOptionValue("hf")!=null) {
        		pp_params.setHighFrequencyCutoff(Double.parseDouble(line.getOptionValue("hf")));
        	} else {
        		//use 1.0 as default
        		pp_params.setHighFrequencyCutoff(1.0);
        	}
        	if (line.hasOption(no_hf_synth.getOpt())) {
        		pp_params.setHfsynth(false);
        	}
        	if (line.hasOption(no_merge_psa.getOpt())) {
        		pp_params.setMergePSA(true);
        	}
        }
        
        if (line.hasOption(sqlIndex.getOpt())) {
        	pp_params.setRvDB(true);
        }
        if (line.hasOption(awp.getOpt())) {
        	pp_params.setUseAWP(true);
        }
        
        if (line.hasOption(no_mpi_cluster.getOpt())) {
        	pp_params.setMPICluster(false);
        }
        if (line.hasOption(zip.getOpt())) {
        	pp_params.setZip(true);
        }
        if (line.hasOption(separate_zip.getOpt())) {
        	pp_params.setSeparateZip(true);
        	if (!pp_params.isZip()) {
        		System.err.println("Separate zip option requires zip option.");
        		System.exit(-5);
        	}
        }
        if (line.hasOption(no_dir_hierarchy.getOpt())) {
        	pp_params.setDirHierarchy(false);
        }

        if (line.hasOption(file_forward.getOpt())) {
        	if (!pp_params.isMPICluster()) {
        		System.err.println("Need to use pegasus-mpi-cluster in order to use file forwarding.  Ignoring file forwarding option.");
        	} else {
        		pp_params.setFileForward(true);
        		if (pp_params.isZip()) {
            		System.out.println("Since we are using file forwarding, turning off zipping.");
            		pp_params.setZip(false);
            		pp_params.setSeparateZip(false);
            	}
        	}
        } else if (line.hasOption(no_forward.getOpt())) {
        	pp_params.setPipeForward(false);
        	pp_params.setFileForward(false);
        }
        
        if (line.hasOption(serial_extract.getOpt())) {
        	System.out.println("Using jbsim3d instead of extract_sgt_mpi.");
        	pp_params.setExtractSGTMPI(false);
        }
        
        if (line.hasOption(global_extract_sgt_mpi.getOpt())) {
        	pp_params.setGlobalExtractSGTMPI(true);
        }
        
        if (line.hasOption(frequency.getOpt())) {
        	pp_params.setDetFrequency(Double.parseDouble(line.getOptionValue("f")));
        	//Adjust a few parameters accordingly
        	NUMTIMESTEPS = "" + (int)(4000*pp_params.getDetFrequency());
        	LF_TIMESTEP = "" + (0.05/pp_params.getDetFrequency());
        	SEIS_LENGTH = "200.0";
        }
        if (line.hasOption(large_mem.getOpt())) {
        	pp_params.setLargeMemSynth(true);
        }
        
        if (line.hasOption(multi_rv.getOpt())) {
        	pp_params.setUseMultiSeisPSA(true);
        	pp_params.setMultiSeisPSAFactor(Integer.parseInt(line.getOptionValue("mr")));
        }
        
        if (line.hasOption(source_forward.getOpt())) {
        	pp_params.setSourceForward(true);
        }
        
        //Removing notifications
        pp_params.setNotifyGroupSize(pp_params.getNumOfDAXes()+1);
        
        return runID;
	}

	public CyberShake_Workflow_Container makeDAX(int runID, PP_DAXParameters params, boolean writeDAX) {
		try {
			this.params = params;
			//Get parameters from DB and calculate number of variations
			ResultSet ruptureSet = getParameters(runID);
			//bin the ruptures
			ArrayList<RuptureEntry>[] bins = binRuptures(ruptureSet);

//			ADAG topLevelDax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName(), 0, 1);
			wfContainer = new CyberShake_Workflow_Container(riq, params);

			//Check to make sure RV model is consistent with in-memory choice
			//since if we generate rupture variations in memory, we only support RV ID 4
			if (riq.getRuptVarScenID()==3) {
				System.out.println("Disabling in-memory rupture variation generation, since it does not support rupture variation ID 3");
				params.setJbsimRVMem(false);
			}
			
			//populate DB with frequency info
			putFreqInDB();
			
			// Add DAX for checking SGT files
			ADAG preDAX = makePreDAX(riq.getRunID(), riq.getSiteName());
			String preDAXFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_pre" + DAX_FILENAME_EXTENSION;
			preDAX.writeToFile(preDAXFile);
			wfContainer.setPreWorkflow(preDAXFile);

			//The arguments here are daxname, the file that the DAX was written to.
			//This file is also the LFN of the daxfile.  We need to create an LFN, PFN association
			//so that the topLevelDax can find it when we plan.
//			DAX preD = new DAX("preDAX", preDAXFile);
//			preD.addArgument("--force");
//			preD.addArgument("-q");
			//Add the dax to the top-level dax like a job
//			topLevelDax.addDAX(preD);
			//Create a file object.
//			File preDFile = new File(preDAXFile);
//			preDFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + preDAXFile, "local");
//			topLevelDax.addFile(preDFile);
			
			ruptureSet.first();
		
			int sourceIndex, rupIndex, numRupPoints;
			int count = 0;
			int localRupCount = 0;

			int currDax = 0;
			ADAG dax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax, currDax, params.getNumOfDAXes());
  	    
			Job[] zipJobs = null;
			
			if (params.isZip()) {
			}
			
			Job extractSGTMPIJob = null;
			ArrayList<String> extractRuptures = null;
			String ruptureListFilename = null;
			if (params.isExtractSGTMPI()) {
				ruptureListFilename = "rupture_file_list_" + riq.getSiteName() + "_" + currDax;
				extractRuptures = new ArrayList<String>();
				extractSGTMPIJob = addExtractSGTMPIJob(dax, currDax, ruptureListFilename);
			}
			
			int numVarsInDAX = 0;

			if (params.isRvDB()) {
	        	sqlDB = new RuptureVariationDB(riq.getSiteName(), riq.getRunID());
			}
					
			//loop over bins
			ArrayList<RuptureEntry> currBin;
			for (int i=0; i<bins.length; i++) {
				currBin = bins[i];
				for (int j=0; j<currBin.size(); j++) {
					count++;
					if (count%100==0) {
						System.out.println("Added " + count + " ruptures.");
						System.gc();
					}

					sourceIndex = currBin.get(j).sourceID;
					rupIndex = currBin.get(j).ruptureID;
					numRupPoints = currBin.get(j).numRupPoints;

					ResultSet variationsSet = getVariations(sourceIndex, rupIndex);
						
					if (params.isExtractSGTMPI()) {
						//Add file to list of rupture files to use
						String rup_geom_filename = "e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceIndex + "_" + rupIndex + ".txt";
						extractRuptures.add(rup_geom_filename);
							
						//Add extracted SGT files as used by extractSGTMPI job
						//In same working dir, so don't need to be transferred
						File rupsgtxFile = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
				        rupsgtxFile.setTransfer(File.TRANSFER.FALSE);
				        rupsgtxFile.setRegister(false);
				        
				        File rupsgtyFile = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
				        rupsgtyFile.setTransfer(File.TRANSFER.FALSE);
				        rupsgtyFile.setRegister(false);
					}

					if (params.isSourceForward()) {
						
					}
					addRupture(dax, variationsSet, sourceIndex, rupIndex, numRupPoints, count, i, zipJobs, extractSGTMPIJob);						
				}
				if (i<bins.length-1) {
					//Create next dax
					if (params.isExtractSGTMPI()) {
//						dax = createNewDax(preD, i, dax, topLevelDax, extractRuptures, ruptureListFilename);
						dax = createNewDax(i, dax, extractRuptures, ruptureListFilename);
					} else {
//						dax = createNewDax(preD, i, dax, topLevelDax);
						dax = createNewDax(i, dax);
					}
						
					if (params.isZip()) {
						zipJobs = addZipJobs(dax, i+1);
					}
						
					if (params.isExtractSGTMPI()) {
						extractRuptures.clear();
						ruptureListFilename = "rupture_file_list_" + riq.getSiteName() + "_" + (i+1);
						extractSGTMPIJob = addExtractSGTMPIJob(dax, i+1, ruptureListFilename);
					}
				}
				currDax = bins.length-1;
			}

			//Write leftover jobs to file
			if (params.isExtractSGTMPI()) {
				java.io.File javaFile = new java.io.File(params.getPPDirectory() + "/" + ruptureListFilename);
				String fullPath = "";
				try {
					fullPath = javaFile.getCanonicalPath();
					BufferedWriter bw = new BufferedWriter(new FileWriter(javaFile));
					bw.write(extractRuptures.size() + "\n");
					for (String s: extractRuptures) {
						bw.write(s + "\n");
					}
					bw.flush();
					bw.close();
				} catch (IOException ioe) {
					ioe.printStackTrace();
					System.exit(3);
				}
				edu.isi.pegasus.planner.dax.File rupListFile = new File(ruptureListFilename);
				rupListFile.addPhysicalFile("file://" + fullPath);
				dax.addFile(rupListFile);
			}
			
			//Write leftover jobs to file
			String daxFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax + DAX_FILENAME_EXTENSION;
			dax.writeToFile(daxFile);
			
			wfContainer.addSubWorkflow(daxFile);
			
			//Add to topLevelDax
//			DAX jDax = new DAX("dax_" + currDax, daxFile);
//			if (params.isMPICluster()) {
//				jDax.addArgument("--cluster label");
//			} else {
//				jDax.addArgument("--cluster horizontal");
//			}
//			jDax.addArgument("--force");
//			jDax.addArgument("-q");
//			jDax.addArgument("--output shock");
//			jDax.addArgument("--output-dir " + OUTPUT_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
//			topLevelDax.addDAX(jDax);
//			topLevelDax.addDependency(preD, jDax);
//			File jDaxFile = new File(daxFile);
//			jDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + daxFile, "local");
//			topLevelDax.addFile(jDaxFile);
			
			// Add DAX for DB insertion/curve generation
			DAX dbDax = null;
			if (params.getInsert()) {
				ADAG dbProductsDAX = genDBProductsDAX(currDax+1);
				String dbDAXFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_DB_Products" + DAX_FILENAME_EXTENSION;
				dbProductsDAX.writeToFile(dbDAXFile);
				
				wfContainer.setDBWorkflow(dbDAXFile);
				
//				dbDax = new DAX("dbDax", dbDAXFile);
//				dbDax.addArgument("--force");
//				dbDax.addArgument("-q");
//				topLevelDax.addDAX(dbDax);
//				for (int i=0; i<=currDax; i++) {
//					topLevelDax.addDependency("dax_" + i, "dbDax");
//				}	
//				File dbDaxFile = new File(dbDAXFile);
//				dbDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + dbDAXFile, "local");
//				topLevelDax.addFile(dbDaxFile);
			}
			
            // Final notifications
            ADAG postDAX = makePostDAX();
			String postDAXFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_post" + DAX_FILENAME_EXTENSION;
			postDAX.writeToFile(postDAXFile);
			
			wfContainer.setPostWorkflow(postDAXFile);
			
//			DAX postD = new DAX("postDax", postDAXFile);
//			postD.addArgument("--force");
//			postD.addArgument("-q");
//			topLevelDax.addDAX(postD);
//			if (params.getInsert()) {
//				topLevelDax.addDependency(dbDax, postD);
//			} else {
//				for (int i=0; i<=currDax; i++) {
//					topLevelDax.addDependency("dax_" + i, "postDax");
//				}	
//			}
//			File postDFile = new File(postDAXFile);
//			postDFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + postDAXFile, "local");
//			topLevelDax.addFile(postDFile);
//
//			if (writeDAX) {
//				String topLevelDaxName = DAX_FILENAME_PREFIX + riq.getSiteName() + DAX_FILENAME_EXTENSION;
//				topLevelDax.writeToFile(topLevelDaxName);
//			}
//			return topLevelDax;
			return wfContainer;
		} catch (SQLException ex) {
			ex.printStackTrace();
			System.exit(1);
		}
		return null;
	}
			
	private Job addExtractSGTMPIJob(ADAG dax, int currDax, String ruptureListFilename) {
		/*--site SGRTT --l
		at 34.1321 --lon -117.9495 --sgt-x SGRTT_fxraw_336.sgt --sgt-y SGRTT_fyraw_336.sgt --header-x SGRTT_fx_33
		6.sgthead --header-y SGRTT_fy_336.sgthead --erf-id 35 --rup-list SGRTT_rupture_list.txt */
		if (params.isUseAWP()) {
	       	 //Use AWP SGTs;  need to include headers as arguments
			 Job extractSGTMPIJob = null;
			 if (riq.getRuptVarScenID()==5) {
				 //Use version linked with RupGen-api-3.3.1
				 extractSGTMPIJob = new Job("Extract_SGT_MPI_AWP_" + currDax, NAMESPACE, EXTRACT_SGT_MPI_AWP_NAME, "3.3.1");	 
			 } else {
				 extractSGTMPIJob = new Job("Extract_SGT_MPI_AWP_" + currDax, NAMESPACE, EXTRACT_SGT_MPI_AWP_NAME, VERSION);
			 }
			
	 		 extractSGTMPIJob.addArgument("--site " + riq.getSiteName());
			 extractSGTMPIJob.addArgument("--lat " + riq.getLat());
			 extractSGTMPIJob.addArgument("--lon " + riq.getLon());
	  		 
			 String sgtx=riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgt";
		     String sgty=riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgt";
				
		     File sgtXFile = new File(sgtx);
		     File sgtYFile = new File(sgty);
		        
		     extractSGTMPIJob.uses(sgtXFile, LINK.INPUT);
		     extractSGTMPIJob.uses(sgtYFile, LINK.INPUT);

		     extractSGTMPIJob.addArgument("--sgt-x " + sgtx);
		     extractSGTMPIJob.addArgument("--sgt-y " + sgty);
			 
		     extractSGTMPIJob.addArgument("--erf-id " + riq.getErfID());
		     
	       	 String sgtheadx = riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgthead";
	       	 String sgtheady = riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgthead";
	       	 
	       	 File sgtheadxFile = new File(sgtheadx);
	       	 File sgtheadyFile = new File(sgtheady);
	       	 
	       	 extractSGTMPIJob.addArgument("--header-x " + sgtheadxFile.getName());
	         extractSGTMPIJob.addArgument("--header-y " + sgtheadyFile.getName());
	       	 
	         extractSGTMPIJob.uses(sgtheadxFile, File.LINK.INPUT);
	         extractSGTMPIJob.uses(sgtheadyFile, File.LINK.INPUT);
	         
	 		 File ruptureListFile = new File(ruptureListFilename);
			
			 ruptureListFile.setTransfer(TRANSFER.TRUE);
			
			 extractSGTMPIJob.addArgument("--rup-list " + ruptureListFile.getName());
			 extractSGTMPIJob.uses(ruptureListFile, LINK.INPUT);
	         
	         dax.addJob(extractSGTMPIJob);
	         
	         return extractSGTMPIJob;
		}
		
		Job extractSGTMPIJob = null;
		if (riq.getRuptVarScenID()==5) {
			 //Use version linked with RupGen-api-3.3.1
			 extractSGTMPIJob = new Job("Extract_SGT_MPI_AWP_" + currDax, NAMESPACE, EXTRACT_SGT_MPI_NAME, "3.3.1");	 
		} else {
			 extractSGTMPIJob = new Job("Extract_SGT_MPI_AWP_" + currDax, NAMESPACE, EXTRACT_SGT_MPI_NAME, VERSION);
		}
		extractSGTMPIJob = new Job("Extract_SGT_MPI_" + currDax, NAMESPACE, EXTRACT_SGT_MPI_NAME, VERSION);
		
		//Switch to flagged arguments
		
		extractSGTMPIJob.addArgument("--site " + riq.getSiteName());
		extractSGTMPIJob.addArgument("--lat " + riq.getLat());
		extractSGTMPIJob.addArgument("--lon " + riq.getLon());
		
		String sgtx=riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgt";
        String sgty=riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgt";
		
        File sgtXFile = new File(sgtx);
        File sgtYFile = new File(sgty);

        extractSGTMPIJob.uses(sgtXFile, LINK.INPUT);
        extractSGTMPIJob.uses(sgtYFile, LINK.INPUT);

	    extractSGTMPIJob.addArgument("--sgt-x " + sgtx);
	    extractSGTMPIJob.addArgument("--sgt-y " + sgty);
		 
	    extractSGTMPIJob.addArgument("--erf-id " + riq.getErfID());
	     
		File ruptureListFile = new File(ruptureListFilename);
		
		ruptureListFile.setTransfer(TRANSFER.TRUE);
		
		extractSGTMPIJob.addArgument("--rup-list " + ruptureListFile.getName());

		extractSGTMPIJob.uses(ruptureListFile, LINK.INPUT);
		
		dax.addJob(extractSGTMPIJob);
		
		return extractSGTMPIJob;
	}

	public ADAG createNewDax(int currDax, ADAG dax, ArrayList<String> extractRuptures, String ruptureListFilename) {
		java.io.File javaFile = new java.io.File(params.getPPDirectory() + "/" + ruptureListFilename);
		String fullPath = "";
		try {
			fullPath = javaFile.getCanonicalPath();
			BufferedWriter bw = new BufferedWriter(new FileWriter(javaFile));
			bw.write(extractRuptures.size() + "\n");
			for (String s: extractRuptures) {
				bw.write(s + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(3);
		}
		edu.isi.pegasus.planner.dax.File rupListFile = new File(ruptureListFilename);
		rupListFile.addPhysicalFile("file://" + fullPath);
		dax.addFile(rupListFile);
		
		return createNewDax(currDax, dax);
	}
	
	public ADAG createNewDax(DAX preDax, int currDax, ADAG dax, ADAG topLevelDax, ArrayList<String> extractRuptures, String ruptureListFilename) {
		java.io.File javaFile = new java.io.File(params.getPPDirectory() + "/" + ruptureListFilename);
		String fullPath = "";
		try {
			fullPath = javaFile.getCanonicalPath();
			BufferedWriter bw = new BufferedWriter(new FileWriter(javaFile));
			bw.write(extractRuptures.size() + "\n");
			for (String s: extractRuptures) {
				bw.write(s + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(3);
		}
		edu.isi.pegasus.planner.dax.File rupListFile = new File(ruptureListFilename);
		rupListFile.addPhysicalFile("file://" + fullPath);
		dax.addFile(rupListFile);
		
		return createNewDax(preDax, currDax, dax, topLevelDax);
	}
	
	public ADAG createNewDax(int currDax, ADAG dax) {
		System.out.println("Writing dax " + currDax);
		String daxFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax + DAX_FILENAME_EXTENSION;
		dax.writeToFile(daxFile);
		wfContainer.addSubWorkflow(daxFile);
		//Add to topLevelDax
//		DAX jDax = new DAX("dax_" + currDax, daxFile);
//		if (params.isMPICluster()) {
//			jDax.addArgument("--cluster label");
//		} else {
//			jDax.addArgument("--cluster horizontal");
//		}
//		//Makes sure it doesn't prune workflow elements
//		jDax.addArgument("--force");
//		jDax.addArgument("-q");
//		//Force stage-out of zip files
//		jDax.addArgument("--output shock");
//		jDax.addArgument("--output-dir " + OUTPUT_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
//		jDax.addProfile("dagman", "category", "subwf");
//		topLevelDax.addDAX(jDax);
//		topLevelDax.addDependency(preDax, jDax);
//		File jDaxFile = new File(daxFile);
//		jDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + daxFile, "local");
//		topLevelDax.addFile(jDaxFile);
					
		ADAG newDax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + (currDax+1), currDax+1, params.getNumOfDAXes());
		//create new set of zip jobs			
		return newDax;
	}
	
	public ADAG createNewDax(DAX preDax, int currDax, ADAG dax, ADAG topLevelDax) {
		System.out.println("Writing dax " + currDax);
		String daxFile = DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + currDax + DAX_FILENAME_EXTENSION;
		dax.writeToFile(daxFile);
		//Add to topLevelDax
		DAX jDax = new DAX("dax_" + currDax, daxFile);
		if (params.isMPICluster()) {
			jDax.addArgument("--cluster label");
		} else {
			jDax.addArgument("--cluster horizontal");
		}
		//Makes sure it doesn't prune workflow elements
		jDax.addArgument("--force");
		jDax.addArgument("-q");
		//Force stage-out of zip files
		jDax.addArgument("--output shock");
		jDax.addArgument("--output-dir " + OUTPUT_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
		jDax.addProfile("dagman", "category", "subwf");
		topLevelDax.addDAX(jDax);
		topLevelDax.addDependency(preDax, jDax);
		File jDaxFile = new File(daxFile);
		jDaxFile.addPhysicalFile("file://" + params.getPPDirectory() + "/" + daxFile, "local");
		topLevelDax.addFile(jDaxFile);
					
		ADAG newDax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_" + (currDax+1), currDax+1, params.getNumOfDAXes());
		//create new set of zip jobs			
		return newDax;
	}

	
	public void addRupture(ADAG dax, ResultSet variationsSet, int sourceIndex, int rupIndex, int numRupPoints, int count, int currDax, Job[] zipJobs, Job extractSGTMPIJob) {
		try {
			Job extractJob = null;

			variationsSet.first();
			
			if (!params.isGlobalExtractSGTMPI() && !params.isExtractSGTMPI()) {
				//Insert extraction job
				extractJob = createExtractJob(sourceIndex, rupIndex, numRupPoints, variationsSet.getString("Rup_Var_LFN"), count, currDax);
				dax.addJob(extractJob);
			}

			int rupvarcount = 0;
			//Iterate over variations
			HashMap<Integer, String> ruptureVariationMap = null;
			int rvClusterIndex = 0;
			while (!variationsSet.isAfterLast()) {
				//Add entry to SQL DB
				if (params.isRvDB()) {
					sqlDB.addMapping(sourceIndex, rupIndex, rupvarcount, currDax);
				}
				if (params.isMergePSA()) {
					Job seisPSAJob = null;
					if (params.isUseMultiSeisPSA()) {
						//Need to accumulate rupture variations
						if (ruptureVariationMap == null) {
							ruptureVariationMap = new HashMap<Integer, String>();
						}
						ruptureVariationMap.put(rupvarcount, variationsSet.getString("Rup_Var_LFN"));
						
						if (ruptureVariationMap.size()==params.getMultiSeisPSAFactor() || variationsSet.isLast()) {
							seisPSAJob = createMultiSeisPSAJob(sourceIndex, rupIndex, ruptureVariationMap, numRupPoints, count, currDax, rvClusterIndex);
							dax.addJob(seisPSAJob);
							ruptureVariationMap.clear();
							rvClusterIndex++;
						} else {
							//Don't want to add dependencies yet, didn't create a job
							rupvarcount++;
							variationsSet.next();
							continue;
						}
						
					} else {
						//add 1 job for seis and PSA
						seisPSAJob = createSeisPSAJob(sourceIndex, rupIndex, rupvarcount, numRupPoints, variationsSet.getString("Rup_Var_LFN"), count, currDax);
						dax.addJob(seisPSAJob);
					}
					if (params.isExtractSGTMPI()) {
						dax.addDependency(extractSGTMPIJob, seisPSAJob);
						} else if (!params.isGlobalExtractSGTMPI()) {
						//set up dependencies
						dax.addDependency(extractJob, seisPSAJob);
					}
					//make the zip jobs appropriate children
					if (params.isZip()) {
						for (Job zipJob: zipJobs) {
							dax.addDependency(seisPSAJob, zipJob);
						}
					}
				} else {
					//create and add seismogram synthesis
					Job seismoJob = createSeismogramJob(sourceIndex, rupIndex, rupvarcount, numRupPoints, variationsSet.getString("Rup_Var_LFN"), count, currDax);
					dax.addJob(seismoJob);
					if (!params.isGlobalExtractSGTMPI()) {
						dax.addDependency(extractJob, seismoJob);
					}
					//if HF jobs, add here
					Job mergeJob = null;
					if (params.isHighFrequency()) {
						Job highFreqJob = null;
						if (params.isHfsynth()) {
							//add merged job
							highFreqJob = createHFSynthJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
							dax.addJob(highFreqJob);
							if (!params.isGlobalExtractSGTMPI()) {
								dax.addDependency(extractJob, highFreqJob);
							}
						} else {
							Job stochJob = createStochJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
							dax.addJob(stochJob);
							if (!params.isGlobalExtractSGTMPI()) {
								dax.addDependency(extractJob, stochJob);
							}

							highFreqJob = createHighFrequencyJob(sourceIndex, rupIndex, rupvarcount, numRupPoints, variationsSet.getString("Rup_Var_LFN"), count, currDax);
							dax.addJob(highFreqJob);
							dax.addDependency(stochJob, highFreqJob);
							//								dax.addDependency(localVMJob, highFreqJob);
						}

						if (params.isMergePSA()) {
							mergeJob = createMergePSAJob(sourceIndex, rupIndex, rupvarcount, numRupPoints, variationsSet.getString("Rup_Var_LFN"), count, currDax);
							dax.addJob(mergeJob);
							dax.addDependency(highFreqJob, mergeJob);							
							dax.addDependency(seismoJob, mergeJob);
							//make the zip jobs appropriate children
							if (params.isZip()) {
								for (Job zipJob: zipJobs) {
									dax.addDependency(mergeJob, zipJob);
								}
							}
						} else {
							mergeJob = createMergeSeisJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
							dax.addJob(mergeJob);
							dax.addDependency(highFreqJob, mergeJob);							
							dax.addDependency(seismoJob, mergeJob);
							Job psaJob = createPSAJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
							dax.addJob(psaJob);
							dax.addDependency(mergeJob, psaJob);
							//make the zip jobs appropriate children
							if (params.isZip()) {
								if (params.isSeparateZip()) {
									dax.addDependency(psaJob, zipJobs[0]);
								} else {
									dax.addDependency(mergeJob, zipJobs[0]);
									dax.addDependency(psaJob, zipJobs[1]);
								}
							}
						}
					} else {
						//create and add PSA
						Job psaJob = createPSAJob(sourceIndex, rupIndex, rupvarcount, variationsSet.getString("Rup_Var_LFN"), count, currDax);
						dax.addJob(psaJob);
						//set up dependencies
						dax.addDependency(seismoJob, psaJob);
						//make the zip jobs appropriate children
						if (params.isZip()) {
							if (params.isSeparateZip()) {
								dax.addDependency(psaJob, zipJobs[0]);
							} else {
								dax.addDependency(seismoJob, zipJobs[0]);
								dax.addDependency(psaJob, zipJobs[1]);
							}
						}
					}
				}
				rupvarcount++;
				variationsSet.next();
			}
		} catch (SQLException sqex) {
			sqex.printStackTrace();
			System.exit(2);
		}
	}


	private double estimateRuntime(int numVariations, int numRupturePoints, double mag) {
		//From Ranger estimates
		//return numVariations*(0.45*Math.pow(1.00033, numRupturePoints));
		//From Kraken estimates
		//double extractTime = 0.603*Math.pow(numRupturePoints, 0.839);
		//2.0 is here because read_sgt is on average half the runtime
		//double seisPSATime = numVariations*(2.0*0.00129*Math.pow(numRupturePoints, 0.926));
		//From Blue Waters estimates
		return numVariations*Math.pow(((double)numRupturePoints)/6000.0, 1.588)*mag;
		//return extractTime + seisPSATime;
	}

	private ArrayList<RuptureEntry>[] binRuptures(ResultSet ruptureSet) {
		try {
			ArrayList<RuptureEntry>[] bins = new ArrayList[params.getNumOfDAXes()];
			int i, sourceIndex, rupIndex, numRupPoints, numVars;
			double mag;
			double[] runtimes = new double[params.getNumOfDAXes()];
			ruptureSet.first();
			//Initialize bins
			for (i=0; i<bins.length; i++) {
				bins[i] = new ArrayList<RuptureEntry>();
				sourceIndex = ruptureSet.getInt("Source_ID");
				rupIndex = ruptureSet.getInt("Rupture_ID");
				numRupPoints = ruptureSet.getInt("Num_Points");
				mag = ruptureSet.getDouble("Mag");
				ResultSet variationsSet = getNumVariations(sourceIndex, rupIndex);
				numVars = variationsSet.getInt("count(*)");
				bins[i].add(new RuptureEntry(sourceIndex, rupIndex, numRupPoints));
				runtimes[i] = estimateRuntime(numVars, numRupPoints, mag);
				ruptureSet.next();
			}
			while (!ruptureSet.isAfterLast()) {
				//find shortest bin
				double shortestVal = runtimes[0];
				int shortestBin = 0;
				for (i=1; i<runtimes.length; i++) {
					if (runtimes[i]<shortestVal) {
						shortestBin = i;
					}
				}
				sourceIndex = ruptureSet.getInt("Source_ID");
				rupIndex = ruptureSet.getInt("Rupture_ID");
				numRupPoints = ruptureSet.getInt("Num_Points");
				mag = ruptureSet.getDouble("Mag");
				ResultSet variationsSet = getNumVariations(sourceIndex, rupIndex);
				numVars = variationsSet.getInt("count(*)");
				bins[shortestBin].add(new RuptureEntry(sourceIndex, rupIndex, numRupPoints));
				runtimes[shortestBin] += estimateRuntime(numVars, numRupPoints, mag);
				ruptureSet.next();
			}
			for (i=0; i<bins.length; i++) {
				System.out.println("Bin " + i + " has " + bins[i].size() + " ruptures, est. runtime " + runtimes[i]);
			}
			return bins;
		} catch (SQLException sqe) {
			sqe.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	private void putFreqInDB() {
		//read info from passwd file
		String pass = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(pass_file));
			pass = br.readLine().trim();
			br.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		if (pass==null) {
			System.err.println("Couldn't find a write password for db " + DB);
			System.exit(3);
		}
		DBConnect dbc = new DBConnect(DB_SERVER, DB, "cybershk", pass);
		String update = "update CyberShake_Runs set Max_Frequency=";
		if (params.isHighFrequency()) {
			update += params.getMaxHighFrequency() + ", Low_Frequency_Cutoff=" + params.getHighFrequencyCutoff();
		} else {
			update += "0.5, Low_Frequency_Cutoff=0.5";
		}
		update += " where Run_ID=" + riq.getRunID();
		dbc.insertData(update);
		dbc.closeConnection();
	}


	private ADAG makePostDAX() {
     	try {
    	    ADAG postDax = new ADAG(DAX_FILENAME_PREFIX + riq.getSiteName() + "_post", 0, 1);
    	    
    	    // Create update run state job
    	    Job updateJob = addUpdate(postDax, riq.getRunID(), "PP_START", "PP_END");

    	    
          	return postDax;
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	return null;
	}

	private ADAG genDBProductsDAX(int numSubDAXes) {
		CyberShake_DB_DAXGen gen = new CyberShake_DB_DAXGen(riq, params, numSubDAXes, params.isZip());
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
		//Sort on reverse # of points
		String query = "select R.Source_ID, R.Rupture_ID, R.Num_Points, R.Mag " +
		"from CyberShake_Site_Ruptures SR, CyberShake_Sites S, Ruptures R " +
		"where S.CS_Short_Name=\"" + stationName + "\" " +
		"and SR.CS_Site_ID=S.CS_Site_ID " +
		"and SR.ERF_ID=" + riq.getErfID() + " " +
		"and SR.ERF_ID=R.ERF_ID " + 
		"and SR.Source_ID=R.Source_ID " +
		"and SR.Rupture_ID=R.Rupture_ID " +
		"order by R.Num_Points desc";

		ResultSet rs = dbc.selectData(query);
		try {
			rs.first();
		 	if (rs.getRow()==0) {
	      	    System.err.println("No ruptures found for site " + stationName + ".");
	      	    System.exit(1);
	      	}
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(2);
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
      		//Skip notify
      		//Job notifyJob = addNotify(preDax, stationName, CHECK_SGT_NAME, 0, 0);
	    
      		/// Make md5 check jobs children of update job
      		preDax.addDependency(updateJob, checkSgtXJob);
      		preDax.addDependency(updateJob, checkSgtYJob);

      		// Make notify job child of the two md5 check jobs
      		//preDax.addDependency(checkSgtXJob, notifyJob);
      		//preDax.addDependency(checkSgtYJob, notifyJob);
	    	
			if (params.isHighFrequency()) {
				//create local velocity model file for everyone to use
				String vmFile = getVM();
				localVMFilename = vmFile + ".local";
				localVMJob = createLocalVMJob(vmFile, localVMFilename);
				preDax.addJob(localVMJob);
			}
      		
      		if (params.isGlobalExtractSGTMPI()) {
      			Job extractMPIJob = new Job("Extract_SGT_MPI", NAMESPACE, EXTRACT_SGT_MPI_NAME, VERSION);
      			
      			extractMPIJob.addArgument(riq.getSiteName());
      			extractMPIJob.addArgument("" + riq.getLat());
      			extractMPIJob.addArgument("" + riq.getLon());

      			String sgtx=riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgt";
      	        String sgty=riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgt";
      	        
      	        File sgtXFile = new File(sgtx);
      	        File sgtYFile = new File(sgty);
      	        
      	        extractMPIJob.uses(sgtXFile, LINK.INPUT);
      	        extractMPIJob.uses(sgtYFile, LINK.INPUT);
      	        
      			extractMPIJob.addArgument(sgtx);
      			extractMPIJob.addArgument(sgty);
      			
      			extractMPIJob.addArgument("" + riq.getErfID());
      			
      			if (params.isDirHierarchy()) {
      				extractMPIJob.addArgument("-dh");
      			}
      			
      			preDax.addJob(extractMPIJob);
      			
      			preDax.addDependency(checkSgtXJob, extractMPIJob);
      			preDax.addDependency(checkSgtYJob, extractMPIJob);
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
		//If using AWP files, then also add sgthead files to uses.
		//They're not actually used by the job, but this forces them to be transferred in.
		if (params.isUseAWP()) {
			File sgtHeadFile = new File(site + "_f" + component + "_" + riq.getRunID() + ".sgthead"); 
			checkJob.uses(sgtHeadFile, File.LINK.INOUT);
			sgtHeadFile.setRegister(true);
		}

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
 
    
    private Job[] addZipJobs(ADAG dax, int daxValue) {
       	File zipSeisFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + daxValue + "_seismograms.zip");
    	zipSeisFile.setTransfer(File.TRANSFER.TRUE);
    	zipSeisFile.setRegister(true);
    	
    	File zipPSAFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + daxValue + "_PSA.zip");
    	zipPSAFile.setTransfer(File.TRANSFER.TRUE);
    	zipPSAFile.setRegister(true);
    	
    	if (params.isSeparateZip()) {
    		String id4 = "ZipCombined_" + daxValue;
    		Job zipCombinedJob = new Job(id4, NAMESPACE, ZIP_COMBINED_NAME, VERSION);
    		dax.addJob(zipCombinedJob);
    		zipCombinedJob.addArgument(".");
    		zipCombinedJob.addArgument(zipSeisFile);
    		zipCombinedJob.addArgument(zipPSAFile);
    		zipCombinedJob.uses(zipSeisFile, File.LINK.OUTPUT);
    		zipCombinedJob.uses(zipPSAFile, File.LINK.OUTPUT);
    		
    		return new Job[]{zipCombinedJob};
    	}
    	
    	String id4 = "ZipSeis_" + daxValue;
    	Job zipSeisJob = new Job(id4, NAMESPACE, ZIP_SEIS_NAME, VERSION);
    	dax.addJob(zipSeisJob);
    	
    	zipSeisJob.addArgument(".");
    	zipSeisJob.addArgument(zipSeisFile);
    	zipSeisJob.uses(zipSeisFile, File.LINK.OUTPUT);
    	
    	String id5 = "ZipPSA_" + daxValue;
    	Job zipPSAJob = new Job(id5, NAMESPACE, ZIP_PSA_NAME, VERSION);
    	dax.addJob(zipPSAJob);

    	zipPSAJob.addArgument(".");
    	zipPSAJob.addArgument(zipPSAFile);
    	zipPSAJob.uses(zipPSAFile, File.LINK.OUTPUT);

    	if (params.isMPICluster() && !params.isSeparateZip()) {
    		zipSeisJob.addProfile("pegasus", "label", "" + daxValue);
    		zipPSAJob.addProfile("pegasus", "label", "" + daxValue);
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
	
	private ResultSet getNumVariations(int sourceIndex, int rupIndex) {
		String query = "select count(*) " +
			"from Rupture_Variations " +
			"where Rup_Var_Scenario_ID=" + riq.getRuptVarScenID() +
			" and ERF_ID=" + riq.getErfID() +
			" and Source_ID=" + sourceIndex +
			" and Rupture_ID=" + rupIndex;
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
	

	private Job createExtractJob(int sourceIndex, int rupIndex, int numRupPoints, String rupVarLFN, int rupCount, int currDax) {
        /**
     	* Add the sgt extraction job
     	*
     	*/
        String id1 = "ID1_" + sourceIndex+"_"+rupIndex;
        String name = EXTRACT_SGT_NAME + "_memcached";
        if (params.isJbsimRVMem()) {
        	name = EXTRACT_SGT_NAME + "_rv_in_mem";
        }
        
        Job job1 = new Job(id1, NAMESPACE, name, VERSION);

        job1.addArgument("stat="+riq.getSiteName());
        job1.addArgument("slon="+riq.getLon());
        job1.addArgument("slat="+riq.getLat());
        job1.addArgument("extract_sgt=1");

        String sgtx=riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgt";
        String sgty=riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgt";
             
        File sgtxFile = new File(sgtx);
        File sgtyFile = new File(sgty);
             
        File rupsgtxFile = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
        rupsgtxFile.setTransfer(File.TRANSFER.FALSE);
        rupsgtxFile.setRegister(false);
        File rupsgtyFile = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
        rupsgtyFile.setTransfer(File.TRANSFER.FALSE);
        rupsgtyFile.setRegister(false);
         
        File rupVarFile = new File(rupVarLFN);
         
        if (params.isJbsimRVMem()) {
        	//Don't use rupture file;  instead, use source/rupture/slip/hypo arguments
 			//43_0.txt.variation-s0000-h0000
 			String[] pieces = rupVarLFN.split("-");
 			int slip = Integer.parseInt(pieces[1].substring(1));
 			int hypo = Integer.parseInt(pieces[2].substring(1));
 			job1.addArgument("slip=" + slip);
 			job1.addArgument("hypo=" + hypo);
 			File rup_geom_file = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceIndex + "_" + rupIndex + ".txt");
 			job1.addArgument("rup_geom_file=" + rup_geom_file.getName());
 			job1.uses(rup_geom_file, File.LINK.INPUT);
         } else {
        	 job1.addArgument("rupmodfile=" + rupVarFile.getName());
             job1.uses(rupVarFile,File.LINK.INPUT);
         }
         
         job1.addArgument("sgt_xfile="+ sgtxFile.getName());
         job1.addArgument("sgt_yfile=" + sgtyFile.getName());
         job1.addArgument("extract_sgt_xfile=" + rupsgtxFile.getName());
         job1.addArgument("extract_sgt_yfile=" + rupsgtyFile.getName());
         
         if (params.isUseAWP()) {
        	 //Use AWP SGTs;  need to include headers as arguments
        	 String sgtheadx = riq.getSiteName()+"_fx_" + riq.getRunID() + ".sgthead";
        	 String sgtheady = riq.getSiteName()+"_fy_" + riq.getRunID() + ".sgthead";
        	 
        	 File sgtheadxFile = new File(sgtheadx);
        	 File sgtheadyFile = new File(sgtheady);
        	 
        	 job1.addArgument("x_header=" + sgtheadxFile.getName());
        	 job1.addArgument("y_header=" + sgtheadyFile.getName());
        	 
        	 job1.uses(sgtheadxFile, File.LINK.INPUT);
        	 job1.uses(sgtheadyFile, File.LINK.INPUT);
         }
         
         if (params.isDirHierarchy()) {
        	 //extraction job will create if needed and extract here
        	 job1.addArgument("sgtdir=" + sourceIndex + "/" + rupIndex);
         }

         job1.uses(sgtxFile,File.LINK.INPUT);
         job1.uses(sgtyFile,File.LINK.INPUT);
         job1.uses(rupsgtxFile, File.LINK.OUTPUT);
         job1.uses(rupsgtyFile, File.LINK.OUTPUT);

         job1.addProfile("globus", "maxWallTime", "2");
         job1.addProfile("pegasus", "group", "" + rupCount);
         job1.addProfile("pegasus", "label", "" + currDax);
         job1.addProfile("dagman", "category", "extract-jobs");
         
         int extractMem = getExtractMem(numRupPoints);
         
         job1.addProfile("pegasus", "pmc_request_memory", "" + extractMem);
         return job1;
	}
	


	private Job createSeismogramJob(int sourceIndex, int rupIndex, int rupvarcount, int numRupPoints, String rupVarLFN, int count, int currDax) {
		String id2 = "ID2_" + sourceIndex+"_"+rupIndex+"_"+rupvarcount;
		String name = SEISMOGRAM_SYNTHESIS_NAME + "_memcached";
        if (params.isJbsimRVMem()) {
        	name = SEISMOGRAM_SYNTHESIS_NAME + "_rv_in_mem";
        }
		
		Job job2= new Job(id2, NAMESPACE, name, VERSION);
        
		
		File seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + 
			riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
			"_"+ rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);                            
		
		if (params.isHighFrequency()) {
			//add 'lf' to file names
			seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + 
					riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
					"_"+ rupvarcount + "_lf" + SEISMOGRAM_FILENAME_EXTENSION);    
		}
		
		
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

		if (params.isDirHierarchy()) {
			seisFile = new File(sourceIndex + "/" + rupIndex + "/" + SEISMOGRAM_FILENAME_PREFIX + 
			riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
			"_"+ rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);
			
			rupsgtx = new File(sourceIndex + "/" + rupIndex + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
			rupsgty = new File(sourceIndex + "/" + rupIndex + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		}

		
		if (params.isJbsimRVMem()) {
			//Don't use rupture file;  instead, use source/rupture/slip/hypo arguments
			//43_0.txt.variation-s0000-h0000
			String[] pieces = rupVarLFN.split("-");
			int slip = Integer.parseInt(pieces[1].substring(1));
			int hypo = Integer.parseInt(pieces[2].substring(1));
			job2.addArgument("slip=" + slip);
			job2.addArgument("hypo=" + hypo);
 			File rup_geom_file = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceIndex + "_" + rupIndex + ".txt");
 			job2.addArgument("rup_geom_file=" + rup_geom_file.getName());
 			job2.uses(rup_geom_file, File.LINK.INPUT);
		} else {
			job2.addArgument("rupmodfile=" + rupVarFile.getName());
	     	job2.uses(rupVarFile,File.LINK.INPUT);   
		}
		job2.addArgument("sgt_xfile=" + rupsgtx.getName());
		job2.addArgument("sgt_yfile=" + rupsgty.getName());
     	job2.addArgument("seis_file=" + seisFile.getName());

     	//Must set flags BEFORE 'uses' call, because uses makes a clone
     	rupsgtx.setRegister(false);
     	rupsgtx.setTransfer(File.TRANSFER.FALSE);
     	rupsgty.setRegister(false);
     	rupsgty.setTransfer(File.TRANSFER.FALSE);
		seisFile.setRegister(false);
		if (params.isZip()) {
			seisFile.setTransfer(File.TRANSFER.FALSE);
		} else {
			//Transfer the seismograms
			seisFile.setTransfer(File.TRANSFER.TRUE);
		}
     	  
     	job2.uses(rupsgtx,File.LINK.INPUT);
		job2.uses(rupsgty,File.LINK.INPUT);
		job2.uses(seisFile, File.LINK.OUTPUT);

		job2.addProfile("globus", "maxWallTime", "2");
     	job2.addProfile("pegasus", "group", "" + count);
        job2.addProfile("pegasus", "label", "" + currDax);
     
        int memNeeded = getSeisMem(numRupPoints);
        
        job2.addProfile("pegasus", "pmc_request_memory", "" + memNeeded);
        
		return job2;
	}

	
	private Job createPSAJob(int sourceIndex, int rupIndex, int rupvarcount, String rupVarLFN, int count, int currDax) {
		File seisFile = new File(SEISMOGRAM_FILENAME_PREFIX +
   			riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
   			"_"+rupvarcount+  SEISMOGRAM_FILENAME_EXTENSION);  
		
    	File peakValsFile = new File(PEAKVALS_FILENAME_PREFIX +
    			riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
    			"_"+rupvarcount+ PEAKVALS_FILENAME_EXTENSION);

    	if (params.isDirHierarchy()) {
			String dir = sourceIndex + "/" + rupIndex;
    		seisFile = new File(dir+ "/" + SEISMOGRAM_FILENAME_PREFIX +
    	   			riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
    	   			"_"+rupvarcount+  SEISMOGRAM_FILENAME_EXTENSION);  
    			
	    	peakValsFile = new File(dir + "/" + PEAKVALS_FILENAME_PREFIX +
    	    			riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
    	    			"_"+rupvarcount+ PEAKVALS_FILENAME_EXTENSION);
    	}
    	
    	// make a new job for extracting peak values(may need differentiation or integration
    	// to convert between IMT values for 1 spectral period
    	String id3 = "ID3_" + count + "_" + rupvarcount;
    	Job job3 = new Job(id3, NAMESPACE, PEAK_VAL_CALC_NAME,VERSION);
    
    	String highFilter = FILTER_HIGHHZ;
    	String numtimesteps = NUMTIMESTEPS;
    	String timestep = LF_TIMESTEP;
    	
    	if (params.isHighFrequency()) {
    		//need to use HF values, since LF was resampled to HF
    		highFilter = ""+ params.getMaxHighFrequency();
    		numtimesteps = ""+ (int)Math.round(Double.parseDouble(SEIS_LENGTH)/Double.parseDouble(HF_DT));
    		timestep = HF_DT;
    	}
    	
    	job3.addArgument("simulation_out_pointsX=2"); //2 b/c 2 components
    	job3.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job3.addArgument("simulation_out_timesamples="+numtimesteps);// numTimeSteps
    	job3.addArgument("simulation_out_timeskip="+ timestep); //dt
    	job3.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job3.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job3.addArgument("surfseis_rspectra_output_type=aa");
    	job3.addArgument("surfseis_rspectra_period=" + SPECTRA_PERIOD1);
    	job3.addArgument("surfseis_rspectra_apply_filter_highHZ="+highFilter);
    	job3.addArgument("surfseis_rspectra_apply_byteswap=no");
    	job3.addArgument("in=" + seisFile.getName());
    	job3.addArgument("out=" + peakValsFile.getName());

        seisFile.setRegister(false);
        seisFile.setTransfer(File.TRANSFER.FALSE);
        job3.uses(seisFile, File.LINK.INPUT);
    	
        peakValsFile.setRegister(false);
        if (params.isZip()) {
        	peakValsFile.setTransfer(File.TRANSFER.FALSE);
        } else {
        	peakValsFile.setTransfer(File.TRANSFER.TRUE);
        }
    	job3.uses(peakValsFile, File.LINK.OUTPUT);
    	
    	job3.addProfile("globus", "maxWallTime", "1");
    	job3.addProfile("pegasus", "group", "" + count);
        job3.addProfile("pegasus", "label", "" + currDax);
        
        int psaMem = getPSAMem();
        
        job3.addProfile("pegasus", "pmc_request_memory", "" + psaMem);
        
        return job3;
	}
	
	private Job createMultiSeisPSAJob(int sourceIndex, int rupIndex, HashMap<Integer, String> ruptureVariationMap, int numRupPoints, int count, int currDax, int clusterIndex) {
        int memNeeded = getSeisMem(numRupPoints) + getPSAMem();
	
		String id2 = "ID2_" + sourceIndex+"_"+rupIndex+"_"+clusterIndex;
		
		String seisPSAName = SEIS_PSA_MULTI_NAME;

		Job job2= new Job(id2, NAMESPACE, seisPSAName, VERSION);

		//Assemble rupture variation string
		//rup_var_string is in form (<rv_id>,<slip_id>,<hypo_id>);(....)
		StringBuffer rup_var_string = new StringBuffer("");
		StringBuffer profileArg = new StringBuffer("");
		File seisFile, psaFile, combinedSeisFile, combinedPsaFile;
		seisFile = psaFile = combinedSeisFile = combinedPsaFile = null;
		if (!params.isFileForward() && !params.isPipeForward()) {
			if (params.isDirHierarchy()) {
				String dir = sourceIndex + "/" + rupIndex;
				seisFile = new File(dir + "/" + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" +
						riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + SEISMOGRAM_FILENAME_EXTENSION);
				psaFile = new File(dir + "/" + PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" +
						riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + PEAKVALS_FILENAME_EXTENSION);
			} else {
				seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" +
						riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + SEISMOGRAM_FILENAME_EXTENSION);
				psaFile = new File(PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" +
						riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + PEAKVALS_FILENAME_EXTENSION);
			}
		} else if (params.isFileForward()) {
			seisFile = new File(TMP_FS + "/" + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + SEISMOGRAM_FILENAME_EXTENSION);
			psaFile = new File(TMP_FS + "/" + PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + PEAKVALS_FILENAME_EXTENSION);
			combinedSeisFile = new File(COMBINED_SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + COMBINED_SEISMOGRAM_FILENAME_EXTENSION);
			combinedPsaFile = new File(COMBINED_PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + COMBINED_PEAKVALS_FILENAME_EXTENSION);
			profileArg.append(" -F " + seisFile.getName() + "=" + combinedSeisFile.getName() + " -F " + psaFile.getName() + "=" + combinedPsaFile.getName());
		} else if (params.isPipeForward()) {
			seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + SEISMOGRAM_FILENAME_EXTENSION);
			psaFile = new File(PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + PEAKVALS_FILENAME_EXTENSION);
			combinedSeisFile = new File(COMBINED_SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + COMBINED_SEISMOGRAM_FILENAME_EXTENSION);
			combinedPsaFile = new File(COMBINED_PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" +
					riq.getRunID() + "_" + sourceIndex + "_" + rupIndex + COMBINED_PEAKVALS_FILENAME_EXTENSION);
			//profile arg is set later
		}

		for (int rv_id: ruptureVariationMap.keySet()) {
			String lfn = ruptureVariationMap.get(rv_id);
			String[] pieces = lfn.split("-");
			int slip_id = Integer.parseInt(pieces[1].split("s")[1]);
			int hypo_id = Integer.parseInt(pieces[2].split("h")[1]);
			if (rup_var_string.length()>0) {
				rup_var_string.append(";");
			}
			rup_var_string.append("(" + rv_id + "," + slip_id + "," + hypo_id + ")");
			//Always have just 1 output file; either with file/pipe fwding, or appended to in normal mode			}
		}
		
		if (params.isFileForward()) {
			//Combine all -F arguments into a single profile
	        job2.addProfile("pegasus", "pmc_task_arguments", profileArg.toString());
		} else if (params.isPipeForward()) {
			job2.addProfile("pegasus", "pmc_task_arguments", "-f " + SEISMOGRAM_ENV_VAR + "=" + combinedSeisFile.getName() + " -f " + PEAKVALS_ENV_VAR + "=" + combinedPsaFile.getName());
    		job2.addArgument("pipe_fwd=1");
		}

		seisFile.setRegister(true);
		seisFile.setTransfer(TRANSFER.TRUE);
		job2.uses(seisFile, File.LINK.OUTPUT);

		psaFile.setRegister(true);
		psaFile.setTransfer(TRANSFER.TRUE);
		job2.uses(psaFile, File.LINK.OUTPUT);
		
		//add source, rupture, rupture variation arguments
		job2.addArgument("source_id=" + sourceIndex);
		job2.addArgument("rupture_id=" + rupIndex);
		job2.addArgument("num_rup_vars=" + ruptureVariationMap.size());
		job2.addArgument("det_max_freq=" + params.getDetFrequency());
		
		if (params.isHighFrequency()) {
			job2.addArgument("stoch_max_freq=" + params.getMaxHighFrequency());
		} else {
			job2.addArgument("stoch_max_freq=-1.0"); //signify no stochastic components
		}
			
		job2.addArgument("rup_vars=" + rup_var_string.toString());
			
		//synth args
		job2.addArgument("stat="+riq.getSiteName());
		job2.addArgument("slon="+riq.getLon());
		job2.addArgument("slat="+riq.getLat());
		job2.addArgument("extract_sgt=0");
		job2.addArgument("outputBinary=1");
		job2.addArgument("mergeOutput=1");
		job2.addArgument("ntout="+NUMTIMESTEPS);
        
		File rupsgtx = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
		File rupsgty = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		
		if (params.isDirHierarchy()) {
			//Add hierarchy to sub sgt path
			String dir = sourceIndex + "/" + rupIndex;
			rupsgtx = new File(dir + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
			rupsgty = new File(dir + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		} else if (params.isGlobalExtractSGTMPI()) {
			//Add pre directory and source dir to sub-sgt path
			String dir = "../CyberShake_%s_pre_preDAX";
			rupsgtx = new File(dir + "/" + sourceIndex + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
			rupsgty = new File(dir + "/" + sourceIndex + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		}
		
		rupsgtx.setRegister(false);
		rupsgtx.setTransfer(File.TRANSFER.FALSE);
		rupsgty.setRegister(false);
		rupsgty.setTransfer(File.TRANSFER.FALSE);
		
		File rup_geom_file = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceIndex + "_" + rupIndex + ".txt");
		job2.addArgument("rup_geom_file=" + rup_geom_file.getName());
		job2.uses(rup_geom_file, File.LINK.INPUT);
		
		job2.addArgument("sgt_xfile=" + rupsgtx.getName());
		job2.addArgument("sgt_yfile=" + rupsgty.getName());

		if (params.isPipeForward()) {
	     	job2.addArgument("seis_file=" + FD_PATH + "/$" + SEISMOGRAM_ENV_VAR);	
		} else {
			job2.addArgument("seis_file=" + seisFile.getName());
		}
		
		if (params.isLargeMemSynth()) {
			job2.addArgument(" max_buf_mb=" + LARGE_MEM_BUF);
		}
		
     	//PSA args
     	job2.addArgument("run_psa=1"); //include PSA part
       	job2.addArgument("simulation_out_pointsX=2"); //2 b/c 2 components
    	job2.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job2.addArgument("simulation_out_timesamples="+NUMTIMESTEPS);// numTimeSteps
    	job2.addArgument("simulation_out_timeskip="+ LF_TIMESTEP); //dt
    	job2.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job2.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job2.addArgument("surfseis_rspectra_output_type=aa");
    	job2.addArgument("surfseis_rspectra_period=" + SPECTRA_PERIOD1);
    	job2.addArgument("surfseis_rspectra_apply_filter_highHZ="+FILTER_HIGHHZ);
    	job2.addArgument("surfseis_rspectra_apply_byteswap=no");
    	
    	if (params.isPipeForward()) {
        	job2.addArgument("out=" + FD_PATH + "/$" + PEAKVALS_ENV_VAR);
    	} else {
    		job2.addArgument("out=" + psaFile.getName());
    	}
    	
     	job2.uses(rupsgtx,File.LINK.INPUT);
		job2.uses(rupsgty,File.LINK.INPUT);

		job2.addProfile("globus", "maxWallTime", "2");
     	job2.addProfile("pegasus", "group", "" + count);
     	
     	if (memNeeded>params.getSeisPSAMemCutoff()) {
            System.err.println("Source " + sourceIndex + ", rupture " + rupIndex + " requires " + memNeeded + " memory, which is more than the permitted " + params.getSeisPSAMemCutoff() + ".  Aborting.");
            System.exit(4);
     	} else {
     		job2.addProfile("pegasus", "label", "" + currDax);
     	}
        
        job2.addProfile("pegasus", "pmc_request_memory", "" + memNeeded);

		return job2;
	}
		
	
	private Job createSeisPSAJob(int sourceIndex, int rupIndex, int rupvarcount, int numRupPoints, String rupVarLFN, int count, int currDax) {
		//<profile namespace="pegasus" key="request_memory">100</profile>
        int memNeeded = getSeisMem(numRupPoints) + getPSAMem();
		
		String id2 = "ID2_" + sourceIndex+"_"+rupIndex+"_"+rupvarcount;
		
		String seisPSAName = SEIS_PSA_MEMCACHED_NAME;
		if (params.isFileForward() || params.isPipeForward()) {
			seisPSAName = SEIS_PSA_HEADER_NAME;
		}
		if (memNeeded>params.getSeisPSAMemCutoff() && !params.isLargeMemSynth()) {
			//Switch to high-mem nodes
			seisPSAName = SEIS_PSA_HIGH_MEM_NODES_NAME;
		}
		if (params.isLargeMemSynth()) {
			seisPSAName = SEIS_PSA_LARGE_MEM_NAME;
		}
		
		Job job2 = null;
		if (riq.getRuptVarScenID()==5) {
			job2 = new Job(id2, NAMESPACE, seisPSAName, "3.3.1");
		} else {
			job2 = new Job(id2, NAMESPACE, seisPSAName, VERSION);
		}
        
		File seisFile = null;
		File peakValsFile = null;
		File combinedSeisFile = null;
		File combinedPeakValsFile = null;
		
		if (params.isDirHierarchy()) {
			String dir = sourceIndex + "/" + rupIndex;
			seisFile = new File(dir + "/" +
					SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" + 
					sourceIndex + "_" + rupIndex + "_" + rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);
			
			peakValsFile = new File(dir + "/" +
					PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + "_" + rupvarcount + PEAKVALS_FILENAME_EXTENSION);
		} else {
			seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + 
					riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
					"_"+ rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);                            
			
			peakValsFile = new File(PEAKVALS_FILENAME_PREFIX +
	    			riq.getSiteName() + "_" + riq.getRunID() + "_" + sourceIndex + "_" + rupIndex +
	    			"_"+rupvarcount+ PEAKVALS_FILENAME_EXTENSION);
		}
		
		if (params.isFileForward()) {
			//Can overwrite dir hierarchy, since /tmp filesystem is node-local
			seisFile = new File(TMP_FS + "/" + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + "_" + rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);
			combinedSeisFile = new File(COMBINED_SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + COMBINED_SEISMOGRAM_FILENAME_EXTENSION);
			peakValsFile = new File(TMP_FS + "/" + PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + "_" + rupvarcount + PEAKVALS_FILENAME_EXTENSION);
			combinedPeakValsFile = new File(COMBINED_PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + COMBINED_PEAKVALS_FILENAME_EXTENSION);
		
			//Combine all -F arguments into a single profile
	        job2.addProfile("pegasus", "pmc_task_arguments", "-F " + seisFile.getName() + "=" + combinedSeisFile.getName() + " -F " + peakValsFile.getName() + "=" + combinedPeakValsFile.getName());

			combinedSeisFile.setRegister(true);
			combinedSeisFile.setTransfer(TRANSFER.TRUE);
			combinedPeakValsFile.setRegister(true);
			combinedPeakValsFile.setTransfer(TRANSFER.TRUE);
			
			job2.uses(combinedSeisFile, File.LINK.OUTPUT);			
			job2.uses(combinedPeakValsFile, File.LINK.OUTPUT);
			
			//add source, rupture, rupture variation arguments
			job2.addArgument("source_id=" + sourceIndex);
			job2.addArgument("rupture_id=" + rupIndex);
			job2.addArgument("rup_var_id=" + rupvarcount);
			job2.addArgument("det_max_freq=" + params.getDetFrequency());
			if (params.isHighFrequency()) {
				job2.addArgument("stoch_max_freq=" + params.getMaxHighFrequency());
			} else {
				job2.addArgument("stoch_max_freq=-1.0"); //signify no stochastic components
			}
	    } else if (params.isPipeForward()) {
			//Can overwrite dir hierarchy - no intermediate files	    	
			if (params.isSourceForward()) {
				combinedSeisFile = new File(COMBINED_SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
						sourceIndex + "_" + currDax + COMBINED_SEISMOGRAM_FILENAME_EXTENSION);
				combinedPeakValsFile = new File(COMBINED_PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
						sourceIndex + "_" + currDax+ COMBINED_PEAKVALS_FILENAME_EXTENSION);
			} else {
				combinedSeisFile = new File(COMBINED_SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + COMBINED_SEISMOGRAM_FILENAME_EXTENSION);
				combinedPeakValsFile = new File(COMBINED_PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + "_" +
					sourceIndex + "_" + rupIndex + COMBINED_PEAKVALS_FILENAME_EXTENSION);
			}
			
			//Combine all -f arguments into a single profile
	        job2.addProfile("pegasus", "pmc_task_arguments", "-f " + SEISMOGRAM_ENV_VAR + "=" + combinedSeisFile.getName() + " -f " + PEAKVALS_ENV_VAR + "=" + combinedPeakValsFile.getName());
			
			if (rupvarcount==0) {
				combinedSeisFile.setRegister(true);
				combinedSeisFile.setTransfer(TRANSFER.TRUE);
				combinedPeakValsFile.setRegister(true);
				combinedPeakValsFile.setTransfer(TRANSFER.TRUE);
				
				job2.uses(combinedSeisFile, File.LINK.OUTPUT);			
				job2.uses(combinedPeakValsFile, File.LINK.OUTPUT);
			}
			
			//add source, rupture, rupture variation arguments
			job2.addArgument("source_id=" + sourceIndex);
			job2.addArgument("rupture_id=" + rupIndex);
			job2.addArgument("rup_var_id=" + rupvarcount);
			job2.addArgument("det_max_freq=" + params.getDetFrequency());
			if (params.isHighFrequency()) {
				job2.addArgument("stoch_max_freq=" + params.getMaxHighFrequency());
			} else {
				job2.addArgument("stoch_max_freq=-1.0"); //signify no stochastic components
			}
			
    		job2.addArgument("pipe_fwd=1");
		}
		
		File rupVarFile = new File(rupVarLFN);
		
		//synth args
		job2.addArgument("stat="+riq.getSiteName());
		job2.addArgument("slon="+riq.getLon());
		job2.addArgument("slat="+riq.getLat());
		job2.addArgument("extract_sgt=0");
		job2.addArgument("outputBinary=1");
		job2.addArgument("mergeOutput=1");
		job2.addArgument("ntout="+NUMTIMESTEPS);
        
		File rupsgtx = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
		File rupsgty = new File(riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		
		if (params.isDirHierarchy()) {
			//Add hierarchy to sub sgt path
			String dir = sourceIndex + "/" + rupIndex;
			rupsgtx = new File(dir + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
			rupsgty = new File(dir + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		} else if (params.isGlobalExtractSGTMPI()) {
			//Add pre directory and source dir to sub-sgt path
			String dir = "../CyberShake_%s_pre_preDAX";
			rupsgtx = new File(dir + "/" + sourceIndex + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfx.sgt");
			rupsgty = new File(dir + "/" + sourceIndex + "/" + riq.getSiteName() + "_"+sourceIndex+"_"+rupIndex +"_subfy.sgt");
		}
		
		rupsgtx.setRegister(false);
		rupsgtx.setTransfer(File.TRANSFER.FALSE);
		rupsgty.setRegister(false);
		rupsgty.setTransfer(File.TRANSFER.FALSE);
		
		if (params.isJbsimRVMem()) {
			//Don't use rupture file;  instead, use source/rupture/slip/hypo arguments
			int slip = -2;
			int hypo = -2;
			if (riq.getRuptVarScenID()==5) {
				//43_0.txt.variation-r000000
				String[] pieces = rupVarLFN.split("-");
				slip = Integer.parseInt(pieces[1].substring(1));
				hypo = 0;
			} else {
				//43_0.txt.variation-s0000-h0000
				String[] pieces = rupVarLFN.split("-");
				slip = Integer.parseInt(pieces[1].substring(1));
				hypo = Integer.parseInt(pieces[2].substring(1));
			}
			job2.addArgument("slip=" + slip);
			job2.addArgument("hypo=" + hypo);
			File rup_geom_file = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceIndex + "_" + rupIndex + ".txt");
			job2.addArgument("rup_geom_file=" + rup_geom_file.getName());
			job2.uses(rup_geom_file, File.LINK.INPUT);
		} else {
			job2.addArgument("rupmodfile=" + rupVarFile.getName());
	     	job2.uses(rupVarFile,File.LINK.INPUT);   
		}
		
		job2.addArgument("sgt_xfile=" + rupsgtx.getName());
		job2.addArgument("sgt_yfile=" + rupsgty.getName());
		if (params.isPipeForward()) {
	     	job2.addArgument("seis_file=" + FD_PATH + "/$" + SEISMOGRAM_ENV_VAR);	
		} else {
			job2.addArgument("seis_file=" + seisFile.getName());
		}

		if (params.isLargeMemSynth()) {
			job2.addArgument(" max_buf_mb=" + LARGE_MEM_BUF);
		}
		
     	//PSA args
     	job2.addArgument("run_psa=1"); //include PSA part
       	job2.addArgument("simulation_out_pointsX=2"); //2 b/c 2 components
    	job2.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job2.addArgument("simulation_out_timesamples="+NUMTIMESTEPS);// numTimeSteps
    	job2.addArgument("simulation_out_timeskip="+ LF_TIMESTEP); //dt
    	job2.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job2.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job2.addArgument("surfseis_rspectra_output_type=aa");
    	job2.addArgument("surfseis_rspectra_period=" + SPECTRA_PERIOD1);
    	job2.addArgument("surfseis_rspectra_apply_filter_highHZ="+FILTER_HIGHHZ);
    	job2.addArgument("surfseis_rspectra_apply_byteswap=no");
    	if (params.isPipeForward()) {
        	job2.addArgument("out=" + FD_PATH + "/$" + PEAKVALS_ENV_VAR);
    	} else {
    		job2.addArgument("out=" + peakValsFile.getName());
    	}
     	
     	//Must set flags BEFORE 'uses' call, because uses makes a clone
		seisFile.setRegister(false);
		seisFile.setTransfer(File.TRANSFER.FALSE);
    	
        peakValsFile.setRegister(false);
        peakValsFile.setTransfer(File.TRANSFER.FALSE);
        
        if (!params.isZip() && (!params.isFileForward() || !params.isPipeForward())) {
    		seisFile.setTransfer(File.TRANSFER.TRUE);
    		peakValsFile.setTransfer(File.TRANSFER.TRUE);
        }
     	
     	job2.uses(rupsgtx,File.LINK.INPUT);
		job2.uses(rupsgty,File.LINK.INPUT);
		if (!params.isPipeForward() && !params.isFileForward()) {
			//Combined files were already taken care of earlier if there's forwarding
			job2.uses(seisFile, File.LINK.OUTPUT);
			job2.uses(peakValsFile, File.LINK.OUTPUT);
		}

		job2.addProfile("globus", "maxWallTime", "2");
     	job2.addProfile("pegasus", "group", "" + count);
     	
     	if (memNeeded>params.getSeisPSAMemCutoff()) {
            System.err.println("Source " + sourceIndex + ", rupture " + rupIndex + " requires " + memNeeded + " memory, which is more than the permitted " + params.getSeisPSAMemCutoff() + ".  Aborting.");
            System.exit(4);
     	} else {
     		job2.addProfile("pegasus", "label", "" + currDax);
     	}
        
        job2.addProfile("pegasus", "pmc_request_memory", "" + memNeeded);

		return job2;
	}
	
	private int getExtractMem(int numRupPoints) {
		int size_sgtmaster = 32;
		int size_sgtindex = 24;
		int size_sgtheader = 128;
		int size_sgtparams = 20;
		int sgt_timesteps = 2000;
		int numComponents = 3;
		double tolerance = 1.1;
		double rvMem = 3.5*1024*1024*(params.getHighFrequencyCutoff()/0.5)*(params.getHighFrequencyCutoff()/0.5);
		//Save about 500,000 pts per SGT set for 0.5 Hz
		int numSGTpts = (int)(500000*(params.getHighFrequencyCutoff()/0.5)*(params.getHighFrequencyCutoff()/0.5));
		double sgtpars = numComponents * (size_sgtmaster + numSGTpts*size_sgtindex);
		double sgtparams = size_sgtparams * numSGTpts * numComponents;
		double sgt_subset = numComponents * (size_sgtmaster + numRupPoints*(size_sgtindex + size_sgtheader + 6*4*sgt_timesteps*(params.getHighFrequencyCutoff()/0.5)));
		return (int)(Math.ceil(tolerance*(rvMem + sgtpars + sgtparams + sgt_subset)/(1024*1024)));
	}
	
	private int getSeisMem(int numRupPoints) {
//		System.out.println("num points: " + numRupPoints);
		//Estimate of memory in MB required from # of points
		int size_sgtmaster = 32;
		int size_sgtindex = 24;
		int size_sgtheader = 128;
		int numComponents = 3;
		int numSGTtimesteps = 2000;
		double tolerance = 1.1;
		//Total size is size for SGTs + size of rupture variation + seismogram
		//Do calculation in MB
		//1 MB is max RV size for 0.5 Hz
		//Scale based on erf spacing
		double rvMem = (1.0/(riq.getErfSpacing()*riq.getErfSpacing()))*(params.getDetFrequency()/0.5)*(params.getDetFrequency()/0.5);
		//double sgtMem = numComponents/(1024.0*1024.0) * (size_sgtmaster + numRupPoints*(size_sgtindex + size_sgtheader + 6*numSGTtimesteps*(params.getHighFrequencyCutoff()/0.5)*4));
		//numComp + 1 b/c we have a read buffer now
		double sgtMem = 0.0;
		if (params.isLargeMemSynth()) {
			sgtMem = LARGE_MEM_BUF + numRupPoints*172.0/(1024.0*1024.0); 
		} else {
			sgtMem = (numComponents+1)/(1024.0*1024.0) * (size_sgtmaster + numRupPoints*(size_sgtindex + size_sgtheader + 6*numSGTtimesteps*(params.getDetFrequency()/0.5)*4));
		}
		double seisOut = Integer.parseInt(NUMTIMESTEPS)*numComponents*4/(1024.0*1024);

		if (params.isHighFrequency()) {
			seisOut *= 0.1/Double.parseDouble(HF_DT);
		}
//		System.out.println("Memory required: " + rvMem + " + " + sgtMem + " + " + seisOut);
		return (int)((sgtMem+rvMem+seisOut)*tolerance)+1;
	}
	
	private int getPSAMem() {
		int numComponents = 3;
		//Need to read in all seismograms
		if (params.isHighFrequency()) {
			//Extra factor of 4 for smaller dt
			return (int)(Math.ceil(Integer.parseInt(NUMTIMESTEPS)*numComponents*4*4/(1024*1024)));
		} else {
			return (int)(Math.ceil(Integer.parseInt(NUMTIMESTEPS)*numComponents*4/(1024*1024)));
		}
	}


	private Job createLocalVMJob(String vmName, String localVMName) {
		String id = "ID0_create_local_VM";
		Job job = new Job(id, NAMESPACE, LOCAL_VM_NAME, VERSION);
		
		File vmIn = new File(vmName);
		File vmOut = new File(localVMName);
		
		job.addArgument(vmIn);
		job.addArgument(vmOut);
		
		job.uses(vmIn, LINK.INPUT);
		job.uses(vmOut, LINK.OUTPUT);
        
        return job;
	}


	private String getVM() {
		//Determine which 1D velocity model is appropriate
		//for now, use hardcoded result
		return "Northridge_1D_VM";
	}
	

	private Job createStochJob(int sourceIndex, int rupIndex, int rupvarcount, String rupVarLFN, int count, int currDax) {
		String id = "ID_Stoch_" + sourceIndex+"_"+rupIndex+"_"+rupvarcount;
		
		Job job = new Job(id, NAMESPACE, STOCH_NAME, VERSION);
		
		File srfFile = new File(rupVarLFN);
		File outfile = new File(rupVarLFN + ".slip");
		
		double DX = 2.0;
		double DY = 2.5;
		
		job.addArgument("infile=" + rupVarLFN);
		job.addArgument("outfile=" + outfile.getName());
		job.addArgument("dx=" + DX);
		job.addArgument("dy=" + DY);
		
		outfile.setRegister(false);
		outfile.setTransfer(File.TRANSFER.FALSE);
		
		job.uses(srfFile, File.LINK.INPUT);
		job.uses(outfile, File.LINK.OUTPUT);
		
		job.addProfile("globus", "maxWallTime", "1");
     	job.addProfile("pegasus", "group", "" + count);
        job.addProfile("pegasus", "label", "" + currDax);
		
		return job;
	}

	
	private Job createHighFrequencyJob(int sourceIndex, int rupIndex, int rupvarcount, int numRupPoints, String rupVarLFN, int count, int currDax) {
		String id = "ID_HF_" + sourceIndex+"_"+rupIndex+"_"+rupvarcount;
		
		Job job = new Job(id, NAMESPACE, HIGH_FREQ_NAME, VERSION);
		
		//Leave off the filename extension so it doesn't get included in the zip job
		File seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + 
				riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
				"_"+ rupvarcount + "_hf");
		
		
		File slipFile = new File(rupVarLFN + ".slip");
		
		File localVMFile = new File(localVMFilename);
		
		job.addArgument("stat=" + riq.getSiteName());
		job.addArgument("slon=" + riq.getLon());
		job.addArgument("slat=" + riq.getLat());
		job.addArgument("slipfile=" + slipFile.getName());
		job.addArgument("vmod=" + localVMFile.getName());
		job.addArgument("outfile=" + seisFile.getName());
		job.addArgument("vs30=" + riq.getVs30());
		job.addArgument("tlen=" + SEIS_LENGTH);
		job.addArgument("dt=" + HF_DT);
		
		
		seisFile.setRegister(false);
		seisFile.setTransfer(File.TRANSFER.FALSE);
	
		job.uses(slipFile, LINK.INPUT);
		job.uses(localVMFile, LINK.INPUT);
		job.uses(seisFile, LINK.OUTPUT);
		
		job.addProfile("globus", "maxWallTime", "2");
     	job.addProfile("pegasus", "group", "" + count);
        job.addProfile("pegasus", "label", "" + currDax);
        
        int memNeeded = getSeisMem(numRupPoints);
        
        job.addProfile("pegasus", "pmc_request_memory", "" + memNeeded);
		
		return job;
	}
	

	private Job createMergeSeisJob(int sourceIndex, int rupIndex, int rupvarcount, String rupVarLFN, int count, int currDax) {
		String id = "ID_Merge" + sourceIndex+"_"+rupIndex+"_"+rupvarcount;
		
		Job job = new Job(id, NAMESPACE, MERGE_NAME, VERSION);
		
		File lfSeisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + "_lf" + SEISMOGRAM_FILENAME_EXTENSION);
		File hfSeisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + "_hf");
		File mergedFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);
		
		int hf_nt = (int)Math.round(Double.parseDouble(SEIS_LENGTH)/Double.parseDouble(HF_DT));
		
		job.addArgument("freq=" + params.getHighFrequencyCutoff());
		job.addArgument("lf_seis=" + lfSeisFile.getName());
		job.addArgument("hf_seis=" + hfSeisFile.getName());
		job.addArgument("outfile=" + mergedFile.getName());
		job.addArgument("hf_dt=" + HF_DT);
		job.addArgument("hf_nt=" + hf_nt);
		job.addArgument("lf_dt=" + LF_TIMESTEP);
		job.addArgument("lf_nt=" + NUMTIMESTEPS);
		
		mergedFile.setRegister(false);
		mergedFile.setTransfer(File.TRANSFER.FALSE);
		
		job.uses(lfSeisFile, File.LINK.INPUT);
		job.uses(hfSeisFile, File.LINK.INPUT);
		job.uses(mergedFile, File.LINK.OUTPUT);
		
		job.addProfile("globus", "maxWallTime", "1");
     	job.addProfile("pegasus", "group", "" + count);
        job.addProfile("pegasus", "label", "" + currDax);
		
        
        int numComponents = 3;
        //x9 because x1 for LF input, x4 for HF input, x4 for HF output
        int memUsage = (int)(Math.ceil(1.1*Integer.parseInt(NUMTIMESTEPS)*numComponents*4*9/(1024*1024)));
        
        job.addProfile("pegasus", "pmc_request_memory", "" + memUsage);
        
		return job;
	}
	

	private Job createHFSynthJob(int sourceIndex, int rupIndex, int rupvarcount, String rupVarLFN, int count, int currDax) {
		String id = "ID_HF_Synth" + sourceIndex + "_" + rupIndex + "_" + rupvarcount;
		
		String jobName = HF_SYNTH_NAME;
		
        if (params.isHfsynthRVMem()) {
        	jobName = HF_SYNTH_NAME + "_rv_in_mem";
        }
		
		
		Job job = new Job(id, NAMESPACE, jobName, VERSION);

		double DX = 2.0;
		double DY = 2.5;
		
		File srfFile = new File(rupVarLFN);
		File localVMFile = new File(localVMFilename);
		File seisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + "_hf");
		
		seisFile.setRegister(false);
		seisFile.setTransfer(File.TRANSFER.FALSE);
		
		job.uses(localVMFile, LINK.INPUT);
		job.uses(seisFile, File.LINK.OUTPUT);
		
		if (params.isHfsynthRVMem()) {
			String[] pieces = rupVarLFN.split("-");
			int slip = Integer.parseInt(pieces[1].substring(1));
			int hypo = Integer.parseInt(pieces[2].substring(1));
			
			File rup_geom_file = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceIndex + "_" + rupIndex + ".txt");
 			job.addArgument("rup_geom_file=" + rup_geom_file.getName());
			job.addArgument("slip=" + slip);
			job.addArgument("hypo=" + hypo);
			
 			job.uses(rup_geom_file, File.LINK.INPUT);
		} else {
			job.addArgument("infile=" + srfFile.getName());
			job.uses(srfFile, File.LINK.INPUT);
		}

		job.addArgument("outfile=" + seisFile.getName());
		job.addArgument("dx=" + DX);
		job.addArgument("dy=" + DY);
		job.addArgument("stat=" + riq.getSiteName());
		job.addArgument("slon=" + riq.getLon());
		job.addArgument("slat=" + riq.getLat());
		job.addArgument("vmod=" + localVMFile.getName());
		job.addArgument("vs30=" + riq.getVs30());
		job.addArgument("tlen=" + SEIS_LENGTH);
		job.addArgument("dt=" + HF_DT);
		
		job.addProfile("globus", "maxWallTime", "1");
		job.addProfile("pegasus", "group", "" + count);
		job.addProfile("pegasus", "label", "" + currDax);
		
        int numComponents = 3;
        //x4 for HF output
        int memUsage = (int)(Math.ceil(1.1*Integer.parseInt(NUMTIMESTEPS)*numComponents*4*4/(1024*1024)));
        
        job.addProfile("pegasus", "pmc_request_memory", "" + memUsage);
		
		return job;
	}
	

	private Job createMergePSAJob(int sourceIndex, int rupIndex, int rupvarcount, int numRupPoints, String rupVarLFN, int count, int currDax) {
		String id = "ID_MergePSA_" + sourceIndex + "_" + rupIndex + "_" + rupvarcount;
		
		Job job = new Job(id, NAMESPACE, MERGE_PSA_NAME, VERSION);
		
		File lfSeisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + "_lf" + SEISMOGRAM_FILENAME_EXTENSION);
		File hfSeisFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + "_hf");
		File mergedFile = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + 
				rupIndex + "_"+ rupvarcount + SEISMOGRAM_FILENAME_EXTENSION);
		File psaFile = new File(PEAKVALS_FILENAME_PREFIX + riq.getSiteName() + "_" + sourceIndex + "_" + rupIndex +
    			"_"+rupvarcount+ PEAKVALS_FILENAME_EXTENSION);

		mergedFile.setTransfer(File.TRANSFER.FALSE);
		mergedFile.setRegister(false);
		
		psaFile.setTransfer(File.TRANSFER.FALSE);
		psaFile.setRegister(false);
		
		job.uses(lfSeisFile, File.LINK.INPUT);
		job.uses(hfSeisFile, File.LINK.INPUT);
		job.uses(mergedFile, File.LINK.OUTPUT);
		job.uses(psaFile, File.LINK.OUTPUT);
		

		int hf_nt = (int)Math.round(Double.parseDouble(SEIS_LENGTH)/Double.parseDouble(HF_DT));
		
		job.addArgument("freq=" + params.getHighFrequencyCutoff());
		job.addArgument("lf_seis=" + lfSeisFile.getName());
		job.addArgument("hf_seis=" + hfSeisFile.getName());
		job.addArgument("outfile=" + mergedFile.getName());
		job.addArgument("hf_dt=" + HF_DT);
		job.addArgument("hf_nt=" + hf_nt);
		job.addArgument("lf_dt=" + LF_TIMESTEP);
		job.addArgument("lf_nt=" + NUMTIMESTEPS);
       	job.addArgument("simulation_out_pointsX=2"); //2b/c 2 components
    	job.addArgument("simulation_out_pointsY=1"); //# of variations per seismogram
    	job.addArgument("simulation_out_timesamples="+hf_nt);// numTimeSteps
    	job.addArgument("simulation_out_timeskip="+ HF_DT); //dt
    	job.addArgument("surfseis_rspectra_seismogram_units=cmpersec");
    	job.addArgument("surfseis_rspectra_output_units=cmpersec2");
    	job.addArgument("surfseis_rspectra_output_type=aa");
    	job.addArgument("surfseis_rspectra_period=" + SPECTRA_PERIOD1);
    	job.addArgument("surfseis_rspectra_apply_filter_highHZ="+params.getMaxHighFrequency());
    	job.addArgument("surfseis_rspectra_apply_byteswap=no");
    	job.addArgument("out=" + psaFile.getName());
		
		job.addProfile("globus", "maxWallTime", "1");
		job.addProfile("pegasus", "group", "" + count);
		job.addProfile("pegasus", "label", "" + currDax);
    	
		int numComponents = 3;
        //x5 for LF input, HF input  + PSA
        int memUsage = (int)(Math.ceil(1.1*Integer.parseInt(NUMTIMESTEPS)*numComponents*4*5/(1024*1024))) + getPSAMem();
		
        job.addProfile("pegasus", "pmc_request_memory", "" + memUsage);
        
		return job;
	}
}
