package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
	private final String VM_FILENAME = "nr02-vs500.fk1d";
	private final String MOJAVE_VM_FILENAME = "mj-vs500.fk1d";
	private final String SOUTHERN_SIERRA_VM_FILENAME = "ssn2-vs500.fk1d";
	private final String CENTRAL_CAL_VM_FILENAME = "pf01-vs500.fk1d";
	private final String NO_CAL_VM_FILENAME = "lp-vs500.fk1d";
	private final HashMap<String, String> hf_vel_model_map;
	private final static String SEISMOGRAM_FILENAME_PREFIX = "Seismogram_";
    private final static String SEISMOGRAM_FILENAME_EXTENSION = ".grm";
    private static double DT = 0.01;
	private static double PSA_FILTER = 100.0;
	private final static String PSA_FILENAME_PREFIX = "PeakVals_";
    private final static String PSA_FILENAME_EXTENSION = ".bsa";
	private final static String ROTD_FILENAME_PREFIX = "RotD_";
    private final static String ROTD_FILENAME_EXTENSION = ".rotd";
    private final static String VERT_RSP_FILENAME_PREFIX = "VerticalRSP_";
    private final static String VERT_RSP_FILENAME_EXTENSION = ".rsp";
    private final static String DURATION_FILENAME_PREFIX = "Duration_";
    private final static String DURATION_FILENAME_EXTENSION = ".dur";
    private final static String PERIOD_DURATION_FILENAME_PREFIX = "PeriodDuration_";
    private final static String PERIOD_DURATION_FILENAME_EXTENSION = ".dur";

	
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
    private static String DB_SERVER = "moment";
    private final static String DB = "CyberShake";
    private final static String USER = "cybershk_ro";
    private final static String PASS = "CyberShake2007";
    
    private final static int VS30 = 0;
    private final static int VS5H = 1;
    private final static int VSD5H = 2;
	
    private final static String CARC_GO_ENDPOINT = "56569ec1-af41-4745-a8d1-8514231c7a6d";
    
	private RunIDQuery riq;
	private Stochastic_DAXParameters sParams;
    
	public CyberShake_Sub_Stoch_DAXGen(int runID, Stochastic_DAXParameters hp) {
		riq = new RunIDQuery(runID, DB_SERVER);
		sParams = hp;
		if (sParams.getLowFreqRIQ().getLowFrequencyCutoff()>=1.0) {
			//@1 Hz we use seismograms which are 500s in length
			sParams.setTlen(400.0);
		}
		sParams.setStochFrequency(riq.getMax_frequency());
		sParams.setMergeFrequency(riq.getLowFrequencyCutoff());
		//Determine dt
		//DT = 0.5/riq.getMax_frequency();
		DT = 0.5/sParams.getStochFrequency();
		//Occasionally there is some kind of problem with the stochastic frequency
		//Add a check so that if it's not at least 10 Hz/DT is less than/equal to 0.05, we abort.
		if (DT>0.05) {
			System.err.println("When setting DT value, found that stochastic freq in DB is set to only " + sParams.getStochFrequency() + " Hz.  Should be at least 10 Hz, aborting.");
			System.exit(3);
		}
		//Create and populate argument string to LFN map for 1D HF velocity files
		hf_vel_model_map = new HashMap<String, String>();
		hf_vel_model_map.put("labasin", VM_FILENAME);
		hf_vel_model_map.put("mojave", MOJAVE_VM_FILENAME);
		hf_vel_model_map.put("southernsierra", SOUTHERN_SIERRA_VM_FILENAME);
		hf_vel_model_map.put("centralcal", CENTRAL_CAL_VM_FILENAME);
		hf_vel_model_map.put("nocal", NO_CAL_VM_FILENAME);
	}
    
	public static void main(String[] args) {
		String usageString = "CyberShake_Sub_Stochastic_DAXGen <runID> <directory>";
		Options cmd_opts = new Options();
	
		Option help = new Option("h", "help", false, "Print help for CyberShake_HF_DAXGen");
		Option runID = OptionBuilder.withArgName("run_id").hasArg().withDescription("Stochastic simulation run ID").create("r");
		Option lfRunID = OptionBuilder.withArgName("lf_run_id").hasArg().withDescription("Low-frequency simulation run ID").create("lr");
		Option noRotd = new Option("nr", "no-rotd", false, "Omit RotD calculations.");
		Option noSiteResponse = new Option("nsr", "no-site-response", false, "Omit site response calculation.");
		Option noLFSiteResponse = new Option("nls", "no-low-site-response", false, "Omit site response calculation for low-frequency seismograms.");
		Option noDuration = new Option("nd", "no-duration", false, "Omit duration calculations.");
		Option velocityFile = OptionBuilder.withArgName("vs30").hasArg().withDescription("Velocity file with Vs0, Vs30, VsD.").create("v");
		Option outputDAX = OptionBuilder.withArgName("output").hasArg().withDescription("output DAX filename").create("o");
		Option server = OptionBuilder.withArgName("server").withLongOpt("server").hasArg().withDescription("Server to use for site parameters and to insert PSA values into").create("sr");
		Option mergeFrequency = OptionBuilder.withArgName("merge_frequency").withLongOpt("merge_frequency").hasArg().withDescription("(Deprecated) merge frequency.  This is now taken from the DB.").create("mf");
        Option db_rvfrac_seed = new Option("dbrs", "db-rv-seed", false, "Use rvfrac value and seed from the database, if provided.");
        Option hf_velocity_model = OptionBuilder.withArgName("hf_vel_model").withLongOpt("hf_vel_model").hasArg().withDescription("1D velocity model to use for high-frequency synth. Options are 'labasin' (default) or 'mojave').").create("hfv");
        Option z_comp = new Option("z", "z_comp", false, "Calculate seismograms and IMs for the vertical Z component.");
        Option periodDepDuration = new Option("pd", "period-duration", false, "Include calculation of period-dependent durations.");        
        Option noVertRsp = new Option("nvr", "no-vertical-response", false, "Skip calculation of vertical response spectra, even if the Z component is present.");        
        Option debug = new Option("d", "debug", false, "Debug flag.");
		
		cmd_opts.addOption(help);
		cmd_opts.addOption(runID);
		cmd_opts.addOption(lfRunID);
		cmd_opts.addOption(noRotd);
		cmd_opts.addOption(noSiteResponse);
		cmd_opts.addOption(noLFSiteResponse);
		cmd_opts.addOption(noDuration);
		cmd_opts.addOption(velocityFile);
		cmd_opts.addOption(outputDAX);
		cmd_opts.addOption(server);
		cmd_opts.addOption(mergeFrequency);
		cmd_opts.addOption(db_rvfrac_seed);
		cmd_opts.addOption(hf_velocity_model);
		cmd_opts.addOption(debug);
        cmd_opts.addOption(periodDepDuration);
		cmd_opts.addOption(z_comp);
		cmd_opts.addOption(noVertRsp);
		
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
        
        if (line.hasOption(server.getOpt())) {
        	DB_SERVER = line.getOptionValue(server.getOpt());
        }
        
        if (!line.hasOption(lfRunID.getOpt())) {
        	System.err.println("Must specify low frequency runID.");
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	System.exit(-2);
        }
        int lf_run_id = Integer.parseInt(line.getOptionValue(lfRunID.getOpt()));
        sParams.setLfRunID(lf_run_id, DB_SERVER);

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
        
        if (line.hasOption(db_rvfrac_seed.getOpt())) {
        	sParams.setUseDBrvfracSeed(true);
        }
        
        if (line.hasOption(hf_velocity_model.getOpt())) {
        	sParams.setHfVelocityModel(line.getOptionValue(hf_velocity_model.getOpt()));
        }
        
        if (line.hasOption(z_comp.getOpt())) {
        	sParams.setZComp(true);
        	//Turn on calculation of vertical response spectra, but can be overrode by no-vert-rsp option
        	sParams.setCalculateVerticalResp(true);
        }
        
        if (line.hasOption(periodDepDuration.getOpt())) {
        	sParams.setRunPeriodDurations(true);
        }
        
        if (line.hasOption(noVertRsp.getOpt())) {
        	sParams.setCalculateVerticalResp(false);
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
	private Job[] createHFSynthJob(int sourceID, int ruptureID, int numRupVars, int numPoints, int numRows, int numCols, String localVMFilename,
			Job localVMJob, Job dirsJob, ADAG dax, double[] velocityArray, HashMap<String, String> rvSeedMap) {
		
		String dirPrefix = "" + sourceID;
		Job combineJob = null;
		Job combinePGAJob = null;
		int numTasks = 1;
		int numRupVarsPerTask = -1;
		
		DBConnect dbc = null;
		//Figure out how many tasks we need
		//Number of total rupture variation points per task to aim for
		if (riq.getRuptVarScenID()==8) {
			//We're using SRF files, so 1 task per rupture variation
			numTasks = numRupVars;
			//Create a dbc connection; we'll need it later
			dbc = new DBConnect(DB_SERVER, DB, USER, PASS);
		} else {
			double POINTS_PER_TASK = 1000000.0;
			numRupVarsPerTask = (int)Math.ceil(POINTS_PER_TASK/((double)numPoints));
			numTasks = (int)Math.ceil((double)numRupVars/(double)numRupVarsPerTask);
		}
			
		if (numTasks>1) {
			combineJob = new Job("Combine_HF_Synth_" + sourceID + "_" + ruptureID, NAMESPACE, COMBINE_NAME, "1.0");
			dax.addJob(combineJob);
			combinePGAJob = new Job("Combine_PGA_" + sourceID + "_" + ruptureID, NAMESPACE, COMBINE_NAME, "1.0");
			dax.addJob(combinePGAJob);
		}
		
		for (int i=0; i<numTasks; i++) {
			String id = "HF_Synth_" + sourceID + "_" + ruptureID;
			if (numTasks>1) {
				id += "_t" + i;
			}
			System.out.println("Creating job " + id);
			Job job = new Job(id, NAMESPACE, HF_SYNTH_NAME, "3.0");
			
			job.addArgument("stat=" + riq.getSiteName());
			job.addArgument("slat=" + riq.getLat());
			job.addArgument("slon=" + riq.getLon());
	
			job.addArgument("source_id=" + sourceID);
			job.addArgument("rupture_id=" + ruptureID);
			
			if (sParams.isUseDBrvfracSeed()) {
				job.addArgument("rvfrac_seed_given=1");
			}
			
			File seisFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
					"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION);
			File pgaFile = new File(dirPrefix + java.io.File.separator + "HighFreqPGA_" + riq.getSiteName() +
					"_" + sourceID + "_" + ruptureID + ".txt");
			if (numTasks>1) {
				seisFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
					"_" + sourceID + "_" + ruptureID + "_hf_t" + i + SEISMOGRAM_FILENAME_EXTENSION);
				pgaFile = new File(dirPrefix + java.io.File.separator + "HighFreqPGA_" + riq.getSiteName() +
						"_" + sourceID + "_" + ruptureID + "_t" + i + ".txt");
			}
			
			//The rupture variations are handled differently, depending on if we're passing SRFs in
			if (riq.getRuptVarScenID()==8) {
				//Get SRF filename from database
				String query = "select Rup_Var_LFN " + 
						"from Rupture_Variations " + 
						"where Rup_Var_Scenario_ID=" + riq.getRuptVarScenID() + " and ERF_ID=" + riq.getErfID() + " " + 
						"and Source_ID=" + sourceID + " and Rupture_ID=" + ruptureID + " and Rup_Var_ID=0";
				ResultSet rs = dbc.selectData(query);
				String rupVarLFN = null;
				try {
					rs.first();
				 	if (rs.getRow()==0) {
			      	    System.err.println("No Rup_Var_LFN found for query '" + query + "'.");
			      	    System.exit(1);
			      	}
					rupVarLFN = rs.getString("Rup_Var_LFN");
				} catch (SQLException e) {
					e.printStackTrace();
					System.exit(2);
				}
				File srfFilename = new File(rupVarLFN);
				srfFilename.setRegister(false);
				srfFilename.setTransfer(TRANSFER.TRUE);
				
				job.addArgument("infile=" + srfFilename.getName());
				job.uses(srfFilename, LINK.INPUT);
				
				//Source ID and rupture ID were included already
				job.addArgument("rup_var_id=" + i);
				
				//If we're running a BBP validation event, use a custom seed. 
				//This is not the place to put this long-term, but until we figure out
				//the suites of validations events we'll be running, leave it here.
				//Eventually, it should either be moved to a file or the DB.
				if (riq.getErfID()==60) {
					query = "select Rup_Var_Seed from Rup_Var_Seeds where ERF_ID=" + riq.getErfID() + " and Rup_Var_Scenario_ID=" + riq.getRuptVarScenID() + " and Source_ID=" + sourceID + " and Rupture_ID=" + ruptureID + " and Rup_Var_ID=" + i;
					rs = dbc.selectData(query);
					try {
						rs.first();
					 	if (rs.getRow()==0) {
				      	    System.err.println("No seeds found for source " + sourceID + ", rupture " + ruptureID + "rup var " + i + ".");
				      	    System.exit(1);
				      	}
						int srf_seed = rs.getInt("Rup_Var_Seed");
						job.addArgument("srf_seed=" + srf_seed);
					} catch (SQLException e) {
						e.printStackTrace();
						System.exit(2);
					}
				}
				
			} else {
				int startingRupVar = i*numRupVarsPerTask;
				int endingRupVar = Math.min((i+1)*numRupVarsPerTask, numRupVars);
				int numRupVarsThisTask = endingRupVar - startingRupVar;
			
				File rupGeomFile = new File("e" + riq.getErfID() + "_rv" + riq.getRuptVarScenID() + "_" + sourceID + "_" + ruptureID + ".txt");
				rupGeomFile.setRegister(false);
				rupGeomFile.setTransfer(TRANSFER.TRUE);
	
				job.addArgument("rup_geom_file=" + rupGeomFile.getName());
				job.uses(rupGeomFile, File.LINK.INPUT); 
				
				job.addArgument("num_rup_vars=" + numRupVarsThisTask);
				
				//Let's just always add this string, even if we're running all the RVs for a rupture
				//We need the rvfrac and seed, and it simplifies this code
//				if (numTasks>1) {
					//Construct rupture variation string
					StringBuffer rup_var_string = new StringBuffer("");
					//If we're including rvfrac and seed
					if (sParams.isUseDBrvfracSeed()==true) {
						String key, value;
						double rvfrac;
						int seed;
						for (int j=startingRupVar; j<endingRupVar; j++) {
							key = sourceID + "_" + ruptureID + "_" + j;
							value = rvSeedMap.get(key);
							rvfrac = Double.parseDouble(value.split("_")[0]);
							seed = Integer.parseInt(value.split("_")[1]);
							//(<rv_id>,<slip_id>,<hypo_id>,<rvfrac>,<seed>);(...)
							rup_var_string.append("(" + j + "," + j + "," + 0 + "," + rvfrac + "," + seed + ")");
							if (j<endingRupVar-1) {
								rup_var_string.append(";");
							}
						}
					} else {
						//If we aren't including rvfrac and seed
						for (int j=startingRupVar; j<endingRupVar; j++) {
							//(<rv_id>,<slip_id>,<hypo_id>);(...)
							rup_var_string.append("(" + j + "," + j + "," + 0 + ")");
							if (j<endingRupVar-1) {
								rup_var_string.append(";");
							}
						}
					}

					job.addArgument("rup_vars=" + rup_var_string.toString());
//				}
			}
		
			seisFile.setRegister(false);
			seisFile.setTransfer(TRANSFER.FALSE);
			job.uses(seisFile, File.LINK.OUTPUT);

			job.addArgument("outfile=" + seisFile.getName());

			pgaFile.setRegister(false);
			pgaFile.setTransfer(TRANSFER.FALSE);
			job.uses(pgaFile, File.LINK.OUTPUT);
			
			job.addArgument("pga_outfile=" + pgaFile.getName());
			
			job.addArgument("tlen=" + sParams.getTlen());
			job.addArgument("dt=" + DT);
			int doSiteResponse = 1;
			if (!sParams.isRunSiteResponse()) {
				doSiteResponse = 0;
			}
			job.addArgument("do_site_response=" + doSiteResponse);
			if (sParams.isRunSiteResponse()) {
				job.addArgument("vs30=" + riq.getVs30());
				job.addArgument("vref=" + sParams.getVref());
				job.addArgument("vpga=" + sParams.getVpga());
			}
			int debug = 0;
			if (sParams.isDebug()) {
				debug = 1;
			}
			job.addArgument("debug=" + debug);
			if (sParams.isZComp()) {
				job.addArgument("num_comps=3");
			} else {
				job.addArgument("num_comps=2");
			}


			File localVMFile = new File(localVMFilename);
			localVMFile.setRegister(false);
			//Set transfer to false, because we sometimes run the LocalVM job on the summit site,
			//but then run the HFSynth job in summit-pilot. If we keep this as transfer=TRUE, a transfer job
			//copies the file in-place (since summit and summit-pilot use the same directory),
			//which ends up deleting it.
			localVMFile.setTransfer(TRANSFER.FALSE);
			job.uses(localVMFile, LINK.INPUT);
	
			job.addArgument("vmod=" + localVMFile.getName());
		
			job.addProfile("pegasus", "pmc_priority", "" + numPoints);
	
			//Memory usage is the size of the SRF, the size of the output, and the size of some mysterious arrays in srf2stoch.
			//Everything is tracked in MB.
			//double srfMem = 14.8 * Math.log10(numPoints) * Math.pow(numPoints, 1.14) / (1024.0*1024.0);
			double single_rv_size = (6.491*Math.pow(numPoints, 1.314)*1.1)/(1024.0*1024.0);
			//Cap at 120 MB
			if (single_rv_size > 120) {
				single_rv_size = 120;
			}	
			//tmp SRF data is sizeof(complex)*(nstk*ndip*13.8+32.7*max(nstk, ndip)*max(nstk, ndip)
			int nstk = numCols;
			int ndip = numRows;
			//If numRows and NumCols are both -1, use min(sqrt(numPoints), 300) for numRows
			if (nstk==-1 || ndip==-1) {
				numCols = Math.min(300, (int)(Math.sqrt(numPoints)));
				numRows = (int)Math.ceil(numPoints/numCols);
				nstk = numCols;
				ndip = numRows;
			}
			double tmpSrfMem = (8.0*(nstk*ndip*13.8+32.7*Math.max(nstk, ndip)*Math.max(nstk, ndip)))/(1024.0*1024.0);
			double outputMem = sParams.getTlen()/DT * 2.0 * 4.0 / (1024.0 * 1024.0);
			//slipfile memory:  3 x NP x NQ x LV x sizeof(float)
			double slipfileMem = 3.0*500.0*2000.0*10.0*4.0 / (1024.0*1024.0);
			//Srf2stoch mem duplicates the slipfile mem, plus the reallocs
			int nx = (int)(Math.floor(nstk/5.0 + 0.5));
			int nxdiv = 1;
			while ((nstk*nxdiv)%nx!=0) {
				nxdiv++;
			}
			int ny = (int)(Math.floor(ndip/5.0 + 0.5));
			int nydiv = 1;
			while ((ndip*nydiv)%ny!=0) {
				nydiv++;
			}
			double srf2stochMem = slipfileMem + (3.0*nxdiv*nydiv*nstk*ndip*4.0+3*nx*ny*4.0)/(1024.0*1024.0);
			int memUsage = (int)(Math.ceil(1.1*(single_rv_size + tmpSrfMem + outputMem + slipfileMem + srf2stochMem)));
		
			job.addProfile("pegasus", "pmc_request_memory", "" + memUsage);
			job.addProfile("pegasus", "label", "pmc");
		
			dax.addJob(job);
			dax.addDependency(localVMJob, job);
			dax.addDependency(dirsJob, job);
			if (numTasks==1) {
				return new Job[] {job};
			} else {
				File combineSeisFile = new File(seisFile.getName());
				combineJob.addArgument(combineSeisFile.getName());
				combineSeisFile.setRegister(false);
				combineSeisFile.setTransfer(TRANSFER.FALSE);
				combineJob.uses(combineSeisFile, LINK.INPUT);

				dax.addDependency(job, combineJob);
				
				File combinePGAFile = new File(pgaFile.getName());
				combinePGAJob.addArgument(combinePGAFile.getName());
				combinePGAFile.setRegister(false);
				combinePGAFile.setTransfer(TRANSFER.FALSE);
				combinePGAJob.uses(combinePGAFile, LINK.INPUT);

				dax.addDependency(job, combinePGAJob);
			}
		}
		File combineSeisOutFile = new File(dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() +
				"_" + sourceID + "_" + ruptureID + "_hf" + SEISMOGRAM_FILENAME_EXTENSION);
		combineSeisOutFile.setRegister(false);
		combineSeisOutFile.setTransfer(TRANSFER.FALSE);
		combineJob.addArgument(combineSeisOutFile.getName());
		combineJob.uses(combineSeisOutFile, LINK.OUTPUT);
		
		combineJob.addProfile("pegasus", "label", "pmc");
		
		File combinePGAOutFile = new File(dirPrefix + java.io.File.separator + "HighFreqPGA_" + riq.getSiteName() +
				"_" + sourceID + "_" + ruptureID + ".txt");
		combinePGAOutFile.setRegister(false);
		combinePGAOutFile.setTransfer(TRANSFER.FALSE);
		combinePGAJob.addArgument(combinePGAOutFile.getName());
		combinePGAJob.uses(combinePGAOutFile, LINK.OUTPUT);
		combinePGAJob.addProfile("pegasus", "label", "pmc");
		
		if (dbc!=null) {
			dbc.closeConnection();
		}
		return new Job[]{combineJob, combinePGAJob};
	}
	
	private Job createLFSiteResponseJob(int sourceID, int ruptureID, int numRupVars, int num_points, double[] velocityArray) {
		String id = "LF_Site_Response_" + sourceID + "_" + ruptureID;

		Job job = new Job(id, NAMESPACE, LF_SITE_RESPONSE_NAME, "2.0");

		File seis_in = new File(SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
				"_" + sourceID + "_" + ruptureID + SEISMOGRAM_FILENAME_EXTENSION);
		
		seis_in.setRegister(false);
		seis_in.setTransfer(TRANSFER.TRUE);
		job.addArgument("seis_in=" + seis_in.getName());
		job.uses(seis_in, LINK.INPUT);
		
		String dir_prefix = "" + sourceID;

		File pgaFile = new File(dir_prefix + java.io.File.separator + "HighFreqPGA_" + riq.getSiteName() +
				"_" + sourceID + "_" + ruptureID + ".txt");

		pgaFile.setRegister(false);
		pgaFile.setTransfer(TRANSFER.FALSE);
		job.addArgument("pga_infile=" + pgaFile.getName());
		job.uses(pgaFile, LINK.INPUT);
		
		File seis_out = new File(dir_prefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
				"_" + sourceID + "_" + ruptureID + "_site_response" + SEISMOGRAM_FILENAME_EXTENSION);

		seis_out.setRegister(false);
		seis_out.setTransfer(TRANSFER.FALSE);
		job.addArgument("seis_out=" + seis_out.getName());
		job.uses(seis_out, LINK.OUTPUT);
		
		job.addArgument("slat=" + riq.getLat());
		job.addArgument("slon=" + riq.getLon());
		//Use the bssa2014 module when using CyberShake (3D) deterministic seismograms
		job.addArgument("module=bssa2014");
		//Need to set both Vs30 and Vref
		//Vs30 becomes vsite; we're using Thompson for this
		job.addArgument("vs30=" + riq.getVs30());
		//Vref = Vsite * Vs30 / VsD
		double vref = velocityArray[VS30] * velocityArray[VSD5H] / velocityArray[VS5H];
		//Round vref to nearest 0.1
		vref = ((double)((int)(vref*10.0)))/10.0;
		//If we're running a validation event, use vref=500.0
//		if (riq.getErfID()==60) {
//			vref = 500.0;
//		}
		job.addArgument("vref=" + vref);
		job.addArgument("vpga=" + sParams.getVpga());
		
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
		vmIn.setTransfer(TRANSFER.FALSE);
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

		Job job = new Job(id, NAMESPACE, MERGE_IM_NAME, "2.0");
		
		String dirPrefix = "" + sourceID;
		
		if (sParams.isRunLFSiteResponse()) {
			String lfSeisName = dirPrefix + java.io.File.separator + SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
					"_" + sourceID + "_" + ruptureID + "_site_response" + SEISMOGRAM_FILENAME_EXTENSION;
			File lfSeisFile = new File(lfSeisName);
			lfSeisFile.setTransfer(TRANSFER.FALSE);
			lfSeisFile.setRegister(false);
			job.uses(lfSeisFile, LINK.INPUT);
			job.addArgument("lf_seis=" + lfSeisFile.getName());
		} else {
			String lfSeisName = SEISMOGRAM_FILENAME_PREFIX + riq.getSiteName() + "_" + sParams.getLowFreqRIQ().getRunID() +
					"_" + sourceID + "_" + ruptureID + SEISMOGRAM_FILENAME_EXTENSION;
			File lfSeisFile = new File(lfSeisName);
			lfSeisFile.setTransfer(TRANSFER.TRUE);
			lfSeisFile.setRegister(false);
			job.uses(lfSeisFile, LINK.INPUT);
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
		int num_comps = 2;
		if (sParams.isZComp()) {
			num_comps = 3;
		}
		job.addArgument("comps=" + num_comps);
		job.addArgument("num_rup_vars=" + numRupVars);
		
		//PSA args
		double psaFilter = PSA_FILTER;
		if (psaFilter<2.0*sParams.getStochFrequency()) {
			psaFilter = 2.0*sParams.getStochFrequency();
		}
		
		int nt = (int)((sParams.getTlen()/DT + 0.5));
		
       	job.addArgument("simulation_out_pointsX=" + num_comps);
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
			if (sParams.isCalculateVerticalResp()) {
				job.addArgument("run_vert_rsp=1");
				String vertRspFilename = dirPrefix + java.io.File.separator + VERT_RSP_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + 
		    			"_" + sourceID + "_" + ruptureID + "_bb" + VERT_RSP_FILENAME_EXTENSION;
				File vertRspFile = new File(vertRspFilename);
				vertRspFile.setRegister(true);
				vertRspFile.setTransfer(TRANSFER.TRUE);
				job.uses(vertRspFile, LINK.OUTPUT);
				job.addArgument("vert_rsp_out=" + vertRspFile.getName());
			} else {
				job.addArgument("run_vert_rsp=0");
			}
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
		
		if (sParams.isRunPeriodDurations()) {
			job.addArgument("run_period_durations=1");
			String periodDurationFilename = dirPrefix + java.io.File.separator + PERIOD_DURATION_FILENAME_PREFIX + riq.getSiteName() + "_" + riq.getRunID() + 
	    			"_" + sourceID + "_" + ruptureID + "_bb" + PERIOD_DURATION_FILENAME_EXTENSION;
			File perDurFile = new File(periodDurationFilename);
			perDurFile.setRegister(true);
			perDurFile.setTransfer(TRANSFER.TRUE);
			job.uses(perDurFile, LINK.OUTPUT);
			job.addArgument("period_duration_out=" + perDurFile.getName());
		} else {
			job.addArgument("run_period_durations=0");
		}
		
		job.addProfile("pegasus", "label", "pmc");
		//Must read in both rupture files at once, merged seismogram, plus array in RotD code
		double lfMem = numRupVars * 2.0 * nt * 4;
		double hfMem = numRupVars * 2.0 * nt * 4;
		double mergeMem = numRupVars * 2.0 * nt * 4;
		double rotdMem = 4*2000000.0;
		double totMem = (int)Math.ceil(1.1*(lfMem + hfMem + rotdMem)/(1024.0*1024.0));
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
			
			String localVMFilename = hf_vel_model_map.get(sParams.getHfVelocityModel()) + ".local";
			
			Job localVMJob = createLocalVMJob(hf_vel_model_map.get(sParams.getHfVelocityModel()), localVMFilename);
			dax.addJob(localVMJob);
			dax.addDependency(updateJob, localVMJob);
			
			HashSet<String> dirNames = new HashSet<String>();
			String dirsInputFilename = "dirs_list.txt";
			Job dirsJob = createDirsJob(dirsInputFilename);
			dax.addJob(dirsJob);
			dax.addDependency(updateJob, dirsJob);
			
			int index = 0;
			
			//Create this out here so we only do it once
			HashMap<String, String> rvSeedMap = null;
			if (sParams.isUseDBrvfracSeed()) {
				rvSeedMap = populateRvfracSeedInfo();
			}
			
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
				Job[] hfSynthJobs = createHFSynthJob(sourceID, ruptureID, numRupVars, numPoints, numRows, numCols, localVMFilename, localVMJob, dirsJob, dax, vsArray, rvSeedMap);
				
				Job mergeIMJob = createMergeIMJob(sourceID, ruptureID, numRupVars, numPoints);
				dax.addJob(mergeIMJob);
				for (Job j: hfSynthJobs) {
					dax.addDependency(j, mergeIMJob);
				}

				if (sParams.isRunLFSiteResponse()) {
					Job lfSiteResponseJob = createLFSiteResponseJob(sourceID, ruptureID, numRupVars, numPoints, vsArray);
					dax.addJob(lfSiteResponseJob);
					for (Job j: hfSynthJobs) {
						dax.addDependency(j, lfSiteResponseJob);
					}
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
			//Switch this to using the GO endpoint
			dirsPegasusFile.addPhysicalFile("go://" + CARC_GO_ENDPOINT + fullPath, "local");
			//dirsPegasusFile.addPhysicalFile("file://" + fullPath, "local");
			dax.addFile(dirsPegasusFile);
		} catch (SQLException se) {
			se.printStackTrace();
			System.exit(3);
		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(4);
		}
		
	}
	
	HashMap<String, String> populateRvfracSeedInfo() {
		DBConnect dbc = new DBConnect(DB_SERVER, DB, USER, PASS);
		HashMap<String, String> rvSeedMap = new HashMap<String, String>();
		//Get rvfrac and seed values
		double cutoffDist = 200.0;
		if (riq.getSiteName().equals("TEST")) {
			cutoffDist = 20.0;
		}
		String query = "select V.Source_ID, V.Rupture_ID, V.Rup_Var_ID, V.rvfrac, D.Rup_Var_Seed " + 
				"from Rupture_Variations V, Rup_Var_Seeds D, CyberShake_Site_Ruptures SR, CyberShake_Sites S " + 
				"where S.CS_Short_Name='" + riq.getSiteName() + "' " +
				"and S.CS_Site_ID=SR.CS_Site_ID " + 
				"and SR.ERF_ID=" + riq.getErfID() + " " +
				"and SR.ERF_ID=V.ERF_ID " + 
				"and SR.Source_ID=V.Source_ID " + 
				"and SR.Rupture_ID=V.Rupture_ID " + 
				"and SR.Cutoff_Dist= " + cutoffDist + " " +
				"and V.Rup_Var_Scenario_ID=" + riq.getRuptVarScenID() + " " +  
				"and D.Rup_Var_Scenario_ID=V.Rup_Var_Scenario_ID " + 
				"and D.ERF_ID=SR.ERF_ID " + 
				"and D.Source_ID=V.Source_ID " + 
				"and D.Rupture_ID=V.Rupture_ID " + 
				"and D.Rup_Var_ID=V.Rup_Var_ID " + 
				"order by D.Source_ID asc, D.Rupture_ID asc, D.Rup_Var_ID asc";
		ResultSet ruptures = dbc.selectData(query);
		try {
			ruptures.first();
			if (ruptures.getRow()==0) {
				System.err.println("No ruptures found for site " + riq.getSiteName() + ", aborting.");
				System.exit(1);
			}
			while (!ruptures.isAfterLast()) {
				int source_id = ruptures.getInt("V.Source_ID");
				int rupture_id = ruptures.getInt("V.Rupture_ID");
				int rup_var_id = ruptures.getInt("V.Rup_Var_ID");
				double rvfrac = ruptures.getDouble("V.rvfrac");
				int seed = ruptures.getInt("D.Rup_Var_Seed");
				String key = source_id + "_" + rupture_id + "_" + rup_var_id;
				String value = rvfrac + "_" + seed;
				rvSeedMap.put(key, value);
				ruptures.next();
			}
			
		} catch (SQLException sqe) {
			sqe.printStackTrace();
			dbc.closeConnection();
			System.exit(2);
		}
		dbc.closeConnection();
		return rvSeedMap;
	}
}
