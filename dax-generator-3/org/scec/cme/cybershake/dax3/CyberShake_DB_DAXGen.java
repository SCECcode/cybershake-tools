package org.scec.cme.cybershake.dax3;

import java.io.FileWriter;
import java.io.IOException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.File.LINK;
import edu.isi.pegasus.planner.dax.File.TRANSFER;
import edu.isi.pegasus.planner.dax.Job;

public class CyberShake_DB_DAXGen {
	//Paths
	//Condor will expand this into the submit username
	public static final String USER = "$ENV(USER)";
	
	public static final String DB_CHECK_OUTFILE_DIR = "/home/shock/scottcal/db_products/db_check_outfiles/";
	public static final String CURVE_OUTPUT_DIR_PREFIX = "/home/shock/scottcal/db_products/curves/";
	public static final String DISAGG_OUTPUT_DIR_PREFIX = "/home/shock/scottcal/db_products/disagg/";
	public static final String OPENSHA_CONF_DIR = "/home/shock-ssd/scottcal/opensha/conf/";
	//public static final String CURVE_ATTEN_REL_XML_FILES = OPENSHA_CONF_DIR + "cb2008.xml"
	//		+ "," + OPENSHA_CONF_DIR + "ba2008.xml"
	//		+ "," + OPENSHA_CONF_DIR + "cy2008.xml"
	//		+ "," + OPENSHA_CONF_DIR + "as2008.xml";
	public static final String CURVE_ATTEN_REL_XML_FILES = OPENSHA_CONF_DIR + "ask2014.xml"
			+ "," + OPENSHA_CONF_DIR + "bssa2014.xml"
			+ "," + OPENSHA_CONF_DIR + "cb2014.xml"
			+ "," + OPENSHA_CONF_DIR + "cy2014.xml";
	public static final String DB_PASS_FILE = "/home/shock/scottcal/runs/config/db_pass.txt";
	public static final String DB_REPORT_OUTPUT_DIR = "/home/shock/scottcal/db_products/db_reports/";
//	public static final String STORAGE_DIR = "/home/scec-02/tera3d/CyberShake2007/data/PPFiles/";
	//scec-04 storage dir
	public static final String STORAGE_DIR = "/project/scec_608/cybershake/results/PPFiles/";

	//Constants
	public static final String DAX_FILENAME_POST = "_db_products";
	public static final String DB_PREFIX = "CS_Products_";
	public static final String DB_CHECK_OUTFILE_PREFIX = "DB_Check_Out_";
	public static final String UCERF2_ERF_FILE = OPENSHA_CONF_DIR + "MeanUCERF.xml"; 
	public static final String RSQSIM_ERF42_FILE = "/home/scec-02/kmilner/simulators/catalogs/rundir2457/erf_params.xml";
	public static final String RSQSIM_ERF48_FILE = "/home/scec-02/kmilner/simulators/catalogs/rundir2585_1myrs/erf_params.xml";
	public static final String RSQSIM_ERF61_FILE = "/home/shock-ssd/scottcal/opensha/conf/erf61_params.xml";
	public static final String RSQSIM_ERF62_FILE = "/home/shock-ssd/scottcal/opensha/conf/erf62_params.xml";
	public static final String CURVE_CALC_PERIODS = "3,5,10";
	public static final String CURVE_OUTPUT_TYPES = "pdf,png";
	public static final double CURVE_DEFAULT_VS30 = 760;
	
	public static final String ROTD_CALC_PERIODS = "3,4,5,7.5,10";
	public static final String ROTD_OUTPUT_TYPES = "pdf,png";
	
	//DB info
	public static String DB_SERVER = "moment";
	
	//Job names
	public static final String DB_INSERT_NAME = "Load_Amps";
	public static final String DB_CHECK_NAME = "Check_DB_Site";
	public static final String CURVE_CALC_NAME = "Curve_Calc";
	public static final String CURVE_CALC_WRAPPER_NAME = "Curve_Calc_Wrapper";
	public static final String DB_REPORT_NAME = "DB_Report";
	public static final String SCATTER_MAP_NAME = "Scatter_Map";
    private final static String CYBERSHAKE_NOTIFY = "CyberShakeNotify";	
    private final static String DBWRITE_STAGE = "DBWrite";
    private final static String DISAGG_NAME = "Disaggregate";
	
	//Instance variables
	private RunIDQuery riq;
	private String filesDir;
	private boolean transferZipFiles = true;
	private int numDAXes;
	private PP_DAXParameters params;
	// Insert duration results
	private boolean insertDurations = false;
	// File with Vs30 value, for use when calculating comparison curves with stochastic results
	private String velocityFile = null;
	
	//Job selection
	// boolean to enable/disable scatter map plotting
	private static final boolean DO_SCATTER_MAP = false;
	// boolean to enable/disable curve generation
	private static final boolean DO_CURVE_GEN = true;
	// boolean to enable/disable curve plotting. if DO_CURVE_GEN is false, then this is ignored.
	private static final boolean DO_CURVE_PLOT = true;

    //Periods used as proxies for PGA or PGV
    private double PGA_period = 1.0e-4;
    private double PGV_period = 1.0e-5;
	
	public CyberShake_DB_DAXGen(RunIDQuery r) {
		riq = r;
		filesDir = ".";
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, PP_DAXParameters params, int numDAXes, boolean transferZip) {
		this(r);
		transferZipFiles = transferZip;
		this.numDAXes = numDAXes;
		this.params = params;
		//For Study 18.8, insert durations 
		insertDurations = params.isCalculateDurations();
		if (params.isFileForward() || params.isPipeForward()){
			this.filesDir = STORAGE_DIR + "/" + r.getSiteName() + "/" + r.getRunID();
		} else {
			this.filesDir = ".";
		}
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, PP_DAXParameters params, int numDAXes, boolean transferZip, String db_server) {
		this(r, params, numDAXes, transferZip);
		DB_SERVER = db_server.split("\\.")[0];
	}
	
	
	public CyberShake_DB_DAXGen(RunIDQuery r, int numDAXes, boolean highFreq, double highFreqCutoff, boolean transferZip, boolean rotD) {
		this(r);
		transferZipFiles = transferZip;
		this.numDAXes = numDAXes;
		params = new PP_DAXParameters();
		params.setStochastic(highFreq);
		params.setStochasticCutoff(highFreqCutoff);
		params.setCalculateRotD(rotD);
		params.setDetFrequency(r.getLowFrequencyCutoff());
		if (params.isFileForward() || params.isPipeForward()){
			this.filesDir = STORAGE_DIR + "/" + r.getSiteName() + "/" + r.getRunID();
		} else {
			this.filesDir = ".";
		}
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, int numDAXes, boolean highFreq, double highFreqCutoff, boolean transferZip, boolean rotD, boolean duration) {
		this(r, numDAXes, highFreq, highFreqCutoff, transferZip, rotD);
		insertDurations = duration;
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, int numDAXes, boolean highFreq, double highFreqCutoff, boolean transferZip, String db_server, boolean rotD, boolean duration) {
		this(r, numDAXes, highFreq, highFreqCutoff, transferZip, rotD);
		insertDurations = duration;
		DB_SERVER = db_server.split("\\.")[0];
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, int numDAXes, boolean highFreq, double highFreqCutoff, boolean transferZip, boolean rotD, boolean duration, String velocityFile) {
		this(r, numDAXes, highFreq, highFreqCutoff, transferZip, rotD, duration);
		this.velocityFile = velocityFile;
	}
	
	public ADAG makeDAX() {
		String daxName = CyberShake_PP_DAXGen.DAX_FILENAME_PREFIX + riq.getSiteName() + DAX_FILENAME_POST;
		
		ADAG dax = new ADAG(daxName, 0, 1);
		
		Job zipPSAJob = null;
		
		//Start by adding zip job, if necessary
		if (!params.isZip() && !(params.isFileForward() || params.isPipeForward())) {
			//Need to zip seis and PSA up on local site
			String id = "ZipSeis";
	    	Job zipSeisJob = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, "ZipSeis", "1.0");
	    	File zipSeisFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_seismograms.zip");
	    	zipSeisFile.setRegister(true);
	    	
	    	zipSeisJob.addArgument(STORAGE_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
	    	zipSeisJob.addArgument(zipSeisFile.getName());
	    	zipSeisJob.uses(zipSeisFile, File.LINK.OUTPUT);

	    	dax.addJob(zipSeisJob);
	    	
	    	String id2 = "ZipPSA";
	    	zipPSAJob = new Job(id2, CyberShake_PP_DAXGen.NAMESPACE, "ZipPSA", "1.0");
	    	File zipPSAFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_PSA.zip");
	    	zipPSAFile.setRegister(true);

	    	zipPSAJob.addArgument(STORAGE_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
	    	zipPSAJob.addArgument(zipPSAFile.getName());
	    	zipPSAJob.uses(zipPSAFile, File.LINK.OUTPUT);

	    	dax.addJob(zipPSAJob);
	    	
	    	//Delete .bsa and .grm files
	    	id = "DeleteSeis";
	    	Job deleteSeisJob = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, "DeleteSeis", "1.0");
	    	deleteSeisJob.addArgument(STORAGE_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
	    	dax.addJob(deleteSeisJob);
	    	dax.addDependency(zipSeisJob, deleteSeisJob);
	    	
	    	id = "DeletePSA";
	    	Job deletePSAJob = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, "DeletePSA", "1.0");
	    	deletePSAJob.addArgument(STORAGE_DIR + "/" + riq.getSiteName() + "/" + riq.getRunID());
	    	dax.addJob(deletePSAJob);
	    	dax.addDependency(zipPSAJob, deletePSAJob);
		}
				
		// Add workflow jobs
		Job rotDJob = null;
		Job insertJob = createDBInsertionJob();
		dax.addJob(insertJob);
		//If we needed to add local zip jobs, add dependency on PSA zip
		if (!params.isZip() && !(params.isFileForward() || params.isPipeForward())) {
			dax.addDependency(zipPSAJob, insertJob);
		}
		
		if (params.isCalculateRotD()) {
			rotDJob = createRotDInsertionJob();
			dax.addJob(rotDJob);
			//Make this a child of insertJob to avoid having both insertion jobs hit the DB at the same time
			dax.addDependency(insertJob, rotDJob);
		}
		
		Job durationJob = null;
		if (insertDurations) {
			durationJob = createInsertDurationJob();
			dax.addJob(durationJob);
			//Make this a child so we don't have multiple insertion jobs running at the same time
			if (rotDJob!=null) {
				dax.addDependency(rotDJob, durationJob);
			} else {
				dax.addDependency(insertJob, durationJob);
			}
		}
		
		Job dbCheckJob = createDBCheckJob();
		dax.addJob(dbCheckJob);
		
		Job curveCalcJob = null;
		Job disaggJob = null;
		if (DO_CURVE_GEN) {
			curveCalcJob = createCurveCalcJob();
			dax.addJob(curveCalcJob);
			dax.addDependency(dbCheckJob, curveCalcJob);
			if (params.isCalculateRotD()) {
				Job rotdCheckJob = createDBCheckRotDJob();
				dax.addJob(rotdCheckJob);
				dax.addDependency(rotDJob, rotdCheckJob);
				Job rotd100CalcJob = createRotD100CurveCalcJob();
				dax.addJob(rotd100CalcJob);
				dax.addDependency(rotdCheckJob, rotd100CalcJob);
				Job rotd50CalcJob = createRotD50CurveCalcJob();
				dax.addJob(rotd50CalcJob);
				dax.addDependency(rotdCheckJob, rotd50CalcJob);
			}
			if (insertDurations) {
				Job durationCheckJob = createDurationCheckJob();
				dax.addJob(durationCheckJob);
				dax.addDependency(durationJob, durationCheckJob);
			}
			
			disaggJob = createDisaggJob();
			dax.addJob(disaggJob);
			dax.addDependency(curveCalcJob, disaggJob);

		}
		//11-21: Remove DB report job from workflow
		//Job reportJob = createDBReportJob();
		//dax.addJob(reportJob);
		Job scatterMapJob = null;
		if (DO_SCATTER_MAP) {
			scatterMapJob = createScatterMapJob();
			dax.addJob(scatterMapJob);
		}

		
		// Add notification job
		Job notifyJob = createNotify(DBWRITE_STAGE);
		dax.addJob(notifyJob);
		
		// PARENT CHILD RELATIONSHIPS
		
		// check is a child of insert
		dax.addDependency(insertJob, dbCheckJob);
		if (params.isCalculateRotD()) {
			dax.addDependency(rotDJob, dbCheckJob);
		}
		
		// curve calc is a child of check
		if (DO_CURVE_GEN) {
			
		}		
		
		// report is a child of check
		//dax.addDependency(dbCheckJob, reportJob);

		// scatter map is a child of curve calc
		if (DO_SCATTER_MAP) {
			dax.addDependency(curveCalcJob, scatterMapJob);
		    // notification is child of scatter map
			dax.addDependency(scatterMapJob, notifyJob);
		} else {
		    // notification is child of report
			//dax.addDependency(reportJob, notifyJob);
		}

		return dax;
	}
	
	
	private Job createInsertDurationJob() {
		String id = DB_PREFIX + "Load_Durations" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_INSERT_NAME, CyberShake_PP_DAXGen.VERSION);
		
		if (transferZipFiles) {
			for (int i=0; i<numDAXes; i++) {
				File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + i + "_PSA.zip");
				zipFile.setRegister(false);
				job.uses(zipFile, File.LINK.INPUT);
			}
		}
		
		job.addArgument("-server " + DB_SERVER);
		if (transferZipFiles) {
			job.addArgument("-z");
		}
		//For duration files
		job.addArgument("-u");
		job.addArgument("-p " + filesDir);

		job.addArgument("-run " + riq.getRunID());
		//This isn't used, but CyberCommands requires it
		job.addArgument("-periods 2,3,5,10");
		
		job.addProfile("globus", "maxWallTime","60");
		job.addProfile("hints","executionPool", "shock");
		
		return job;
	}
		
	
	private Job createRotDInsertionJob() {
		String id = DB_PREFIX + "Load_Amps_RotD" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_INSERT_NAME, CyberShake_PP_DAXGen.VERSION);
		
		if (transferZipFiles) {
			for (int i=0; i<numDAXes; i++) {
				File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + i + "_PSA.zip");
				zipFile.setRegister(false);
				job.uses(zipFile, File.LINK.INPUT);
			}
		}
		/* Not sure that this is used
		 * } else {
			File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() +  "_PSA.zip");
			zipFile.setRegister(false);
			job.uses(zipFile, File.LINK.INPUT);
		}*/
		
		job.addArgument("-server " + DB_SERVER);
		if (transferZipFiles) {
			job.addArgument("-z");
		}
		//For RotD files
		job.addArgument("-r");
		//Convert to cm/s^2
		job.addArgument("-c");
		job.addArgument("-p " + filesDir);

		job.addArgument("-run " + riq.getRunID());
		String periods = "10,7.5,5,4,3";
		if (params.getDetFrequency()>=1.0) {
			periods = periods + ",2";
		}
		if (params.isStochastic()) {
			periods = periods + ",1,0.75,0.5,0.4,0.3,0.2,0.1,PGA,PGV";
			if (params.getStochasticFrequency()>10.0) {
				periods = periods + ",0.075,0.05";
			}
			if (params.getStochasticFrequency()>20.0) {
				periods = periods + "0.04,0.03,0.02,0.01";
			}
		} else {
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("-periods " + periods);
		
		job.addProfile("globus", "maxWallTime","60");
		job.addProfile("hints","executionPool", "shock");
		
		return job;
	}

	private Job createDisaggJob() {
		//./disagg_plot_wrapper.sh --run-id 247 --period 3 --probs 4.0e-4 --imls 0.2,0.5 --output-dir /tmp
		String id = DISAGG_NAME + "_" + riq.getSiteName();
		Job disaggJob = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DISAGG_NAME, CyberShake_PP_DAXGen.VERSION);
		disaggJob.addArgument("--run-id " + riq.getRunID());
		disaggJob.addArgument("--period 3");
		disaggJob.addArgument("--probs 4.0e-4");
		disaggJob.addArgument("--output-dir " + DISAGG_OUTPUT_DIR_PREFIX);
		disaggJob.addArgument("--type pdf,png,txt");
		String erfFile = UCERF2_ERF_FILE;
		if (riq.getErfID()==42) {
			erfFile = RSQSIM_ERF42_FILE;
		} else if (riq.getErfID()==48){
			erfFile = RSQSIM_ERF48_FILE;
		}
		disaggJob.addArgument("--erf-file " + erfFile);
		
		disaggJob.addProfile("globus", "maxWallTime", "5");
		
		return disaggJob;
	}

	private Job createNotify(String stage) {
        String id = DB_PREFIX + CYBERSHAKE_NOTIFY + "_" + riq.getSiteName() + "_" + stage;
    	Job notifyJob = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CYBERSHAKE_NOTIFY, CyberShake_PP_DAXGen.VERSION);
    	
        notifyJob.addArgument(riq.getRunID() + "");
        notifyJob.addArgument("PP");
        notifyJob.addArgument(stage);
        notifyJob.addArgument("0");
        notifyJob.addArgument("0");

        notifyJob.addProfile("globus", "maxWallTime", "5");

        return notifyJob;
	}

	private Job createScatterMapJob() {
		String id = DB_PREFIX + "Scatter_Map" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, SCATTER_MAP_NAME, CyberShake_PP_DAXGen.VERSION);
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "opensha");
		
		return job;
	}

	private Job createDBReportJob() {
		String id = DB_PREFIX + "DB_Report" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_REPORT_NAME, CyberShake_PP_DAXGen.VERSION);
		
		job.addArgument("--runID=" + riq.getRunID());
		// it will automatically come up with a file name if given a directory
		job.addArgument("--file=" + DB_REPORT_OUTPUT_DIR);
		job.addArgument(riq.getSiteName());
		//Add arguments to be sure we use the right data
		job.addArgument("--erfID " + riq.getErfID());
		job.addArgument("--rupVarID " + riq.getRuptVarScenID());
		job.addArgument("--sgtVarID " + riq.getSgtVarID());
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "shock");
		
		return job;
	}

	private Job createCurveCalcJob() {
		//If running stochastic curves, then run the wrapper instead, which will read the velocity file to retrieve the right Vs30
		//Doing it this way b/c it's easier than creating the DAX on the fly
		String id = null;
		Job job = null;
		if (params.isStochastic()) {
			id = DB_PREFIX + "Curve_Calc_Wrapper" + "_" + riq.getSiteName();
			job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_WRAPPER_NAME, CyberShake_PP_DAXGen.VERSION);
			if (this.velocityFile==null) {
				System.err.println("Error!  Velocity file must be specified for Vs30 for stochastic curve calculation, exiting.");
				System.exit(1);
			}
			File velocityFile = new File(this.velocityFile);
			job.addArgument(velocityFile.getName());
			velocityFile.setTransfer(TRANSFER.FALSE);
			velocityFile.setRegister(false);
			job.uses(velocityFile, LINK.INPUT);
		} else {
			id = DB_PREFIX + "Curve_Calc" + "_" + riq.getSiteName();
			job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_NAME, CyberShake_PP_DAXGen.VERSION);
		}
		
		String outputDir = CURVE_OUTPUT_DIR_PREFIX + riq.getSiteName();
		
		job.addArgument("--site " + riq.getSiteName());
		job.addArgument("--run-id " + riq.getRunID());
		String erfFile = UCERF2_ERF_FILE;
		if (riq.getErfID()==42) {
			erfFile = RSQSIM_ERF42_FILE;
		} else if (riq.getErfID()==48){
			erfFile = RSQSIM_ERF48_FILE;
		}
		job.addArgument("--erf-file " + erfFile);
		//Comment this out until we update so that we can use Velocity_Model_ID=4 data
		job.addArgument("--atten-rel-file " + CURVE_ATTEN_REL_XML_FILES);
		String periods = CURVE_CALC_PERIODS;
		if (params.isStochastic()) {
			periods += ",2,1,0.5,0.2,0.1";
		} else {
			if (params.getDetFrequency()>=1.0) {
				periods = periods + ",2";
			}
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("--period " + periods);
		job.addArgument("--output-dir " + outputDir);
		job.addArgument("--type " + CURVE_OUTPUT_TYPES);
		// this makes it calculate and the add the curve without prompting if needed
		job.addArgument("--force-add");
		
		// db password file
		job.addArgument("--password-file " + DB_PASS_FILE);
		if (!DO_CURVE_PLOT) {
			// this makes it just calculate the curve, without plotting
			job.addArgument("--calc-only");
		}
		
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}

	private Job createRotD100CurveCalcJob() {
		//If running stochastic curves, then run the wrapper instead, which will read the velocity file to retrieve the right Vs30
		//Doing it this way b/c it's easier than creating the DAX on the fly
		String id = null;
		Job job = null;
		if (params.isStochastic()) {
			id = DB_PREFIX + "Curve_Calc_RotD100_Wrapper" + "_" + riq.getSiteName();
			job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_WRAPPER_NAME, CyberShake_PP_DAXGen.VERSION);
			if (this.velocityFile==null) {
				System.err.println("Error!  Velocity file must be specified for Vs30 for stochastic curve calculation, exiting.");
				System.exit(1);
			}
			File velocityFile = new File(this.velocityFile);
			job.addArgument(velocityFile.getName());
			velocityFile.setTransfer(TRANSFER.FALSE);
			velocityFile.setRegister(false);
			job.uses(velocityFile, LINK.INPUT);
		} else {
			id = DB_PREFIX + "Curve_Calc_RotD100" + "_" + riq.getSiteName();
			job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_NAME, CyberShake_PP_DAXGen.VERSION);
		}
				
		String outputDir = CURVE_OUTPUT_DIR_PREFIX + riq.getSiteName();
		
		job.addArgument("--site " + riq.getSiteName());
		job.addArgument("--run-id " + riq.getRunID());
		String erfFile = UCERF2_ERF_FILE;
		if (riq.getErfID()==42) {
			erfFile = RSQSIM_ERF42_FILE;
		} else if (riq.getErfID()==48){
			erfFile = RSQSIM_ERF48_FILE;
		}
		job.addArgument("--erf-file " + erfFile);
		//Comment this out until we update so that we can use Velocity_Model_ID=4 data
		job.addArgument("--atten-rel-file " + CURVE_ATTEN_REL_XML_FILES);
		String periods = ROTD_CALC_PERIODS;
		if (params.isStochastic()) {
			periods += ",2,1,0.5,0.2,0.1";
		} else {
			if (params.getDetFrequency()>=1.0) {
				periods = periods + ",2";
			}
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("--period " + periods);
		job.addArgument("--output-dir " + outputDir);
		job.addArgument("--type " + ROTD_OUTPUT_TYPES);
		// this makes it calculate and the add the curve without prompting if needed
		job.addArgument("--force-add");
		job.addArgument("--cmp RotD100");
		
		// db password file
		job.addArgument("--password-file " + DB_PASS_FILE);
		if (!DO_CURVE_PLOT) {
			// this makes it just calculate the curve, without plotting
			job.addArgument("--calc-only");
		}
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}
	
	private Job createRotD50CurveCalcJob() {
		//If running stochastic curves, then run the wrapper instead, which will read the velocity file to retrieve the right Vs30
		//Doing it this way b/c it's easier than creating the DAX on the fly
		String id = null;
		Job job = null;
		if (params.isStochastic()) {
			id = DB_PREFIX + "Curve_Calc_RotD50_Wrapper" + "_" + riq.getSiteName();
			job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_WRAPPER_NAME, CyberShake_PP_DAXGen.VERSION);
			if (this.velocityFile==null) {
				System.err.println("Error!  Velocity file must be specified for Vs30 for stochastic curve calculation, exiting.");
				System.exit(1);
			}
			File velocityFile = new File(this.velocityFile);
			job.addArgument(velocityFile.getName());
			velocityFile.setTransfer(TRANSFER.FALSE);
			velocityFile.setRegister(false);
			job.uses(velocityFile, LINK.INPUT);
		} else {
			id = DB_PREFIX + "Curve_Calc_RotD50" + "_" + riq.getSiteName();
			job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_NAME, CyberShake_PP_DAXGen.VERSION);
		}
				
		String outputDir = CURVE_OUTPUT_DIR_PREFIX + riq.getSiteName();
		
		job.addArgument("--site " + riq.getSiteName());
		job.addArgument("--run-id " + riq.getRunID());
		String erfFile = UCERF2_ERF_FILE;
		if (riq.getErfID()==42) {
			erfFile = RSQSIM_ERF42_FILE;
		} else if (riq.getErfID()==48){
			erfFile = RSQSIM_ERF48_FILE;
		}
		job.addArgument("--erf-file " + erfFile);
		//Comment this out until we update so that we can use Velocity_Model_ID=4 data
		job.addArgument("--atten-rel-file " + CURVE_ATTEN_REL_XML_FILES);
		String periods = ROTD_CALC_PERIODS;
		if (params.isStochastic()) {
			periods += ",2,1,0.5,0.2,0.1";
		} else {
			if (params.getDetFrequency()>=1.0) {
				periods = periods + ",2";
			}
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("--period " + periods);
		job.addArgument("--output-dir " + outputDir);
		job.addArgument("--type " + ROTD_OUTPUT_TYPES);
		// this makes it calculate and the add the curve without prompting if needed
		job.addArgument("--force-add");
		job.addArgument("--cmp RotD50");
		
		// db password file
		job.addArgument("--password-file " + DB_PASS_FILE);
		if (!DO_CURVE_PLOT) {
			// this makes it just calculate the curve, without plotting
			job.addArgument("--calc-only");
		}
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}
	
	private Job createDBCheckJob() {
		String id = DB_PREFIX + "DB_Check" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_CHECK_NAME, CyberShake_PP_DAXGen.VERSION);
		
		job.addArgument("-r " + riq.getRunID());
		
		String outFile = DB_CHECK_OUTFILE_DIR + DB_CHECK_OUTFILE_PREFIX + riq.getSiteName();
		
		job.addArgument("-o " + outFile);
		job.addArgument("-c geometric");
		String periods = "10,5,3";
		if (params.isStochastic()) {
			periods = periods + ",2,1,0.5,0.2,0.1";
		} else {
			if (params.getDetFrequency()>=1.0) {
				periods = periods + ",2";
			}
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("-p " + periods);
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}
	
	private Job createDBCheckRotDJob() {
		String id = DB_PREFIX + "DB_Check_RotD" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_CHECK_NAME, CyberShake_PP_DAXGen.VERSION);
		
		job.addArgument("-r " + riq.getRunID());
		
		String outFile = DB_CHECK_OUTFILE_DIR + DB_CHECK_OUTFILE_PREFIX + "RotD_" +  riq.getSiteName();
		
		job.addArgument("-o " + outFile);
		job.addArgument("-c rotd");
		String periods = "10,7.5,5,4,3";
		if (params.isStochastic()) {
			periods = periods + ",2,1,0.5,0.2,0.1";
		} else {
			if (params.getDetFrequency()>=1.0) {
				periods = periods + ",2";
			}
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("-p " + periods);
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}
	
	private Job createDurationCheckJob() {
		String id = DB_PREFIX + "DB_Check_Duration" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_CHECK_NAME, CyberShake_PP_DAXGen.VERSION);
		
		job.addArgument("-r " + riq.getRunID());
		
		String outFile = DB_CHECK_OUTFILE_DIR + DB_CHECK_OUTFILE_PREFIX + "Duration_" +  riq.getSiteName();
		
		job.addArgument("-o " + outFile);
		job.addArgument("-c duration");

		//List of IM_Type_IDs for duration
		job.addArgument("-t 176,177,180,181");
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}

	private Job createDBInsertionJob() {
		String id = DB_PREFIX + "Load_Amps" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, DB_INSERT_NAME, CyberShake_PP_DAXGen.VERSION);
		
		if (transferZipFiles) {
			for (int i=0; i<numDAXes; i++) {
				File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + i + "_PSA.zip");
				zipFile.setRegister(false);
				job.uses(zipFile, File.LINK.INPUT);
			}
		}
		/* Not sure that this is used
		 * } else {
			File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() +  "_PSA.zip");
			zipFile.setRegister(false);
			job.uses(zipFile, File.LINK.INPUT);
		}*/
		
		job.addArgument("-server " + DB_SERVER);
		if (transferZipFiles) {
			job.addArgument("-z");
		} else if (params.isFileForward() || params.isPipeForward()) {
			//Using header files
			job.addArgument("-d");
		}
		
		job.addArgument("-p " + filesDir);

		job.addArgument("-run " + riq.getRunID());
		String periods = "10,5,3";
		if (params.isStochastic()) {
			periods = periods + ",2,1,0.5,0.2,0.1";
		} else {
			if (params.getDetFrequency()>=1.0) {
				periods = periods + ",2";
			}
			if (params.getDetFrequency()>=2.0) {
				periods = periods + ",1";
			}
		}
		job.addArgument("-periods " + periods);
		
		job.addProfile("globus", "maxWallTime","60");
		job.addProfile("hints","executionPool", "shock");
		
		return job;
	}

	public static void main(String args[]) {
		if (args.length!=1 && args.length!=2) {
			System.out.println("USAGE: CyberShakeDBProductsDAXGen RUN_ID [NUM_DAXES]");
			System.exit(1);
		}
		int runID = Integer.parseInt(args[0]);
		CyberShake_DB_DAXGen gen;
		RunIDQuery rid = new RunIDQuery(runID);
		
		if (args.length == 2)
			gen = new CyberShake_DB_DAXGen(rid, Integer.parseInt(args[1]), false, 0.5, true, true);
		else
			gen = new CyberShake_DB_DAXGen(rid);
		
		gen.makeDAX();
	}
}


