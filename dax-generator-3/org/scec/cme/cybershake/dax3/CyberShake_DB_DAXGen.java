package org.scec.cme.cybershake.dax3;

import java.io.FileWriter;
import java.io.IOException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;

public class CyberShake_DB_DAXGen {
	//Paths
	//Condor will expand this into the submit username
	public static final String USER = "$ENV(USER)";
	
	public static final String DB_CHECK_OUTFILE_DIR = "/home/scec-02/tera3d/CyberShake2007/cybershk/pegasus/db_check_outfiles/";
	public static final String CURVE_OUTPUT_DIR_PREFIX = "/home/scec-00/" + USER + "/opensha/curves/";
	public static final String DISAGG_OUTPUT_DIR_PREFIX = "/home/scec-00/" + USER + "/opensha/disagg/";
	public static final String OPENSHA_CONF_DIR = "/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/";
	public static final String CURVE_ATTEN_REL_XML_FILES = OPENSHA_CONF_DIR + "cb2008.xml"
			+ "," + OPENSHA_CONF_DIR + "ba2008.xml"
			+ "," + OPENSHA_CONF_DIR + "cy2008.xml"
			+ "," + OPENSHA_CONF_DIR + "as2008.xml";
	public static final String DB_PASS_FILE = "/home/scec-00/" + USER + "/config/db_pass.txt";
	public static final String DB_REPORT_OUTPUT_DIR = "/home/scec-00/" + USER + "/db_reports/";

	//Constants
	public static final String DAX_FILENAME_POST = "_db_products";
	public static final String DB_PREFIX = "CS_Products_";
	public static final String DB_CHECK_OUTFILE_PREFIX = "DB_Check_Out_";
	public static final String CURVE_ERF_XML_FILE = OPENSHA_CONF_DIR + "MeanUCERF.xml"; 
	public static final String CURVE_CALC_PERIODS = "3,5,10";
	public static final String CURVE_OUTPUT_TYPES = "pdf,png";
	public static final double CURVE_DEFAULT_VS30 = 760;
	
	//DB info
	public static final String DB_SERVER = "focal";
	
	//Job names
	public static final String DB_INSERT_NAME = "Load_Amps";
	public static final String DB_CHECK_NAME = "Check_DB_Site";
	public static final String CURVE_CALC_NAME = "Curve_Calc";
	public static final String DB_REPORT_NAME = "DB_Report";
	public static final String SCATTER_MAP_NAME = "Scatter_Map";
    private final static String CYBERSHAKE_NOTIFY = "CyberShakeNotify";	
    private final static String DBWRITE_STAGE = "DBWrite";
    private final static String DISAGG_NAME = "Disaggregate";
	
	//Instance variables
	private RunIDQuery riq;
	private String zipFilesDir;
	private boolean transferZipFiles = true;
	private int numDAXes;
	private PP_DAXParameters params;
	
	//Job selection
	// boolean to enable/disable scatter map plotting
	private static final boolean DO_SCATTER_MAP = false;
	// boolean to enable/disable curve generation
	private static final boolean DO_CURVE_GEN = true;
	// boolean to enable/disable curve plotting. if DO_CURVE_GEN is false, then this is ignored.
	private static final boolean DO_CURVE_PLOT = true;
	
	public CyberShake_DB_DAXGen(RunIDQuery r) {
		riq = r;
		zipFilesDir = ".";
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, PP_DAXParameters params, String zipFilesDir, int numDAXes, boolean transferZip) {
		this(r);
		this.zipFilesDir = zipFilesDir;
		transferZipFiles = transferZip;
		this.numDAXes = numDAXes;
		this.params = params;
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, String zipFilesDir, int numDAXes, boolean highFreq, double highFreqCutoff, boolean transferZip) {
		this(r);
		this.zipFilesDir = zipFilesDir;
		transferZipFiles = transferZip;
		this.numDAXes = numDAXes;
		params = new PP_DAXParameters();
		params.setHighFrequency(highFreq);
		params.setHighFrequencyCutoff(highFreqCutoff);
	}
	
	
	public ADAG makeDAX() {
		String daxName = CyberShake_PP_DAXGen.DAX_FILENAME_PREFIX + riq.getSiteName() + DAX_FILENAME_POST;
		
		ADAG dax = new ADAG(daxName, 0, 1);
		
		Job zipPSAJob = null;
		
		//Start by adding zip job, if necessary
		if (!params.isZip()) {
			//Need to zip seis and PSA up on local site
			String id = "ZipSeis";
	    	Job zipSeisJob = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, "ZipSeis", "1.0");
	    	File zipSeisFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_seismograms.zip");
	    	zipSeisFile.setRegister(true);
	    	
	    	zipSeisJob.addArgument(".");
	    	zipSeisJob.addArgument(zipSeisFile.getName());
	    	zipSeisJob.uses(zipSeisFile, File.LINK.OUTPUT);

	    	dax.addJob(zipSeisJob);
	    	
	    	String id2 = "ZipPSA";
	    	zipPSAJob = new Job(id2, CyberShake_PP_DAXGen.NAMESPACE, "ZipPSA", "1.0");
	    	File zipPSAFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_PSA.zip");
	    	zipPSAFile.setRegister(true);

	    	zipPSAJob.addArgument(".");
	    	zipPSAJob.addArgument(zipPSAFile.getName());
	    	zipPSAJob.uses(zipPSAFile, File.LINK.OUTPUT);

	    	dax.addJob(zipPSAJob);
		}
		
		// Add workflow jobs
		Job insertJob = createDBInsertionJob();
		dax.addJob(insertJob);
		//If we needed to add local zip jobs, add dependency on PSA zip
		if (!params.isZip()) {
			dax.addDependency(zipPSAJob, insertJob);
		}
		
		Job dbCheckJob = createDBCheckJob();
		dax.addJob(dbCheckJob);
		
		Job curveCalcJob = null;
		Job disaggJob = null;
		if (DO_CURVE_GEN) {
			curveCalcJob = createCurveCalcJob();
			dax.addJob(curveCalcJob);
			disaggJob = createDisaggJob();
			dax.addJob(disaggJob);
		}
		Job reportJob = createDBReportJob();
		dax.addJob(reportJob);
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
		
		// curve calc is a child of check
		if (DO_CURVE_GEN) {
			dax.addDependency(dbCheckJob, curveCalcJob);
			dax.addDependency(curveCalcJob, disaggJob);
		}
		

		
		// report is a child of check
		dax.addDependency(dbCheckJob, reportJob);

		// scatter map is a child of curve calc
		if (DO_SCATTER_MAP) {
			dax.addDependency(curveCalcJob, scatterMapJob);
		    // notification is child of scatter map
			dax.addDependency(scatterMapJob, notifyJob);
		} else {
		    // notification is child of report
			dax.addDependency(reportJob, notifyJob);
		}

		return dax;
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
		
		job.addProfile("globus", "maxWallTime", "15");
		job.addProfile("hints","executionPool", "shock");
		
		return job;
	}

	private Job createCurveCalcJob() {
		String id = DB_PREFIX + "Curve_Calc" + "_" + riq.getSiteName();
		Job job = new Job(id, CyberShake_PP_DAXGen.NAMESPACE, CURVE_CALC_NAME, CyberShake_PP_DAXGen.VERSION);
		
		String outputDir = CURVE_OUTPUT_DIR_PREFIX + riq.getSiteName();
		
		job.addArgument("--site " + riq.getSiteName());
		job.addArgument("--run-id " + riq.getRunID());
		job.addArgument("--erf-file " + CURVE_ERF_XML_FILE);
		job.addArgument("--atten-rel-file " + CURVE_ATTEN_REL_XML_FILES);
		String periods = CURVE_CALC_PERIODS;
		if (params.isHighFrequency()) {
			periods += ",1,0.5,0.2,0.1";
		}
		job.addArgument("--period " + periods);
		job.addArgument("--output-dir " + outputDir);
		job.addArgument("--type " + CURVE_OUTPUT_TYPES);
		// this makes it calculate and the add the curve without prompting if needed
		job.addArgument("--force-add");
		
		// db password file
		job.addArgument("--password-file " + DB_PASS_FILE);
		job.addArgument("--vs30 " + CURVE_DEFAULT_VS30);
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
		
		job.addArgument(riq.getRunID() + "");
		
		String outFile = DB_CHECK_OUTFILE_DIR + DB_CHECK_OUTFILE_PREFIX + riq.getSiteName();
		
		job.addArgument(outFile);
		
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
		} else {
			File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() +  "_PSA.zip");
			zipFile.setRegister(false);
			job.uses(zipFile, File.LINK.INPUT);
		}
		
		job.addArgument("-server " + DB_SERVER);
		job.addArgument("-z");
		job.addArgument("-p " + zipFilesDir);
		job.addArgument("-run " + riq.getRunID());
		String periods = "10,5,3";
		if (params.isHighFrequency()) {
			periods = periods + ",2,1,0.5,0.2,0.1";
		}
		job.addArgument("-periods " + periods);
		
		job.addProfile("globus", "maxWallTime","90");
		job.addProfile("hints","executionPool", "shock");
		
		return job;
	}

	public static void main(String args[]) {
		if (args.length!=1 && args.length!=3) {
			System.out.println("USAGE: CyberShakeDBProductsDAXGen RUN_ID [ZIP_FILES_DIR NUM_DAXES]");
			System.exit(1);
		}
		int runID = Integer.parseInt(args[0]);
		CyberShake_DB_DAXGen gen;
		RunIDQuery rid = new RunIDQuery(runID, false);
		
		if (args.length == 3)
			gen = new CyberShake_DB_DAXGen(rid, args[1], Integer.parseInt(args[2]), false, 0.5, true);
		else
			gen = new CyberShake_DB_DAXGen(rid);
		
		gen.makeDAX();
	}
}


