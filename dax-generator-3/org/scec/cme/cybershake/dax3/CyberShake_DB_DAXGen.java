package org.scec.cme.cybershake.dax3;

import java.io.FileWriter;
import java.io.IOException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;

public class CyberShake_DB_DAXGen {
	//Paths
	public static final String DB_CHECK_OUTFILE_DIR = "/home/scec-02/tera3d/CyberShake2007/cybershk/pegasus/db_check_outfiles/";
	public static final String CURVE_OUTPUT_DIR_PREFIX = "/home/scec-00/cybershk/opensha/curves/";
	public static final String OPENSHA_CONF_DIR = "/home/scec-00/cybershk/opensha/OpenSHA/org/opensha/sha/cybershake/conf/";
	public static final String CURVE_ATTEN_REL_XML_FILES = OPENSHA_CONF_DIR + "cb2008.xml"
			+ "," + OPENSHA_CONF_DIR + "ba2008.xml"
			+ "," + OPENSHA_CONF_DIR + "cy2008.xml"
			+ "," + OPENSHA_CONF_DIR + "as2008.xml";
	public static final String DB_PASS_FILE = "/home/scec-00/cybershk/config/db_pass.txt";
	public static final String DB_REPORT_OUTPUT_DIR = "/home/scec-00/cybershk/db_reports/";

	//Constants
	public static final String DAX_FILENAME_POST = "_db_products";
	public static final String DB_PREFIX = "CS_Products_";
	public static final String DB_CHECK_OUTFILE_PREFIX = "DB_Check_Out_";
	public static final String CURVE_ERF_XML_FILE = OPENSHA_CONF_DIR + "MeanUCERF.xml"; 
	public static final String CURVE_CALC_PERIODS = "2,3,5,10";
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
	
	//Instance variables
	private RunIDQuery riq;
	private String zipFilesDir;
	private boolean transferZipFiles = true;
	private int numDAXes;
	
	//Job selection
	// boolean to enable/disable scatter map plotting
	private static final boolean DO_SCATTER_MAP = false;
	// boolean to enable/disable curve generation
	private static final boolean DO_CURVE_GEN = false;
	// boolean to enable/disable curve plotting. if DO_CURVE_GEN is false, then this is ignored.
	private static final boolean DO_CURVE_PLOT = false;
	
	public CyberShake_DB_DAXGen(RunIDQuery r) {
		riq = r;
		zipFilesDir = ".";
	}
	
	public CyberShake_DB_DAXGen(RunIDQuery r, String zipFilesDir, int numDAXes) {
		this(r);
		this.zipFilesDir = zipFilesDir;
		transferZipFiles = false;
		this.numDAXes = numDAXes;
	}
	
	public ADAG makeDAX() {
		String daxName = CyberShake_PP_DAXGen.DAX_FILENAME_PREFIX + riq.getSiteName() + DAX_FILENAME_POST;
		
		ADAG dax = new ADAG(daxName, 0, 1);
		
		// Add workflow jobs
		Job insertJob = createDBInsertionJob();
		dax.addJob(insertJob);
		Job dbCheckJob = createDBCheckJob();
		dax.addJob(dbCheckJob);
		
		Job curveCalcJob = null;
		if (DO_CURVE_GEN) {
			curveCalcJob = createCurveCalcJob();
			dax.addJob(curveCalcJob);
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
		job.addProfile("hints","executionPool", "local");
		
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
		job.addArgument("--period " + CURVE_CALC_PERIODS);
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
			for (int i=1; i<numDAXes; i++) {
				File zipFile = new File("CyberShake_" + riq.getSiteName() + "_" + riq.getRunID() + "_" + i + "_PSA.zip");
				job.uses(zipFile, File.LINK.INPUT);
			}
		}
		
		job.addArgument("-server " + DB_SERVER);
		job.addArgument("-z");
		job.addArgument("-p " + zipFilesDir);
		job.addArgument("-run " + riq.getRunID());
		
		job.addProfile("globus", "maxWallTime","90");
		job.addProfile("hints","executionPool", "local");
		
		return job;
	}

	public static void main(String args[]) {
		if (args.length!=1 && args.length!=3) {
			System.out.println("USAGE: CyberShakeDBProductsDAXGen RUN_ID [ZIP_FILES_DIR NUM_DAXES]");
			System.exit(1);
		}
		int runID = Integer.parseInt(args[0]);
		CyberShake_DB_DAXGen gen;
		RunIDQuery rid = new RunIDQuery(runID);
		
		if (args.length == 3)
			gen = new CyberShake_DB_DAXGen(rid, args[1], Integer.parseInt(args[2]));
		else
			gen = new CyberShake_DB_DAXGen(rid);
		
		gen.makeDAX();
	}
}


