package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.DAX;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;

public class CyberShake_SGT_DAXGen implements CyberShake_SGT {
	//Constants
    private final static String DAX_FILENAME_PREFIX = "CyberShake_SGT";
    private final static String DAX_FILENAME_EXTENSION = ".dax";
	private final static String NAMESPACE = "scec";
	private final static String VERSION = "1.0";
	
	private static double CUTOFF_DISTANCE = 200;
	
	private RunIDQuery riq;
	
	public static void main(String[] args) {
        Options cmd_opts = new Options();
        Option cvms = new Option("v4", "Use CVM-S4 velocity model");
        Option cvmh = new Option("vh", "use CVM-H velocity model");
        OptionGroup vModelGroup = new OptionGroup();
        vModelGroup.addOption(cvms);
        vModelGroup.addOption(cvmh);
        cmd_opts.addOptionGroup(vModelGroup);
        Option awp = new Option("awp", "Use AWP-ODC-SGT to generate the SGTs");
        cmd_opts.addOption(awp);
        Option runIDFile = OptionBuilder.withArgName("runID_file").hasArg().withDescription("File containing list of Run IDs to use.").create("f");
        Option runIDList = OptionBuilder.withArgName("runID_list").hasArgs().withDescription("List of Run IDs to use.").create("r");
        OptionGroup runIDGroup = new OptionGroup();
        runIDGroup.setRequired(true);
        runIDGroup.addOption(runIDFile);
        runIDGroup.addOption(runIDList);
        cmd_opts.addOptionGroup(runIDGroup);
        
        String usageString = "Usage: CyberShake_SGT_DAXGen <output filename> <destination directory> [options] [-f <runID file, one per line> | -r <runID1> <runID2> ... ]";
        CommandLineParser parser = new GnuParser();
        if (args.length<4) {
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
        
		String outputFilename = args[0]; 
		String directory = args[1];
		String velocityModel = "1D";
		if (line.hasOption(cvms.getOpt())) {
			velocityModel = "cvms";
		} else if (line.hasOption(cvmh.getOpt())) {
			velocityModel = "cvmh";
		}		
		
		boolean awp_bool = false;
		if (line.hasOption(awp.getOpt())) {
			awp_bool = true;
		}
		
		ArrayList<RunIDQuery> runIDQueries = null;
		
		if (line.hasOption(runIDFile.getOpt())) {
			runIDQueries = runIDsFromFile(awp, line.getOptionValue(runIDFile.getOpt()));
		} else {		
			runIDQueries = runIDsFromArgs(awp, line.getOptionValues(runIDList.getOpt()));
		}
				
		ADAG[] sgtDAXes = makeWorkflows(runIDQueries, velocityModel, awp_bool, directory, outputFilename);
		
		if (sgtDAXes.length>1) {
			//Create a top-level DAX
			long timeStamp = System.currentTimeMillis();
			ADAG topLevelDAX = new ADAG(DAX_FILENAME_PREFIX + "_" + timeStamp + DAX_FILENAME_EXTENSION);
			for (int i=0; i<runIDQueries.size(); i++) {
				String daxFileName = DAX_FILENAME_PREFIX + "_" + runIDQueries.get(i).getSiteName() + "_" + i + ".dax";
				sgtDAXes[i].writeToFile(daxFileName);
				DAX sgtDaxJob = new DAX("SGT_" + runIDQueries.get(i).getSiteName(), daxFileName);
				//Avoid pruning of jobs
				sgtDaxJob.addArgument("--force");
				//Copy results to ranger unpurged directory
				sgtDaxJob.addArgument("-o ranger");
				topLevelDAX.addDAX(sgtDaxJob);
			
				File sgtDaxFile = new File(daxFileName);
				sgtDaxFile.addPhysicalFile("file://" + directory + "/" + daxFileName, "local");
				topLevelDAX.addFile(sgtDaxFile);
			}
			topLevelDAX.writeToFile(outputFilename);
		} else {
			sgtDAXes[0].writeToFile(outputFilename);
		}
	} 

	public static ArrayList<RunIDQuery> runIDsFromFile(Option awp, String inputFile) {
		ArrayList<RunIDQuery> runIDQueries = new ArrayList<RunIDQuery>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String entry = br.readLine();
			while (entry!=null) {
				runIDQueries.add(new RunIDQuery(Integer.parseInt(entry), false));
			}
			br.close();
		} catch (IOException iex) {
			iex.printStackTrace();
			System.exit(1);
		}
		
		return runIDQueries;
	}
	
	public static ArrayList<RunIDQuery> runIDsFromArgs(Option awp, String[] runIDs) {
		ArrayList<RunIDQuery> runIDQueries = new ArrayList<RunIDQuery>();
		
		for (String runID: runIDs) {
			runIDQueries.add(new RunIDQuery(Integer.parseInt(runID), false));
		}
		return runIDQueries;
	}
		
	public static ADAG[] makeWorkflows(ArrayList<RunIDQuery> runIDQueries, String velocityModel, boolean awp, String directory, String outputFilename) {
		//Create a DAX for each site
		ADAG[] daxes = new ADAG[runIDQueries.size()];
		
		for (int i=0; i<runIDQueries.size(); i++) {
			CyberShake_SGT sd;
			if (awp) {
				sd = new CyberShake_AWP_SGT_DAXGen(runIDQueries.get(i));	
			} else {
				sd = new CyberShake_SGT_DAXGen(runIDQueries.get(i));	
			}
			
			daxes[i] = sd.makeDAX(velocityModel);
		}
		return daxes;	
	}

	
	public CyberShake_SGT_DAXGen(RunIDQuery r) {
		riq = r;
	}
	
	public ADAG makeDAX(String velModel) {
		ADAG workflowDAX = new ADAG(DAX_FILENAME_PREFIX + "_" + riq.getSiteName() + DAX_FILENAME_EXTENSION);
		// Workflow jobs
		Job updateStart = addUpdate("SGT_INIT", "SGT_START");
		workflowDAX.addJob(updateStart);
		Job preCVM = addPreCVM();
		workflowDAX.addJob(preCVM);
		Job vMeshGen = addVMeshGen(velModel);
		workflowDAX.addJob(vMeshGen);
		Job vMeshMerge = addVMeshMerge(velModel);
		workflowDAX.addJob(vMeshMerge);
		Job preSGT = addPreSGT();
		workflowDAX.addJob(preSGT);
		Job sgtGenX = addSGTGen("x");
		workflowDAX.addJob(sgtGenX);
		Job sgtGenY = addSGTGen("y");
		workflowDAX.addJob(sgtGenY);
//		String nanTestX = addNanTest("x");
//		String nanTestY = addNanTest("y");
//		String sgtMergeX = addSGTMerge("x");
//		String sgtMergeY = addSGTMerge("y");
		//Updated to have merged nantest job
		Job nanTest = addNanTest();
		workflowDAX.addJob(nanTest);
		//Updated to have merged sgt merge job
		Job sgtMerge = addSGTMerge();
		workflowDAX.addJob(sgtMerge);
		
		Job updateEnd = addUpdate("SGT_START", "SGT_END");
		workflowDAX.addJob(updateEnd);

		// Notification jobs
		Job notifypreCVM = addNotify("preCVM");
		workflowDAX.addJob(notifypreCVM);
		Job notifyvMeshGen = addNotify("vMeshGen");
		workflowDAX.addJob(notifyvMeshGen);
		Job notifyvMeshMerge = addNotify("vMeshMerge");
		workflowDAX.addJob(notifyvMeshMerge);
		Job notifysgtGenXY = addNotify("sgtGenXY");
		workflowDAX.addJob(notifysgtGenXY);
		Job notifysgtMergeXY = addNotify("sgtMergeXY");
		workflowDAX.addJob(notifysgtMergeXY);
		
		// Workflow dependencies
		workflowDAX.addDependency(updateStart, preCVM);
		workflowDAX.addDependency(preCVM, vMeshGen);
		workflowDAX.addDependency(vMeshGen, vMeshMerge);
		workflowDAX.addDependency(preCVM, vMeshMerge);
		workflowDAX.addDependency(preCVM, preSGT);
		workflowDAX.addDependency(preSGT, sgtGenX);
		workflowDAX.addDependency(preCVM, sgtGenX);
		workflowDAX.addDependency(preSGT, sgtGenY);
		workflowDAX.addDependency(preCVM, sgtGenY);
		workflowDAX.addDependency(vMeshMerge, sgtGenX);
		workflowDAX.addDependency(vMeshMerge, sgtGenY);
		
		//Updated to have merged nantest and merged sgt merge jobs
		workflowDAX.addDependency(sgtGenX, nanTest);
		workflowDAX.addDependency(sgtGenY, nanTest);
		workflowDAX.addDependency(sgtGenX, sgtMerge);
		workflowDAX.addDependency(sgtGenY, sgtMerge);
		workflowDAX.addDependency(nanTest, updateEnd);
		workflowDAX.addDependency(sgtMerge, updateEnd);

		// Notification dependencies
		workflowDAX.addDependency(preCVM, notifypreCVM);
		workflowDAX.addDependency(vMeshGen, notifyvMeshGen);
		workflowDAX.addDependency(vMeshMerge, notifyvMeshMerge);
		workflowDAX.addDependency(preSGT, notifyvMeshMerge);
		workflowDAX.addDependency(sgtGenX, notifysgtGenXY);
		workflowDAX.addDependency(sgtGenY, notifysgtGenXY);
		
		//Updated to have merged nantest and merged sgt merge jobs
		workflowDAX.addDependency(sgtMerge, notifysgtGenXY);
		workflowDAX.addDependency(nanTest, notifysgtGenXY);
		
		return workflowDAX;
	}
	
   private Job addUpdate(String from_state, String to_state) {
		String id = "UpdateRun_" + to_state + "_" + riq.getSiteName();
		Job updateJob = new Job(id, NAMESPACE, "UpdateRun", VERSION);

		updateJob.addArgument(riq.getRunID() + "");
		updateJob.addArgument(from_state);
		updateJob.addArgument(to_state);
		
		updateJob.addProfile("globus", "maxWallTime","5");
		updateJob.addProfile("hints","executionPool", "local");
	
		return updateJob;
	}
   
	private Job addPreCVM() {
		String id = "PreCVM_" + riq.getSiteName();
		Job preCVMJob = new Job(id, NAMESPACE, "PreCVM", VERSION);
		
		preCVMJob.addArgument(riq.getSiteName());
		preCVMJob.addArgument(riq.getErfID() + "");
		
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File gridfileFile = new File("gridfile_" + riq.getSiteName());
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		File paramFile = new File("model_params_GC_" + riq.getSiteName());
		File boundsFile = new File("model_bounds_GC_" + riq.getSiteName());
		
		modelboxFile.setTransfer(File.TRANSFER.FALSE);
		gridfileFile.setTransfer(File.TRANSFER.FALSE);
		gridoutFile.setTransfer(File.TRANSFER.FALSE);
		coordFile.setTransfer(File.TRANSFER.FALSE);
		paramFile.setTransfer(File.TRANSFER.FALSE);
		boundsFile.setTransfer(File.TRANSFER.FALSE);
		
		modelboxFile.setRegister(false);
		gridfileFile.setRegister(false);
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
		paramFile.setRegister(false);
		boundsFile.setRegister(false);
		
		preCVMJob.addArgument(modelboxFile);
		preCVMJob.addArgument(gridfileFile);
		preCVMJob.addArgument(gridoutFile);
		preCVMJob.addArgument(coordFile);
		preCVMJob.addArgument(paramFile);
		preCVMJob.addArgument(boundsFile);
		
		preCVMJob.uses(modelboxFile, File.LINK.OUTPUT);
		preCVMJob.uses(gridfileFile, File.LINK.OUTPUT);
		preCVMJob.uses(gridoutFile, File.LINK.OUTPUT);
		preCVMJob.uses(coordFile, File.LINK.OUTPUT);
		preCVMJob.uses(paramFile, File.LINK.OUTPUT);
		preCVMJob.uses(boundsFile, File.LINK.OUTPUT);
		
		return preCVMJob;
	}
	
	private Job addVMeshGen(String velModel) {
		System.err.println("here");
		
		String id = "UCVMMeshGen_" + riq.getSiteName();
		Job vMeshGenJob = new Job(id, NAMESPACE, "UCVMMeshGen", VERSION);
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		
		vMeshGenJob.addArgument(riq.getSiteName());
		vMeshGenJob.addArgument(gridoutFile);		
		vMeshGenJob.addArgument(coordFile);
		
		
		if (velModel.equals("cvms")) {
			vMeshGenJob.addArgument("cvms");
		} else if (velModel.equals("cvmh")) {
			vMeshGenJob.addArgument("cvmh");
		} else {
			System.out.println(velModel + " is an invalid velocity model option, exiting.");
			System.exit(2);
		}
		
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
		
		vMeshGenJob.uses(gridoutFile, File.LINK.INPUT);
		vMeshGenJob.uses(coordFile, File.LINK.INPUT);
		
		/*String id = "VMeshGen_" + riq.getSiteName();
		Job vMeshGenJob = null;
		if (velModel.equals("-v4")) {
			vMeshGenJob = new Job(id, NAMESPACE, "V4MeshGen", VERSION);
		} else if (velModel.equals("-vh")) {
			vMeshGenJob = new Job(id, NAMESPACE, "VHMeshGen", VERSION);
		} else {
			System.out.println(velModel + " is an invalid velocity model option, exiting.");
			System.exit(2);
		}
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		
		vMeshGenJob.addArgument(riq.getSiteName());
		vMeshGenJob.addArgument(gridoutFile);		
		vMeshGenJob.addArgument(coordFile);
		
		vMeshGenJob.uses(gridoutFile, File.LINK.INPUT);
		vMeshGenJob.uses(coordFile, File.LINK.INPUT);*/
		
		return vMeshGenJob;
	}
	
	private Job addVMeshMerge(String velModel) {
		String id = "VMeshMerge_" + riq.getSiteName();
		Job vMeshMergeJob = new Job(id, NAMESPACE, "UCVMMeshMerge", VERSION);
				
		File gridfileFile = new File("gridfile_" + riq.getSiteName());		
		File pFile = new File("v_sgt-" + riq.getSiteName() + ".p");
		File sFile = new File("v_sgt-" + riq.getSiteName() + ".s");
		File dFile = new File("v_sgt-" + riq.getSiteName() + ".d");
		
		pFile.setTransfer(File.TRANSFER.FALSE);
		sFile.setTransfer(File.TRANSFER.FALSE);
		dFile.setTransfer(File.TRANSFER.FALSE);
		
		pFile.setRegister(false);
		sFile.setRegister(false);
		dFile.setRegister(false);
		
		vMeshMergeJob.addArgument(riq.getSiteName());
		vMeshMergeJob.addArgument(gridfileFile);
		vMeshMergeJob.addArgument(pFile);
		vMeshMergeJob.addArgument(sFile);
		vMeshMergeJob.addArgument(dFile);
		
		vMeshMergeJob.uses(gridfileFile, File.LINK.INPUT);
		vMeshMergeJob.uses(pFile, File.LINK.OUTPUT);
		vMeshMergeJob.uses(sFile, File.LINK.OUTPUT);
		vMeshMergeJob.uses(dFile, File.LINK.OUTPUT);

		vMeshMergeJob.addProfile("globus", "maxWallTime", "120");
		vMeshMergeJob.addProfile("globus", "host_count", "4");
		vMeshMergeJob.addProfile("globus", "count", "8");
		
		return vMeshMergeJob;
	}
	
	private Job addPreSGT() {
		String id = "PreSGT_" + riq.getSiteName();
		Job preSGTJob = new Job(id, NAMESPACE, "PreSGT", VERSION);
		
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File faultlistFile = new File(riq.getSiteName() + ".faultlist");
		File radiusFile = new File(riq.getSiteName() + ".radiusfile");
		File sgtcordFile = new File(riq.getSiteName() + ".cordfile");
		
		fdlocFile.setTransfer(File.TRANSFER.FALSE);
		faultlistFile.setTransfer(File.TRANSFER.FALSE);
		radiusFile.setTransfer(File.TRANSFER.FALSE);
		sgtcordFile.setTransfer(File.TRANSFER.FALSE);
		
		fdlocFile.setRegister(false);
		faultlistFile.setRegister(false);
		radiusFile.setRegister(false);
		sgtcordFile.setRegister(false);
		
		preSGTJob.addArgument(riq.getSiteName());
		preSGTJob.addArgument(riq.getErfID() + "");
		preSGTJob.addArgument(modelboxFile);
		preSGTJob.addArgument(gridoutFile);
		preSGTJob.addArgument(coordFile);
		preSGTJob.addArgument(fdlocFile);
		preSGTJob.addArgument(faultlistFile);
		preSGTJob.addArgument(radiusFile);
		preSGTJob.addArgument(sgtcordFile);
		
		preSGTJob.uses(modelboxFile, File.LINK.INPUT);
		preSGTJob.uses(gridoutFile, File.LINK.INPUT);
		preSGTJob.uses(coordFile, File.LINK.INPUT);
		preSGTJob.uses(fdlocFile, File.LINK.OUTPUT);
		preSGTJob.uses(faultlistFile, File.LINK.OUTPUT);
		preSGTJob.uses(radiusFile, File.LINK.OUTPUT);
		preSGTJob.uses(sgtcordFile, File.LINK.OUTPUT);
		
		return preSGTJob;
	}
	
	private Job addSGTGen(String component) {
		String id = "SGTGen_" + component + "_" + riq.getSiteName();
		Job sgtGenJob = new Job(id, NAMESPACE, "SGTGen", VERSION);
		
		if (riq.getSgtVarID()==7) {
			//Use V3.0.3 
			sgtGenJob = new Job(id, NAMESPACE, "SGTGen", "3.0.3");
		}
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File cordFile = new File(riq.getSiteName() + ".cordfile");
		
		sgtGenJob.addArgument(riq.getSiteName());
		
		sgtGenJob.addArgument(gridoutFile);
		sgtGenJob.addArgument(modelboxFile);
		sgtGenJob.addArgument(fdlocFile);
		sgtGenJob.addArgument(cordFile);
		sgtGenJob.addArgument(component);
		
		sgtGenJob.uses(gridoutFile, File.LINK.INPUT);
		sgtGenJob.uses(modelboxFile, File.LINK.INPUT);
		sgtGenJob.uses(fdlocFile, File.LINK.INPUT);
		sgtGenJob.uses(cordFile, File.LINK.INPUT);
		
		if (riq.getSiteName().equals("TEST")) {
			CUTOFF_DISTANCE = 20;
		}
		
		if (CUTOFF_DISTANCE <= 20) {
			System.out.println("NOTE: Using cutoff distance=20 SGT params");
			sgtGenJob.addProfile("globus", "maxWallTime", "360");
			sgtGenJob.addProfile("globus", "host_count", "60");
			sgtGenJob.addProfile("globus", "count", "240");
		} else {
			sgtGenJob.addProfile("globus", "maxWallTime", "1200");
			sgtGenJob.addProfile("globus", "host_count", "25");
			sgtGenJob.addProfile("globus", "count", "400");
		}
		
		return sgtGenJob;
	}
	
	private Job addNanTest() {
		String id = "NanTest_" + riq.getSiteName();
		Job nanTestJob = new Job(id, NAMESPACE, "NanTest", VERSION);
		
		String xPrefix = riq.getSiteName() + "-fx_sgt-";
		String yPrefix = riq.getSiteName() + "-fy_sgt-";

		int numProcessors = 400;
		
		if (CUTOFF_DISTANCE==20) {
			numProcessors = 240;
		}
		
		nanTestJob.addArgument(xPrefix);
		nanTestJob.addArgument(yPrefix);
		nanTestJob.addArgument(numProcessors + "");
		
		return nanTestJob;
	}
	
	private Job addNanTest(String component) {
		String id = "NanTest_" + component + "_" + riq.getSiteName();
		Job nanTestJob = new Job(id, NAMESPACE, "NanTest", VERSION);
		
		String prefix = riq.getSiteName() + "-f" + component + "_sgt-";
		int numProcessors = 400;
		
		if (CUTOFF_DISTANCE==20) {
			numProcessors = 240;
		}
		
		nanTestJob.addArgument(prefix);
		nanTestJob.addArgument(numProcessors + "");
		
		return nanTestJob;
	}
	
	private Job addSGTMerge() {
		String id = "SGTMerge_" + riq.getSiteName();
		Job sgtMergeJob = new Job(id, NAMESPACE, "SGTMerge", VERSION);
		
		File sgtFileX = new File(riq.getSiteName() + "_fx_" + riq.getRunID() + ".sgt");
		File md5FileX = new File(sgtFileX.getName() + ".md5");
		File sgtFileY = new File(riq.getSiteName() + "_fy_" + riq.getRunID() + ".sgt");
		File md5FileY = new File(sgtFileY.getName() + ".md5");
		
		//sgtFilename.setTransfer(LFN.XFER_NOT);
		
		// Stage SGT and md5 files to storage directory, and register in RLS
		sgtFileX.setTransfer(File.TRANSFER.TRUE);
		sgtFileX.setRegister(true);
		md5FileX.setTransfer(File.TRANSFER.TRUE);
		md5FileX.setRegister(true);
		sgtFileY.setTransfer(File.TRANSFER.TRUE);
		sgtFileY.setRegister(true);
		md5FileY.setTransfer(File.TRANSFER.TRUE);
		md5FileY.setRegister(true);
		
		sgtMergeJob.addArgument(riq.getSiteName());
		sgtMergeJob.addArgument(sgtFileX);
		sgtMergeJob.addArgument(sgtFileY);
		
		sgtMergeJob.uses(sgtFileX, File.LINK.OUTPUT);
		sgtMergeJob.uses(md5FileX, File.LINK.OUTPUT);
		sgtMergeJob.uses(sgtFileY, File.LINK.OUTPUT);
		sgtMergeJob.uses(md5FileY, File.LINK.OUTPUT);
		
		sgtMergeJob.addProfile("globus", "maxWallTime", "180");
		
		return sgtMergeJob;
	}
	
	private Job addSGTMerge(String component) {
		String id = "SGTMerge_" + component + "_" + riq.getSiteName();
		Job sgtMergeJob = new Job(id, NAMESPACE, "SGTMerge", VERSION);
		
		File sgtFile = new File(riq.getSiteName() + "_f" + component + "_" + riq.getRunID() + ".sgt");
		File md5File = new File(sgtFile.getName() + ".md5");
		
		// Stage SGT and md5 files to storage directory, and register in RLS
		sgtFile.setTransfer(File.TRANSFER.TRUE);
		sgtFile.setRegister(true);
		md5File.setTransfer(File.TRANSFER.TRUE);
		md5File.setRegister(true);       
				
		sgtMergeJob.addArgument(riq.getSiteName());
		sgtMergeJob.addArgument(sgtFile);
		sgtMergeJob.addArgument(component);
		
		sgtMergeJob.uses(sgtFile, File.LINK.OUTPUT);
		sgtMergeJob.uses(md5File, File.LINK.OUTPUT);
		
		sgtMergeJob.addProfile("globus", "maxWallTime", "180");
		
		return sgtMergeJob;
	}
	
	private Job addNotify(String stage) {
		String id = "CyberShakeNotify_" + stage + "_" + riq.getSiteName();
		Job notifyJob = new Job(id, NAMESPACE, "CyberShakeNotify", VERSION);
		
		notifyJob.addArgument(riq.getRunID() + "");
		notifyJob.addArgument("SGT");		
		notifyJob.addArgument(stage);
		
		notifyJob.addProfile("globus", "maxWallTime", "5");
		
		return notifyJob;
	}
}
