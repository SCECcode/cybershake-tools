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
import edu.isi.pegasus.planner.dax.File.LINK;
import edu.isi.pegasus.planner.dax.File.TRANSFER;

public class CyberShake_AWP_SGT_DAXGen implements CyberShake_SGT {
	//Constants
    private final String DAX_FILENAME_PREFIX = "CyberShake_SGT";
    private final String DAX_FILENAME_EXTENSION = ".dax";
	private final String NAMESPACE = "scec";
	private final String VERSION = "1.0";
	
	private double CUTOFF_DISTANCE = 200;
	
	private double SPACING = 0.2;
	private int TIMESTEPS = 20000;
	private double DT = 0.01;
	private int TIME_SKIP = 10;
	private double MOMENT = 1.0e20;
	private double MAX_FREQ = 0.5;
	
	private RunIDQuery riq;

	public CyberShake_AWP_SGT_DAXGen(RunIDQuery r) {
		riq = r;
	}
	
	public ADAG makeDAX(String velModel) {
		ADAG workflowDAX = new ADAG(DAX_FILENAME_PREFIX + "_" + riq.getSiteName() + DAX_FILENAME_EXTENSION);
		// Workflow jobs
		Job updateStart = addUpdate("SGT_INIT", "SGT_START");
		workflowDAX.addJob(updateStart);
		Job preCVM = addPreAWPCVM();
		workflowDAX.addJob(preCVM);
		Job vMeshGen = addVMeshGen(velModel);
		workflowDAX.addJob(vMeshGen);
		Job preSGT = addPreAWPSGT();
		workflowDAX.addJob(preSGT);
		Job sgtGenX = addSGTGen("x");
		workflowDAX.addJob(sgtGenX);
		Job sgtGenY = addSGTGen("y");
		workflowDAX.addJob(sgtGenY);
		Job writeHeadX = addWriteHead("x");
		workflowDAX.addJob(writeHeadX);
		Job writeHeadY = addWriteHead("y");
		workflowDAX.addJob(writeHeadY);
		Job reformatX = addReformat("x");
		workflowDAX.addJob(reformatX);
		Job reformatY = addReformat("y");
		workflowDAX.addJob(reformatY);
		
		Job updateEnd = addUpdate("SGT_START", "SGT_END");
		workflowDAX.addJob(updateEnd);

		// Notification jobs
		// Replace with Pegasus notifications!
		Job notifypreCVM = addNotify("preCVM");
		workflowDAX.addJob(notifypreCVM);
		Job notifyvMeshGen = addNotify("vMeshGen");
		workflowDAX.addJob(notifyvMeshGen);
		Job notifysgtGenXY = addNotify("sgtGenXY");
		workflowDAX.addJob(notifysgtGenXY);
		
		// Workflow dependencies
		workflowDAX.addDependency(updateStart, preCVM);
		workflowDAX.addDependency(preCVM, vMeshGen);
		workflowDAX.addDependency(preCVM, preSGT);
		workflowDAX.addDependency(preSGT, sgtGenX);
		workflowDAX.addDependency(preCVM, sgtGenX);
		workflowDAX.addDependency(preSGT, sgtGenY);
		workflowDAX.addDependency(preCVM, sgtGenY);
		workflowDAX.addDependency(vMeshGen, sgtGenX);
		workflowDAX.addDependency(vMeshGen, sgtGenY);
		workflowDAX.addDependency(preSGT, writeHeadX);
		workflowDAX.addDependency(preSGT, writeHeadY);
		workflowDAX.addDependency(sgtGenX, reformatX);
		workflowDAX.addDependency(sgtGenY, reformatY);
		workflowDAX.addDependency(reformatX, updateEnd);
		workflowDAX.addDependency(reformatY, updateEnd);

		// Notification dependencies
		workflowDAX.addDependency(preCVM, notifypreCVM);
		workflowDAX.addDependency(vMeshGen, notifyvMeshGen);
		workflowDAX.addDependency(sgtGenX, notifysgtGenXY);
		workflowDAX.addDependency(sgtGenY, notifysgtGenXY);
				
		return workflowDAX;
	}

	private Job addUpdate(String from_state, String to_state) {
		String id = "UpdateRun_" + to_state + "_" + riq.getSiteName();
		Job updateJob = new Job(id, NAMESPACE, "UpdateRun", VERSION);

		updateJob.addArgument(riq.getRunID() + "");
		updateJob.addArgument(from_state);
		updateJob.addArgument(to_state);
		
		updateJob.addProfile("globus", "maxWallTime","5");
		updateJob.addProfile("hints","executionPool", "shock");
	
		return updateJob;
	}
   
	private Job addPreAWPCVM() {
		String id = "PreAWPCVM_" + riq.getSiteName();
		Job preCVMJob = new Job(id, NAMESPACE, "PreAWPCVM", VERSION);
		
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
		String id = "AWPMeshGen_" + riq.getSiteName();
		Job vMeshGenJob = new Job(id, NAMESPACE, "AWPMeshGen", VERSION);
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		File mediaFile = new File("awp." + riq.getSiteName() + ".media");
		
		vMeshGenJob.addArgument(riq.getSiteName());
		vMeshGenJob.addArgument(gridoutFile);		
		vMeshGenJob.addArgument(coordFile);
		vMeshGenJob.addArgument(mediaFile);
		
		if (velModel.equals("-v4")) {
			vMeshGenJob.addArgument("cvms");
		} else if (velModel.equals("-vh")) {
			vMeshGenJob.addArgument("cvmh");			
		} else {
			System.out.println(velModel + " is an invalid velocity model option, exiting.");
			System.exit(2);
		}
		
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
		
		vMeshGenJob.uses(gridoutFile, File.LINK.INPUT);
		vMeshGenJob.uses(coordFile, File.LINK.INPUT);
		vMeshGenJob.uses(mediaFile, File.LINK.OUTPUT);
		
		return vMeshGenJob;
	}
	
	private Job addPreAWPSGT() {
		String id = "PreAWPSGT_" + riq.getSiteName();
		Job preSGTJob = new Job(id, NAMESPACE, "PreAWPSGT", VERSION);
		
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File faultlistFile = new File(riq.getSiteName() + ".faultlist");
		File radiusFile = new File(riq.getSiteName() + ".radiusfile");
		File sgtcordFile = new File(riq.getSiteName() + ".cordfile");
		File awpcordFile = new File("awp." + riq.getSiteName() + ".cordfile");
		
		fdlocFile.setTransfer(File.TRANSFER.FALSE);
		faultlistFile.setTransfer(File.TRANSFER.FALSE);
		radiusFile.setTransfer(File.TRANSFER.FALSE);
		sgtcordFile.setTransfer(File.TRANSFER.FALSE);
		awpcordFile.setTransfer(File.TRANSFER.FALSE);
		
		fdlocFile.setRegister(false);
		faultlistFile.setRegister(false);
		radiusFile.setRegister(false);
		sgtcordFile.setRegister(false);
		awpcordFile.setRegister(false);
		
		preSGTJob.addArgument(riq.getSiteName());
		preSGTJob.addArgument(riq.getErfID() + "");
		preSGTJob.addArgument(modelboxFile);
		preSGTJob.addArgument(gridoutFile);
		preSGTJob.addArgument(coordFile);
		preSGTJob.addArgument(fdlocFile);
		preSGTJob.addArgument(faultlistFile);
		preSGTJob.addArgument(radiusFile);
		preSGTJob.addArgument(sgtcordFile);
		preSGTJob.addArgument(awpcordFile);
		
		preSGTJob.uses(modelboxFile, File.LINK.INPUT);
		preSGTJob.uses(gridoutFile, File.LINK.INPUT);
		preSGTJob.uses(coordFile, File.LINK.INPUT);
		preSGTJob.uses(fdlocFile, File.LINK.OUTPUT);
		preSGTJob.uses(faultlistFile, File.LINK.OUTPUT);
		preSGTJob.uses(radiusFile, File.LINK.OUTPUT);
		preSGTJob.uses(sgtcordFile, File.LINK.OUTPUT);
		preSGTJob.uses(awpcordFile, File.LINK.OUTPUT);
		
		return preSGTJob;
	}
	
	private Job addSGTGen(String component) {
		String id = "AWP_SGTGen_" + component + "_" + riq.getSiteName();
		Job sgtGenJob = new Job(id, NAMESPACE, "AWP_SGTGen", VERSION);
		
		File sourceFile = new File("f" + component + "_src");
		File cordFile = new File("awp." + riq.getSiteName() + ".cordfile");
		
		sgtGenJob.addArgument(riq.getSiteName());
		
		sgtGenJob.addArgument(sourceFile);
		sgtGenJob.addArgument(cordFile);
		sgtGenJob.addArgument(component);
		
		sgtGenJob.uses(sourceFile, File.LINK.INPUT);
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

	
   private Job addWriteHead(String component) {
	   String id = "WriteHead_" + component + "_" + riq.getSiteName();
	   Job writeHeadJob = new Job(id, NAMESPACE, "WriteHead", VERSION);

	   File modelboxFile = new File(riq.getSiteName() + ".modelbox");
	   File cordFile = new File(riq.getSiteName() + ".cordfile");
	   File fdlocFile = new File(riq.getSiteName() + ".fdloc");
	   File gridoutFile = new File("gridout_" + riq.getSiteName());
	   File velocityFile = new File("awp." + riq.getSiteName() + ".media");
	   File headerFile = new File(riq.getSiteName() + "_f" + component + "_" + riq.getRunID() + ".sgthead");
	   
	   writeHeadJob.addArgument(modelboxFile.getName());
	   writeHeadJob.addArgument(cordFile.getName());
	   writeHeadJob.addArgument(fdlocFile.getName());
	   writeHeadJob.addArgument(gridoutFile.getName());
	   writeHeadJob.addArgument("" + SPACING);
	   writeHeadJob.addArgument("" + TIMESTEPS);
	   writeHeadJob.addArgument("" + DT);
	   writeHeadJob.addArgument("" + TIME_SKIP);
	   writeHeadJob.addArgument(component);
	   writeHeadJob.addArgument("" + MOMENT);
	   writeHeadJob.addArgument("" + MAX_FREQ);
	   writeHeadJob.addArgument(velocityFile.getName());
	   writeHeadJob.addArgument(headerFile.getName());
	   
	   headerFile.setRegister(true);
	   headerFile.setTransfer(File.TRANSFER.TRUE);
	   
	   writeHeadJob.uses(modelboxFile, File.LINK.INPUT);
	   writeHeadJob.uses(cordFile, File.LINK.INPUT);
	   writeHeadJob.uses(fdlocFile, File.LINK.INPUT);
	   writeHeadJob.uses(gridoutFile, File.LINK.INPUT);
	   writeHeadJob.uses(velocityFile, File.LINK.INPUT);
	   writeHeadJob.uses(headerFile, File.LINK.OUTPUT);
	   
	   return writeHeadJob;
	}
	

    private Job addReformat(String component) {
    	String id = "ReformatAWP_" + component + "_" + riq.getSiteName();
    	Job reformatJob = new Job(id, NAMESPACE, "ReformatAWP", VERSION);
    	
    	File sgt = new File(riq.getSiteName() + "_AWP_f" + component + "_" + riq.getRunID() + ".sgt");
    	File sgt_out = new File(riq.getSiteName() + "_f" + component + "_" + riq.getRunID() + ".sgt");
    	
    	reformatJob.addArgument(sgt.getName());
    	int timestepsInSGT = TIMESTEPS/TIME_SKIP;
    	reformatJob.addArgument("" + timestepsInSGT);
    	reformatJob.addArgument(sgt_out.getName());
    	
    	sgt_out.setRegister(true);
    	sgt_out.setTransfer(TRANSFER.TRUE);
    	
    	reformatJob.uses(sgt, LINK.INPUT);
    	reformatJob.uses(sgt_out, LINK.OUTPUT);
    	
    	return reformatJob;
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
