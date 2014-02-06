package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.FileReader;

import edu.isi.pegasus.planner.dax.ADAG;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;
import edu.isi.pegasus.planner.dax.File.LINK;
import edu.isi.pegasus.planner.dax.File.TRANSFER;


public class CyberShake_AWP_SGT_DAXGen {
	//Called as part of workflow to handle custom number of cores for each AWP SGT job
	private static RunIDQuery riq = null;
	
	public static void main(String[] args) {
		if (args.length<3) {
			System.out.println("Usage: CyberShake_AWP_SGT_DAXGen <run id> <gridout file> <output dax file> ['separate']");
			System.exit(-1);
		}
		
		int runID = Integer.parseInt(args[0]);
		riq = new RunIDQuery(runID);
		String gridoutFilename = args[1];
		String outputDAXFilename = args[2];
		boolean separateVelJobs = false;
		if (args.length==5) {
			separateVelJobs = true;
		}
		
		int[] dims = getVolume(gridoutFilename);
		int[] procDims = getProcessors(dims);
		
		ADAG sgtDAX = new ADAG("AWP_SGT_" + riq.getSiteName() + ".dax");
		
		Job velocityJob = null;
		
		if (separateVelJobs) {
			Job vMeshGen = addVMeshGen(riq.getVelModelID());
			sgtDAX.addJob(vMeshGen);
		
			Job vMeshMerge = addVMeshMerge();
			sgtDAX.addJob(vMeshMerge);
			sgtDAX.addDependency(vMeshGen, vMeshMerge);
			
			velocityJob = vMeshMerge;
		} else {
			Job vMeshJob = addVMeshSingle();
			sgtDAX.addJob(vMeshJob);
			
			velocityJob = vMeshJob;
		}
	
		Job preSGT = addPreSGT();
		sgtDAX.addJob(preSGT);
		
		Job preAWP = addPreAWP(separateVelJobs, procDims);
		sgtDAX.addJob(preAWP);
		sgtDAX.addDependency(preSGT, preAWP);
		sgtDAX.addDependency(velocityJob, preAWP);
		
		Job awpSGTx = addAWPSGTGen("x", procDims);
		sgtDAX.addJob(awpSGTx);
		Job awpSGTy = addAWPSGTGen("y", procDims);
		sgtDAX.addJob(awpSGTy);
		
		sgtDAX.addDependency(preAWP, awpSGTx);
		sgtDAX.addDependency(preAWP, awpSGTy);
		
		Job postAWPX = addPostAWP("x", separateVelJobs);
		sgtDAX.addJob(postAWPX);
		sgtDAX.addDependency(awpSGTx, postAWPX);
		Job postAWPY = addPostAWP("y", separateVelJobs);
		sgtDAX.addJob(postAWPY);
		sgtDAX.addDependency(awpSGTy, postAWPY);
		
		Job nanCheckX = addAWPNanCheck("x");
		sgtDAX.addJob(nanCheckX);
		sgtDAX.addDependency(awpSGTx, nanCheckX);
		Job nanCheckY = addAWPNanCheck("y");
		sgtDAX.addJob(nanCheckY);
		sgtDAX.addDependency(awpSGTx, nanCheckY);
		
		Job updateEnd = addUpdate("SGT_START", "SGT_END");
		sgtDAX.addJob(updateEnd);
		sgtDAX.addDependency(postAWPX, updateEnd);
		sgtDAX.addDependency(postAWPY, updateEnd);
		sgtDAX.addDependency(postAWPX, updateEnd);
		sgtDAX.addDependency(postAWPX, updateEnd);
		
		sgtDAX.writeToFile(outputDAXFilename);
		
	}

	private static int[] getVolume(String gridoutFilename) {
		int[] dims = new int[3];
		//Have to swap X and Y for AWP, since gridout has RWG X and RWG Y
		try {
			BufferedReader br = new BufferedReader(new FileReader(gridoutFilename));
			//2nd line has nx
			br.readLine();
			String line = br.readLine();
			//This is the AWP Y
			dims[1] = Integer.parseInt(line.split("=")[1]);
			for (int i=0; i<dims[1]+1; i++) {
				br.readLine();
			}
			line = br.readLine();
			//This is AWP X
			dims[0] = Integer.parseInt(line.split("=")[1]);
			for (int i=0; i<dims[0]+1; i++) {
				br.readLine();
			}
			line = br.readLine();
			dims[2] = Integer.parseInt(line.split("=")[1]);
			br.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
		return dims;
	}

	private static int[] getProcessors(int[] dims) {
		int [] procDims = new int[3];
		if (riq.getSgtString().equals("awp")) {
			//We would like each processor responsible for 64x64x50 ideally; if not, then 64x50x50; if not, then something around 64 x 50 x 50.
			//Z-dimension can always be 50
			if (dims[2] % 50 != 0) {
				System.err.println("Z-dimension is " + dims[2] + " which is not divisible by 50, aborting.");
			}
			procDims[2] = dims[2] / 50;
			//First, see if one/both of the dimensions is divisible by 64
			boolean x_by_64 = ((dims[0] % 64) == 0);
			boolean y_by_64 = ((dims[1] % 64) == 0);
			if (x_by_64 || y_by_64) {
				//We can at least do 64x50x50
				if (x_by_64) {
					procDims[0] = dims[0] / 64;
				} else {
					procDims[0] = dims[0] / 50;
				}
				if (y_by_64) {
					procDims[1] = dims[1] / 64;
				} else {
					procDims[1] = dims[1] / 50;
				}
			} else {
				//Walk up/down from 64 on both X and Y
				int xTest = 63;
				int yTest = 63;
				int adjust = 2;
				
				//Eventually we'll get to 50, which we know will work
				while ((dims[0] % xTest)!=0 && (dims[1] % yTest)!=0) {
					//Change both
					adjust = (int)Math.copySign(adjust+1, -1*adjust);
					xTest += adjust;
					yTest += adjust;
				}
				if ((dims[0] % xTest)==0) {
					procDims[0] = dims[0] / xTest;
					procDims[1] = dims[1] / 50;
				} else if ((dims[1] % yTest)==0) {
					procDims[0] = dims[0] / 50;
					procDims[1] = dims[1] / yTest;
				} else {
					System.err.println("Weird error condition:  we thought either " + dims[0] + " was divisible by " + xTest + " or " + dims[1] + " was divisible by " + yTest + ", but we were tragically wrong.");
					System.exit(2);
				}
			}
		} else if (riq.getSgtString().equals("awp_gpu")) {
			//Choose core count to be 10 x 10; each processor must be responsible for an even chunk, so each dim must be divisible by 20
			for (int i=0; i<3; i++) {
				if (dims[i] % 20 != 0) {
					System.err.println("One of the volume dimensions is " + dims[i] + " which is not divisible by 20.  Aborting.");
					System.exit(3);
				}
			}
			procDims[0] = 10;
			procDims[1] = 10;
			procDims[2] = 1;
		} else {
			System.err.println("SGT string " + riq.getSgtString() + " is not 'awp' or 'awp_gpu', so we don't know what to do with it in CyberShake_AWP_SGT_DAXGen.");
			System.exit(2);
		}
		
		return procDims;
	}
	
	
	
	private static Job addPreSGT() {
		String id = "PreSGT_" + riq.getSiteName();
		Job preSGTJob = new Job(id, "scec", "PreSGT", "1.0");
		
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File faultlistFile = new File(riq.getSiteName() + ".faultlist");
		File radiusFile = new File(riq.getSiteName() + ".radiusfile");
		File sgtcordFile = new File(riq.getSiteName() + ".cordfile");
		
		modelboxFile.setTransfer(File.TRANSFER.TRUE);
		gridoutFile.setTransfer(File.TRANSFER.TRUE);
		coordFile.setTransfer(File.TRANSFER.FALSE);
		fdlocFile.setTransfer(File.TRANSFER.FALSE);
		faultlistFile.setTransfer(File.TRANSFER.FALSE);
		radiusFile.setTransfer(File.TRANSFER.FALSE);
		sgtcordFile.setTransfer(File.TRANSFER.FALSE);
		
		modelboxFile.setRegister(false);
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
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

	
	private static Job addPreAWP(boolean separate, int[] procDims) {
		String jobname = "PreAWP";
		if (riq.getSgtString().equals("awp_gpu")) {
			jobname = "PreAWP_GPU";
		}
		String id = jobname + "_" + riq.getSiteName() + "_" + riq.getVelModelString();
		Job preAWPJob = new Job(id, "scec", jobname, "1.0");
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File mergeVelocityFile = new File("v_sgt-" + riq.getSiteName());
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File cordFile = new File(riq.getSiteName() + ".cordfile");

		gridoutFile.setTransfer(TRANSFER.TRUE);
		mergeVelocityFile.setTransfer(TRANSFER.FALSE);
		fdlocFile.setTransfer(TRANSFER.FALSE);
		cordFile.setTransfer(TRANSFER.FALSE);

		gridoutFile.setRegister(false);
		mergeVelocityFile.setRegister(false);
		fdlocFile.setRegister(false);
		cordFile.setRegister(false);
		
		preAWPJob.addArgument("--site " + riq.getSiteName());
		preAWPJob.addArgument("--gridout " + gridoutFile.getName());
		preAWPJob.addArgument("--fdloc " + fdlocFile.getName());
		preAWPJob.addArgument("--cordfile " + cordFile.getName());
		preAWPJob.addArgument("--frequency " + riq.getFrequencyString());
		preAWPJob.addArgument("--px " + procDims[0]);
		preAWPJob.addArgument("--py " + procDims[1]);
		preAWPJob.addArgument("--pz " + procDims[2]);
		
		//Only need to reformat velocity if we ran separate velocity jobs
		if (separate) {
			preAWPJob.addArgument("--velocity-prefix " + mergeVelocityFile);
		}
		
		preAWPJob.uses(gridoutFile, LINK.INPUT);
		preAWPJob.uses(mergeVelocityFile, LINK.INPUT);
		preAWPJob.uses(fdlocFile, LINK.INPUT);
		preAWPJob.uses(cordFile, LINK.INPUT);
		
		return preAWPJob;
	}
	
	private static Job addAWPSGTGen(String component, int[] procDims) {
		String jobname = "AWP";
		if (riq.getSgtString().equals("awp_gpu")) {
			jobname = "AWP_GPU";
		}
		String id = jobname + "_" + riq.getSiteName() + "_" + riq.getVelModelString() + "_" + component;
		Job awpJob = new Job(id, "scec", jobname, "1.0");
		
		File in3DFile = new File("IN3D." + riq.getSiteName() + "." + component);
		
		in3DFile.setTransfer(TRANSFER.FALSE);
		
		in3DFile.setRegister(false);

		int cores = procDims[0]*procDims[1]*procDims[2];
		int hosts = (int)Math.ceil(((double)cores)/32);
		if (riq.getSgtString().equals("awp_gpu")) {
			hosts = cores;
		}
		
		if (riq.getSgtString().equals("awp")) {
			awpJob.addArgument("" + cores);
		}
		awpJob.addArgument(in3DFile);
		
		awpJob.uses(in3DFile, LINK.INPUT);
				
		awpJob.addProfile("globus", "host_count", "" + hosts);
		awpJob.addProfile("globus", "count", "" + cores);
		awpJob.addProfile("pegasus", "cores", "" + cores);
		
		return awpJob;
	}
		
	private static Job addVMeshSingle() {
		String id = "UCVMMesh_" + riq.getSiteName();
		Job vMeshJob = new Job(id, "scec", "UCVMMesh", "1.0");
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		
		vMeshJob.addArgument(riq.getSiteName());
		vMeshJob.addArgument(gridoutFile);
		vMeshJob.addArgument(coordFile);
		vMeshJob.addArgument(riq.getVelModelString());
		
		String sgtType = riq.getSgtString();
		if (sgtType.contains("rwg")) {
			vMeshJob.addArgument("rwg");
		} else if (sgtType.contains("awp")) {
			vMeshJob.addArgument("awp");
		} else {
			System.err.println("SGT type " + sgtType + " is not RWG or AWP.  Not sure what velocity mesh format to use, aborting.");
			System.exit(1);
		}
		
		vMeshJob.addArgument(riq.getFrequencyString());
		
		gridoutFile.setTransfer(File.TRANSFER.FALSE);
		coordFile.setTransfer(File.TRANSFER.FALSE);
		
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
		
		vMeshJob.uses(gridoutFile, File.LINK.INPUT);
		vMeshJob.uses(coordFile, File.LINK.INPUT);
		
		if (riq.getSiteName().equals("TEST")) {
			vMeshJob.addProfile("globus", "count", "155");
		}
		
		return vMeshJob;
	}
	
	private static Job addVMeshGen(int velID) {
		String id = "UCVMMeshGen_" + riq.getSiteName();
		Job vMeshGenJob = new Job(id, "scec", "UCVMMeshGen", "1.0");
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		
		vMeshGenJob.addArgument(riq.getSiteName());
		vMeshGenJob.addArgument(gridoutFile);		
		vMeshGenJob.addArgument(coordFile);
		vMeshGenJob.addArgument(riq.getVelModelString());
		
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
		
		vMeshGenJob.uses(gridoutFile, File.LINK.INPUT);
		vMeshGenJob.uses(coordFile, File.LINK.INPUT);
		
		return vMeshGenJob;
	}
	
	private static Job addVMeshMerge() {
		String id = "VMeshMerge_" + riq.getSiteName();
		Job vMeshMergeJob = new Job(id, "scec", "UCVMMeshMerge", "1.0");
				
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
	
	private static Job addPostAWP(String component, boolean separate) {
		String id = "PostAWP_" + riq.getSiteName() + "_" + riq.getVelModelString() + "_" + component;
		Job postAWPJob = new Job(id, "scec", "PostAWP", "1.0");
		
		String awpSubdir = "AWP_SGT_" + riq.getSiteName();
		
		File awpStrainInFile = new File(awpSubdir + "/comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component);
		//We swap the component value in the output file, because AWP X = RWG Y
		String rwgComponent = "z";
		if (component.equals("x")) {
			rwgComponent = "y";
		} else if (component.equals("y")) {
			rwgComponent = "x";
		}
		File awpStrainOutFile = new File(riq.getSiteName() + "_f" + rwgComponent + "_" + riq.getRunID() + ".sgt");
		File md5OutFile = new File(awpStrainOutFile.getName() + ".md5");
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File cordFile = new File(riq.getSiteName() + ".cordfile");
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File in3DFile = new File(awpSubdir + "/IN3D." + riq.getSiteName() + "." + component);
		File awpMediaFile = new File("awp." + riq.getSiteName() + ".media");
		if (separate) {
			awpMediaFile = new File("v_sgt-" + riq.getSiteName());
		}
		File headerFile = new File(riq.getSiteName() + "_f" + rwgComponent + "_" + riq.getRunID() + ".sgthead");
		
		awpStrainInFile.setTransfer(TRANSFER.FALSE);
		modelboxFile.setTransfer(TRANSFER.FALSE);
		cordFile.setTransfer(TRANSFER.FALSE);
		fdlocFile.setTransfer(TRANSFER.FALSE);
		gridoutFile.setTransfer(TRANSFER.FALSE);
		in3DFile.setTransfer(TRANSFER.FALSE);
		awpMediaFile.setTransfer(TRANSFER.FALSE);
		
		awpStrainOutFile.setTransfer(TRANSFER.TRUE);
		headerFile.setTransfer(TRANSFER.TRUE);
		md5OutFile.setTransfer(TRANSFER.TRUE);
		
		awpStrainInFile.setRegister(false);
		modelboxFile.setRegister(false);
		cordFile.setRegister(false);
		fdlocFile.setRegister(false);
		gridoutFile.setRegister(false);
		in3DFile.setRegister(false);
		awpMediaFile.setRegister(false);
		
		awpStrainOutFile.setRegister(true);
		headerFile.setRegister(true);
		md5OutFile.setRegister(true);

		postAWPJob.addArgument(riq.getSiteName());
		postAWPJob.addArgument(awpStrainInFile);
		postAWPJob.addArgument(awpStrainOutFile);
		postAWPJob.addArgument(modelboxFile);
		postAWPJob.addArgument(cordFile);
		postAWPJob.addArgument(fdlocFile);
		postAWPJob.addArgument(gridoutFile);
		postAWPJob.addArgument(in3DFile);
		postAWPJob.addArgument(awpMediaFile);
		postAWPJob.addArgument(component);
		postAWPJob.addArgument(riq.getRunID() + "");
		postAWPJob.addArgument(headerFile);

		postAWPJob.uses(awpStrainInFile, LINK.INPUT);
		postAWPJob.uses(awpStrainOutFile, LINK.OUTPUT);
		postAWPJob.uses(modelboxFile, LINK.INPUT);
		postAWPJob.uses(cordFile, LINK.INPUT);
		postAWPJob.uses(fdlocFile, LINK.INPUT);
		postAWPJob.uses(gridoutFile, LINK.INPUT);
		postAWPJob.uses(in3DFile, LINK.INPUT);
		postAWPJob.uses(awpMediaFile, LINK.INPUT);
		postAWPJob.uses(headerFile, LINK.OUTPUT);
		postAWPJob.uses(md5OutFile, LINK.OUTPUT);
		
		return postAWPJob;
	}

	private static Job addAWPNanCheck(String component) {
		String id = "AWP_NaN_Check_" + component;
		Job awpJob = new Job(id, "scec", "AWP_NaN_Check", "1.0");
		
		String awpSubdir = "AWP_SGT_" + riq.getSiteName();
		
		File awpStrainInFile = new File(awpSubdir + "/comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component);
		awpStrainInFile.setTransfer(TRANSFER.FALSE);
		awpStrainInFile.setRegister(false);
		
		awpJob.addArgument(awpStrainInFile.getName());
		awpJob.uses(awpStrainInFile, LINK.INPUT);

		return awpJob;
	}
	
	 private static Job addUpdate(String from_state, String to_state) {
			String id = "UpdateRun_" + to_state + "_" + riq.getSiteName();
			Job updateJob = new Job(id, "scec", "UpdateRun", "1.0");

			updateJob.addArgument(riq.getRunID() + "");
			updateJob.addArgument(from_state);
			updateJob.addArgument(to_state);
			
			updateJob.addProfile("globus", "maxWallTime","5");
			updateJob.addProfile("hints","executionPool", "local");
		
			return updateJob;
		}
}