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
	
	public static void main(String[] args) {
		if (args.length<3) {
			System.out.println("Usage: CyberShake_AWP_SGT_DAXGen <run id> <gridout file> <output dax file> ['separate']");
			System.exit(-1);
		}
		
		int runID = Integer.parseInt(args[0]);
		RunIDQuery riq = new RunIDQuery(runID);
		String gridoutFilename = args[1];
		String outputDAXFilename = args[2];
		boolean separate = false;
		if (args.length==5) {
			separate = true;
		}
		
		int[] dims = getVolume(gridoutFilename);
		int[] procDims = getProcessors(dims, riq);
		
		ADAG sgtDAX = new ADAG("AWP_SGT_" + "_" + riq.getSiteName() + ".dax");
		
		Job preAWP = addPreAWP(riq, separate, procDims);
		sgtDAX.addJob(preAWP);
		
		Job awpSGTxJob = addAWPSGTGen("x", riq, procDims);
		sgtDAX.addJob(awpSGTxJob);
		Job awpSGTyJob = addAWPSGTGen("y", riq, procDims);
		sgtDAX.addJob(awpSGTyJob);
		
		sgtDAX.addDependency(preAWP, awpSGTxJob);
		sgtDAX.addDependency(preAWP, awpSGTyJob);
		
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

	private static int[] getProcessors(int[] dims, RunIDQuery riq) {
		int [] procDims = new int[3];
		if (riq.getSgtString().equals("awp")) {
			//Choose core count so that each processor is responsible for 50x50x50 grid points
			for (int i=0; i<3; i++) {
				if (dims[i] % 50 != 0) {
					System.err.println("One of the volume dimensions is " + dims[i] + " which is not divisible by 50.  Aborting.");
					System.exit(3);
				}
				procDims[i] = dims[i] / 50;
			}
		} else if (riq.getSgtString().equals("awp_gpu")) {
			//Choose core count so each processor is responsible for 200x200x200 grid points
			for (int i=0; i<3; i++) {
				if (dims[i] % 200 != 0) {
					System.err.println("One of the volume dimensions is " + dims[i] + " which is not divisible by 200.  Aborting.");
					System.exit(3);
				}
				procDims[i] = dims[i] / 200;
			}
		} else {
			System.err.println("SGT string " + riq.getSgtString() + " is not 'awp' or 'awp_gpu', so we don't know what to do with it in CyberShake_AWP_SGT_DAXGen.");
			System.exit(2);
		}
		
		return procDims;
	}
	
	private static Job addPreAWP(RunIDQuery riq, boolean separate, int[] procDims) {
		String jobname = "PreAWP";
		if (riq.getSgtString().equals("awp_gpu")) {
			jobname = "PreAWP_GPU";
		}
		String id = jobname + "_" + riq.getSiteName() + "_" + riq.getVelModelString();
		Job preAWPJob = new Job(id, "scec", jobname, "1.0");
		
		File gridoutFile = new File("../gridout_" + riq.getSiteName());
		File mergeVelocityFile = new File("../v_sgt-" + riq.getSiteName());
		File fdlocFile = new File("../" + riq.getSiteName() + ".fdloc");
		File cordFile = new File("../" + riq.getSiteName() + ".cordfile");

		gridoutFile.setTransfer(TRANSFER.FALSE);
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
	
	private static Job addAWPSGTGen(String component, RunIDQuery riq, int[] procDims) {
		String jobname = "AWP";
		if (riq.getSgtString().equals("awp_gpu")) {
			jobname = "AWP_GPU";
		}
		String id = jobname + "_" + riq.getSiteName() + "_" + riq.getVelModelString() + "_" + component;
		Job awpJob = new Job(id, "scec", jobname, "1.0");
		
		File in3DFile = new File("IN3D." + riq.getSiteName() + "." + component);
		
		in3DFile.setTransfer(TRANSFER.FALSE);
		
		in3DFile.setRegister(false);
		
		awpJob.addArgument(in3DFile);
		
		awpJob.uses(in3DFile, LINK.INPUT);
		
		int cores = procDims[0]*procDims[1]*procDims[2];
		int hosts = cores/32;
		if (riq.getSgtString().equals("awp_gpu")) {
			hosts = cores;
		}
		
		awpJob.addProfile("globus", "host_count", "" + hosts);
		awpJob.addProfile("globus", "count", "" + cores);
		awpJob.addProfile("pegasus", "cores", "" + cores);
		
		return awpJob;
	}
		
}