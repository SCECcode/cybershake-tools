package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.FileReader;

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


public class CyberShake_AWP_SGT_DAXGen {
	//Called as part of workflow to handle custom number of cores for each AWP SGT job
	private static RunIDQuery riq = null;
	
	public static void main(String[] args) {
		if (args.length<3) {
			System.out.println("Usage: CyberShake_AWP_SGT_DAXGen <run id> <gridout file> <output dax file> ['separate']");
			System.exit(-1);
		}
		
		Options cmd_opts = new Options();
		
        Option runIDopt = OptionBuilder.withArgName("runID").hasArg().withDescription("Run_ID").create("r");
        Option gridoutFile = OptionBuilder.withArgName("gridout").hasArg().withDescription("gridout_file").create("gf");
        Option outputDAX = OptionBuilder.withArgName("output").hasArg().withDescription("output DAX filename").create("o");
        Option separateVelJobsOpt = new Option("sv", "split-velocity", false, "Use separate velocity generation and merge jobs (default is to use combined job)");
        Option maxSGTCores = OptionBuilder.withArgName("maxCores").hasArg().withDescription("maximum number of cores for SGT jobs").create("mc");
        Option separateMD5Jobs = new Option("sm", "separate-md5", false, "Run md5 jobs separately from PostAWP jobs (default is to combine).");
        Option handoffJobOpt = new Option("hd", "handoff", false, "Run handoff job, which puts SGT into pending file on shock when completed.");
        Option spacingOpt = OptionBuilder.withArgName("spacing").hasArg().withDescription("Override the default grid spacing, in km.").create("sp");
        Option minvs = OptionBuilder.withArgName("minvs").hasArg().withDescription("Override the minimum Vs value").create("mv");
        Option server = OptionBuilder.withArgName("server").hasArg().withDescription("Server to use for site parameters and to insert PSA values into").create("sr");
        Option noSmoothing = new Option("ns", "no-smoothing", false, "Turn off smoothing (default is to smooth)");
		Option h_frac = OptionBuilder.withArgName("h_fraction").withLongOpt("h_fraction").hasArg().withDescription("Depth, in fractions of a grid point, to query UCVM at when populating the surface points.").create("hf");
        Option elyTaper = OptionBuilder.withArgName("ely-taper").hasArg().withDescription("Ely taper mode to use, either 'all' (always use the taper), 'none' (the default: use the model, never the taper), or 'ifless' (at each point, use the approach with the smaller Vs)").withLongOpt("ely-taper").create("et");
        Option taperDepthOpt = OptionBuilder.withArgName("taper-depth").hasArg().withDescription("Depth in meters to use with Ely taper for 'all' or 'ifless' modes.  Default is 700.").withLongOpt("taper-depth").create("td");
        Option taperModels = OptionBuilder.withArgName("taper-models").hasArg().withDescription("List of models to apply the taper to, 'all', or 'none'.  Default is 'all'.").withLongOpt("taper-models").create("tm");
        Option z_comp = new Option("z", "z_comp", false, "Calculate SGTs for the vertical Z component.");
        Option localVelParamInsert = new Option("lv", "local_vparam_insert", false, "Insert the velocity parameters into the database in a separate job on shock, not on the remote compute nodes.");
        
        cmd_opts.addOption(runIDopt);
        cmd_opts.addOption(gridoutFile);
        cmd_opts.addOption(outputDAX);
        cmd_opts.addOption(separateVelJobsOpt);
        cmd_opts.addOption(maxSGTCores);
        cmd_opts.addOption(separateMD5Jobs);
        cmd_opts.addOption(handoffJobOpt);
        cmd_opts.addOption(spacingOpt);
        cmd_opts.addOption(minvs);
        cmd_opts.addOption(server);
        cmd_opts.addOption(noSmoothing);
		cmd_opts.addOption(h_frac);
		cmd_opts.addOption(elyTaper);
		cmd_opts.addOption(taperDepthOpt);
		cmd_opts.addOption(taperModels);
		cmd_opts.addOption(z_comp);
		cmd_opts.addOption(localVelParamInsert);
        
        String usageString = "CyberShake_AWP_SGT_DAXGen [options]";
        CommandLineParser parser = new GnuParser();
        if (args.length<4) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
            System.exit(1);
        }
        
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(2);
        }
        
        int runID = -1;
        if (line.hasOption(runIDopt.getOpt())) {
        	runID = Integer.parseInt(line.getOptionValue(runIDopt.getOpt()));
        } else {
        	System.err.println("Run ID must be provided.");
        	System.exit(1);
        }
        
        String db_server = "moment.usc.edu";
        if (line.hasOption(server.getOpt())) {
        	db_server = line.getOptionValue(server.getOpt());
        }
        
		riq = new RunIDQuery(runID, db_server);
		
		String gridoutFilename = "";		
		if (line.hasOption(gridoutFile.getOpt())) {
        	gridoutFilename = line.getOptionValue(gridoutFile.getOpt());
        } else {
        	System.err.println("gridout file must be provided.");
        	System.exit(2);
        }
        
		String outputDAXFilename = "";
		if (line.hasOption(outputDAX.getOpt())) {
			outputDAXFilename = line.getOptionValue(outputDAX.getOpt());
        } else {
        	System.err.println("output DAX file must be provided.");
        	System.exit(3);
        }

		boolean separateVelJobs = false;
		if (line.hasOption(separateVelJobsOpt.getOpt())) {
			separateVelJobs = true;
		}
		
		int maxCores = -1;
		if (line.hasOption(maxSGTCores.getOpt())) {
			maxCores = Integer.parseInt(line.getOptionValue(maxSGTCores.getOpt()));
		}
		
		int[] dims = getVolume(gridoutFilename);
		int[] procDims = getProcessors(dims, maxCores);

		boolean separateMD5 = false;
		if (line.hasOption(separateMD5Jobs.getOpt())) {
			separateMD5 = true;
		}
		
		boolean handoffJob = false;
		if (line.hasOption(handoffJobOpt.getOpt())) {
				handoffJob = true;
		}
		
		double spacing = -1.0;
		if (line.hasOption(spacingOpt.getOpt())) {
			spacing = Double.parseDouble(line.getOptionValue(spacingOpt.getOpt()));
		}
		
		double min_vs = -1.0;
		if (line.hasOption(minvs.getOpt())) {
			min_vs = Double.parseDouble(line.getOptionValue(minvs.getOpt()));
		}
		
		boolean smoothing = true;
		if (line.hasOption(noSmoothing.getOpt())) {
			smoothing = false;
		}
	
		//Set default h_frac to 0.25
		double h_frac_val = 0.25;	
		if (line.hasOption(h_frac.getOpt())) {
			h_frac_val = Double.parseDouble(line.getOptionValue(h_frac.getOpt()));
		}

		String taperMode = "none";
		if (line.hasOption(elyTaper.getOpt())) {
			taperMode = line.getOptionValue(elyTaper.getOpt());
		}
		
		
		double taperDepth = 700.0;
		if (line.hasOption(taperDepthOpt.getOpt())) {
			taperDepth = Double.parseDouble(line.getOptionValue(taperDepthOpt.getOpt()));
		}
		
		String taperModelsString = "none";
		if (line.hasOption(taperModels.getOpt())) {
			taperModelsString = line.getOptionValue(taperModels.getOpt());
		}
		
		boolean runZComp = false;
		if (line.hasOption(z_comp.getOpt())) {
			runZComp = true;
		}
		
		boolean lVelParmInsert = false;
		if (line.hasOption(localVelParamInsert.getOpt())) {
			lVelParmInsert = true;
		}
		
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
			Job vMeshJob = addVMeshSingle(spacing, min_vs, h_frac_val, taperMode, taperDepth, taperModelsString);
			sgtDAX.addJob(vMeshJob);
			
			velocityJob = vMeshJob;
		}
		
		Job smoothingJob = null;
		if (smoothing) {
			smoothingJob = addSmoothing(spacing);
			sgtDAX.addJob(smoothingJob);
			sgtDAX.addDependency(velocityJob, smoothingJob);
		}
		
		Job preSGT = addPreSGT(spacing);
		sgtDAX.addJob(preSGT);
		
		Job velocityParams = addVelocityParams(db_server, smoothing, lVelParmInsert);
		sgtDAX.addJob(velocityParams);
		if (smoothing) {
			sgtDAX.addDependency(smoothingJob, velocityParams);
		} else {
			sgtDAX.addDependency(velocityJob, velocityParams);
		}
		sgtDAX.addDependency(preSGT, velocityParams);
		
		Job preAWP = addPreAWP(separateVelJobs, procDims, spacing, smoothing, runZComp);
		sgtDAX.addJob(preAWP);
		sgtDAX.addDependency(preSGT, preAWP);
		sgtDAX.addDependency(velocityJob, preAWP);
		if (smoothing) {
			//PreAWP might have to reformat the velocity file
			sgtDAX.addDependency(smoothingJob, preAWP);
		}
		
		Job awpSGTx = addAWPSGTGen("x", procDims);
		sgtDAX.addJob(awpSGTx);
		Job awpSGTy = addAWPSGTGen("y", procDims);
		sgtDAX.addJob(awpSGTy);
		Job awpSGTz = null;
		if (runZComp==true) {
			awpSGTz = addAWPSGTGen("z", procDims);
			sgtDAX.addJob(awpSGTz);
		}
		
		sgtDAX.addDependency(preAWP, awpSGTx);
		sgtDAX.addDependency(preAWP, awpSGTy);
		if (runZComp==true) {
			sgtDAX.addDependency(preAWP, awpSGTz);
		}
		
		Job postAWPX = addPostAWP("x", separateVelJobs, separateMD5, smoothing);
		sgtDAX.addJob(postAWPX);
		sgtDAX.addDependency(awpSGTx, postAWPX);
		Job postAWPY = addPostAWP("y", separateVelJobs, separateMD5, smoothing);
		sgtDAX.addJob(postAWPY);
		sgtDAX.addDependency(awpSGTy, postAWPY);
		if (smoothing) {
			//PostAWP uses the velocity file
			sgtDAX.addDependency(smoothingJob, postAWPX);
			sgtDAX.addDependency(smoothingJob, postAWPY);
		}
		Job postAWPZ = null;
		if (runZComp==true) {
			postAWPZ = addPostAWP("z", separateVelJobs, separateMD5, smoothing);
			sgtDAX.addJob(postAWPZ);
			sgtDAX.addDependency(awpSGTz, postAWPZ);
			if (smoothing) {
				sgtDAX.addDependency(smoothingJob, postAWPZ);
			}
		}

		Job nanCheckX = addAWPNanCheck("x");
		sgtDAX.addJob(nanCheckX);
		sgtDAX.addDependency(awpSGTx, nanCheckX);
		Job nanCheckY = addAWPNanCheck("y");
		sgtDAX.addJob(nanCheckY);
		sgtDAX.addDependency(awpSGTy, nanCheckY);
		Job nanCheckZ = null;
		if (runZComp==true) {
			nanCheckZ = addAWPNanCheck("z");
			sgtDAX.addJob(nanCheckZ);
			sgtDAX.addDependency(awpSGTz, nanCheckZ);
		}
		
		Job updateEnd = addUpdate("SGT_START", "SGT_END");
		sgtDAX.addJob(updateEnd);
		
		if (lVelParmInsert==true) {
			Job paramInsertJob = addInsertVelocityParams();
			sgtDAX.addJob(paramInsertJob);
			sgtDAX.addDependency(velocityParams, paramInsertJob);
			sgtDAX.addDependency(paramInsertJob, updateEnd);
		}
		
		Job md5X, md5Y, md5Z;
		md5X = md5Y = md5Z = null;
		
		if (separateMD5) {
			md5X = addMD5("x");
			md5Y = addMD5("y");
			sgtDAX.addJob(md5X);
			sgtDAX.addJob(md5Y);
			sgtDAX.addDependency(md5X, updateEnd);
			sgtDAX.addDependency(md5Y, updateEnd);
			if (runZComp==true) {
				md5Z = addMD5("z");
				sgtDAX.addJob(md5Z);
				sgtDAX.addDependency(md5Z, updateEnd);
			}
		} else {		
			sgtDAX.addDependency(postAWPX, updateEnd);
			sgtDAX.addDependency(postAWPY, updateEnd);
			if (runZComp==true) {
				sgtDAX.addDependency(postAWPZ, updateEnd);
			}
		}
		sgtDAX.addDependency(nanCheckX, updateEnd);
		sgtDAX.addDependency(nanCheckY, updateEnd);
		if (runZComp==true) {
			sgtDAX.addDependency(nanCheckZ, updateEnd);
		}
		
		//Add dependency on VelocityParams, since otherwise update can run even though velocityParams fails
		sgtDAX.addDependency(velocityParams, updateEnd);
		
		if (handoffJob==true) {
			Job handoff = addHandoff();
			sgtDAX.addJob(handoff);
			sgtDAX.addDependency(postAWPX, handoff);
			sgtDAX.addDependency(postAWPY, handoff);
			sgtDAX.addDependency(nanCheckX, handoff);
			sgtDAX.addDependency(nanCheckY, handoff);
			if (separateMD5) {
				sgtDAX.addDependency(md5X, handoff);
				sgtDAX.addDependency(md5Y, handoff);
			}
			if (runZComp==true) {
				sgtDAX.addDependency(postAWPZ, handoff);
				sgtDAX.addDependency(nanCheckZ, handoff);
				if (separateMD5) {
					sgtDAX.addDependency(md5Z, handoff);
				}
			}
			sgtDAX.addDependency(handoff, updateEnd);
		}

		
		sgtDAX.writeToFile(outputDAXFilename);
		
	}


	private static Job addVelocityParams(String server, boolean smoothing, boolean localVelParamInsert) {
		String id = "Velocity_Params_" + riq.getSiteName();
		Job velocityParamsJob = new Job(id, "scec", "Velocity_Params", "1.0");
		
		File velocityMesh = new File("awp." + riq.getSiteName() + ".media");
		if (smoothing) {
			velocityMesh = new File("awp." + riq.getSiteName() + ".smoothed.media");
		}
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");

		velocityMesh.setTransfer(File.TRANSFER.FALSE);
		gridoutFile.setTransfer(File.TRANSFER.TRUE);
		fdlocFile.setTransfer(File.TRANSFER.FALSE);
		
		velocityMesh.setRegister(false);
		gridoutFile.setRegister(false);
		fdlocFile.setRegister(false);
		
		velocityParamsJob.addArgument("-lat " + riq.getLat());
		velocityParamsJob.addArgument("-lon " + riq.getLon());
		velocityParamsJob.addArgument("-m " + riq.getVelModelString());
		velocityParamsJob.addArgument("-vmesh " + velocityMesh.getName());
		velocityParamsJob.addArgument("-fd " + fdlocFile.getName());
		velocityParamsJob.addArgument("-go " + gridoutFile.getName());
		velocityParamsJob.addArgument("-s " + server);
		velocityParamsJob.addArgument("-r " + riq.getRunID());
		
		if (localVelParamInsert==true) {
			//Insert values into a file, not the DB
			File velParamFile = new File(riq.getSiteName() + "_" + riq.getRunID() + "_velocity_params.txt");
			velParamFile.setTransfer(TRANSFER.TRUE);
			velocityParamsJob.addArgument("-o " + velParamFile.getName());
			velocityParamsJob.uses(velParamFile, LINK.OUTPUT);
			
		}
		
		velocityParamsJob.uses(velocityMesh, LINK.INPUT);
		velocityParamsJob.uses(gridoutFile, LINK.INPUT);
		velocityParamsJob.uses(fdlocFile, LINK.INPUT);
		
		return velocityParamsJob;
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

	private static int[] getProcessors(int[] dims, int maxCores) {
		int [] procDims = new int[3];
		if (riq.getSgtString().equals("awp")) {
			if (maxCores<0) {
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
			} else {
				//Don't want to exceed max cores total
				//Assume 2 in Z-dimension
				if (dims[2] % 100 !=0) {
					System.err.println("Z-dimension is " + dims[2] + " which is not divisible by 50, aborting.");
				}
				procDims[2] = dims[2]/100;
				
				int xyCores = maxCores / procDims[2];
				//See what procDims[0] = procDims[1] = 50 would be
				int xTest = dims[0]/50;
				int yTest = dims[1]/50;
				int xyTest = xTest*yTest;
				if (xyTest<xyCores) {
					procDims[0] = xTest;
					procDims[1] = yTest;
				} else {
					double scale = ((double)xyCores)/xyTest;
					xTest = (int)(scale*xTest);
					yTest = (int)(scale*yTest);
					//Find a workable x value
					int adjust = 1;
					while (dims[0] % xTest != 0) {
						xTest += adjust;
						adjust = (int)Math.copySign(adjust+1, -1*adjust);
					}
					//Find y-value
					yTest = (int)(xyCores / xTest);
					while (dims[1] % yTest != 0) {
						yTest--;
					}
					procDims[0] = xTest;
					procDims[1] = yTest;
				}
			}
		} else if (riq.getSgtString().equals("awp_gpu")) {
			/*The X and Y dimension (NX and NY) always have to be divisible by Number of GPU (NPX and NPY) multiple with output decimation number (NXSKP and NYSKP).
				After that, the divided number also need to be divisible by 2, The formulas are as followings,
					NX % (NPX*NXSKP) == 0
					NY % (NPY*NYSKP) == 0,
				and
					(NX % (NPX*NXSKP)) % 2 == 0
					(NY % (NPY*NYSKP)) % 2 == 0 
					*/
			if (riq.getLowFrequencyCutoff()<1.0) {
				//Choose core count to be 10 x 10; each processor must be responsible for an even chunk, so each dim must be divisible by 20
				for (int i=0; i<2; i++) {
					if (dims[i] % 20 != 0) {
						System.err.println("One of the volume dimensions is " + dims[i] + " which is not divisible by 20.  Aborting.");
						System.exit(3);
					}
				}
				procDims[0] = 10;
				procDims[1] = 10;
				procDims[2] = 1;
			} else {
				/* For Study 16.9, suggestions:
					2760x4416x288, using 10x16x1=160 gpus (276*276*288) 
					2840x6560x288, using 10x20x1=200 gpus (284*328*288)
					*/ 
				//Determine total number of points
				long num_pts = ((long)dims[0])*((long)dims[1])*((long)dims[2]);
				long TWO_BILLION = 2000000000L;
				long SEVEN_BILLION = 7000000000L;
				long FIFTEEN_BILLION = 15000000000L;
				if (num_pts<TWO_BILLION) {
					//Use 10x10x1
					for (int i=0; i<2; i++) {
						if (dims[i] % 20 != 0) {
							System.err.println("We would like to use 10x10x1 GPUs, but one of the volume dimensions is " + dims[i] + " which is not divisible by 20.  Aborting.");
							System.exit(3);
						}
					}
					procDims[0] = 10;
					procDims[1] = 10;
					procDims[2] = 1;
				} else if (num_pts<SEVEN_BILLION) {
					//Use 20x10x1
					if (dims[0] % 40 != 0) {
						System.err.println("One of the volume dimensions is " + dims[0] + " which is not divisible by 40.  Aborting.");
						System.exit(3);
					}
					if (dims[1] % 20 != 0) {
						System.err.println("One of the volume dimensions is " + dims[1] + " which is not divisible by 20.  Aborting.");
						System.exit(3);
					}
					procDims[0] = 20;
					procDims[1] = 10;
					procDims[2] = 1;
				} else {
					//Study 24.8: 20x40x1
					//Study 18.8: change to 40x30x1
					//Go back to 20x20x1 for Summit testing
					if (dims[0] % 40 != 0) {
						System.err.println("One of the volume dimensions is " + dims[0] + " which is not divisible by 40.  Aborting.");
						System.exit(3);
					}
					if (dims[1] % 80 != 0) {
						System.err.println("One of the volume dimensions is " + dims[1] + " which is not divisible by 80.  Aborting.");
						System.exit(3);
					}
					procDims[0] = 20;
					procDims[1] = 40;
					procDims[2] = 1;
				}
				
			}
		} else {
			System.err.println("SGT string " + riq.getSgtString() + " is not 'awp' or 'awp_gpu', so we don't know what to do with it in CyberShake_AWP_SGT_DAXGen.");
			System.exit(2);
		}
		
		return procDims;
	}
	
	
	
	private static Job addPreSGT(double spacing) {
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
		coordFile.setTransfer(File.TRANSFER.TRUE);
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
		
		preSGTJob.addArgument("--site " + riq.getSiteName());
		preSGTJob.addArgument("--erf_id " + riq.getErfID());
		preSGTJob.addArgument("--modelbox " + modelboxFile.getName());
		preSGTJob.addArgument("--gridout " + gridoutFile.getName());
		preSGTJob.addArgument("--coordfile " + coordFile.getName());
		preSGTJob.addArgument("--fdloc " + fdlocFile.getName());
		preSGTJob.addArgument("--faultlist " + faultlistFile.getName());
		preSGTJob.addArgument("--radiusfile " + radiusFile.getName());
		preSGTJob.addArgument("--sgtcords " + sgtcordFile.getName());
		if (spacing<=0.0) {
			spacing = (0.1/Double.parseDouble(riq.getLowFrequencyCutoffString()));
		}
		preSGTJob.addArgument("--spacing " + spacing);
		
		preSGTJob.addArgument("--frequency " + riq.getLowFrequencyCutoffString());
		
		//Use Rup Var Scenario ID = 8
		if (riq.getRuptVarScenID()==8) {
			preSGTJob.addArgument("--rsqsim");
		}
		
		preSGTJob.uses(modelboxFile, File.LINK.INPUT);
		preSGTJob.uses(gridoutFile, File.LINK.INPUT);
		preSGTJob.uses(coordFile, File.LINK.INPUT);
		preSGTJob.uses(fdlocFile, File.LINK.OUTPUT);
		preSGTJob.uses(faultlistFile, File.LINK.OUTPUT);
		preSGTJob.uses(radiusFile, File.LINK.OUTPUT);
		preSGTJob.uses(sgtcordFile, File.LINK.OUTPUT);
		
		return preSGTJob;
	}

	
	private static Job addPreAWP(boolean separate, int[] procDims, double spacing, boolean smoothing, boolean zComp) {
		String jobname = "PreAWP";
		if (riq.getSgtString().equals("awp_gpu")) {
			jobname = "PreAWP_GPU";
		}
		String id = jobname + "_" + riq.getSiteName();
		Job preAWPJob = new Job(id, "scec", jobname, "1.0");
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File cordFile = new File(riq.getSiteName() + ".cordfile");
		//Try not telling Pegasus about IN3D file: this way it won't get transferred between
		//titan and titan-pilot, which results in the file being zeroed
		//File in3dXFile = new File("IN3D." + riq.getSiteName() + ".x");
		//File in3dYFile = new File("IN3D." + riq.getSiteName() + ".y");

		gridoutFile.setTransfer(TRANSFER.TRUE);
		fdlocFile.setTransfer(TRANSFER.FALSE);
		cordFile.setTransfer(TRANSFER.FALSE);
		//in3dXFile.setTransfer(TRANSFER.FALSE);
		//in3dYFile.setTransfer(TRANSFER.FALSE);

		gridoutFile.setRegister(false);
		fdlocFile.setRegister(false);
		cordFile.setRegister(false);
		//in3dXFile.setRegister(false);
		//in3dYFile.setRegister(false);
		
		preAWPJob.addArgument("--site " + riq.getSiteName());
		preAWPJob.addArgument("--gridout " + gridoutFile.getName());
		preAWPJob.addArgument("--fdloc " + fdlocFile.getName());
		preAWPJob.addArgument("--cordfile " + cordFile.getName());
		preAWPJob.addArgument("--frequency " + riq.getLowFrequencyCutoffString());
		preAWPJob.addArgument("--px " + procDims[0]);
		preAWPJob.addArgument("--py " + procDims[1]);
		preAWPJob.addArgument("--pz " + procDims[2]);
		preAWPJob.addArgument("--source-frequency " + riq.getSourceFrequency());
		preAWPJob.addArgument("--run_id " + riq.getRunID());
		
		if (spacing>0.0) {
			preAWPJob.addArgument("--spacing " + spacing);
		}
		
		if (smoothing) {
			preAWPJob.addArgument("--velocity-mesh awp." + riq.getSiteName() + ".smoothed.media");
		}
		
		//Only need to reformat velocity if we ran separate velocity jobs
		if (separate) {
			File mergeVelocityFile = new File("v_sgt-" + riq.getSiteName());
			mergeVelocityFile.setTransfer(TRANSFER.FALSE);
			mergeVelocityFile.setRegister(false);
			preAWPJob.uses(mergeVelocityFile, LINK.INPUT);

			preAWPJob.addArgument("--velocity-prefix " + mergeVelocityFile);
		}

		if (zComp==true) {
			preAWPJob.addArgument("--z_comp");
		}
		
		preAWPJob.uses(gridoutFile, LINK.INPUT);
		preAWPJob.uses(fdlocFile, LINK.INPUT);
		preAWPJob.uses(cordFile, LINK.INPUT);
		//preAWPJob.uses(in3dXFile, LINK.OUTPUT);
		//preAWPJob.uses(in3dYFile, LINK.OUTPUT);
		
		return preAWPJob;
	}
	
	private static Job addSmoothing(double spacing) {
		String jobname = "Smooth";
		String id = jobname + "_" + riq.getSiteName();
		Job smoothJob = new Job(id, "scec", jobname, "1.0");
		//run_smoothing.py --gridout gridout_s1252 --coords model_coords_GC_s1252 --models cca,usgs,cvmsi
		//--smoothing-dist 58 --mesh awp.s1252.media --mesh-out awp.s1252.integ_smoothed.media
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		
		gridoutFile.setTransfer(TRANSFER.FALSE);
		gridoutFile.setRegister(false);
		smoothJob.uses(gridoutFile, LINK.INPUT);
		
		File modelCoordsFile = new File("model_coords_GC_" + riq.getSiteName());
		
		modelCoordsFile.setTransfer(TRANSFER.FALSE);
		modelCoordsFile.setRegister(false);
		smoothJob.uses(modelCoordsFile, LINK.INPUT);
		
		File inputMeshFile = new File("awp." + riq.getSiteName() + ".media");
		
		inputMeshFile.setTransfer(TRANSFER.FALSE);
		inputMeshFile.setRegister(false);
		smoothJob.uses(inputMeshFile, LINK.INPUT);
		
		File outputMeshFile = new File("awp." + riq.getSiteName() + ".smoothed.media");
		
		outputMeshFile.setTransfer(TRANSFER.FALSE);
		outputMeshFile.setRegister(false);
		smoothJob.uses(outputMeshFile, LINK.OUTPUT);
		
		//Determine smoothing distance in grid points - we want it to be about 20 km
		if (spacing<0.0) {
			//Determine spacing from frequency
			double freq = riq.getLowFrequencyCutoff();
			//Assume .1km for 1.0 Hz, .2km for 0.5 
			spacing = 0.1/freq;
		}
//		int smoothingDist = (int)Math.ceil(20.0/spacing);
		//For Study 17.3 testing, go back to 10 km
		int smoothingDist = (int)Math.ceil(10.0/spacing);
		
		smoothJob.addArgument("--gridout " + gridoutFile.getName());
		smoothJob.addArgument("--coords " + modelCoordsFile.getName());
		smoothJob.addArgument("--models " + riq.getVelModelString());
		smoothJob.addArgument("--smoothing-dist " + smoothingDist);
		smoothJob.addArgument("--mesh " + inputMeshFile.getName());
		smoothJob.addArgument("--mesh-out " + outputMeshFile.getName());
				
		//smoothJob.addProfile("globus", "hostcount", "2");
//		smoothJob.addProfile("globus", "count", "64");
//		smoothJob.addProfile("pegasus", "cores", "64");
		
		return smoothJob;
	}
	
	private static Job addAWPSGTGen(String component, int[] procDims) {
		String jobname = "AWP";
		if (riq.getSgtString().equals("awp_gpu")) {
			jobname = "AWP_GPU";
		}
		String id = jobname + "_" + riq.getSiteName() + "_" + component;
		Job awpJob = new Job(id, "scec", jobname, "1.0");
		
		//Try not telling Pegasus about IN3D or awp-strain files: this way they won't get transferred between
		//titan and titan-pilot, which results in the files being zeroed
		//File in3DFile = new File("IN3D." + riq.getSiteName() + "." + component);
		String in3DFilename = "IN3D." + riq.getSiteName() + "." + component;
		//in3DFile.setTransfer(TRANSFER.FALSE);
		
		//in3DFile.setRegister(false);
		
		
		int cores = procDims[0]*procDims[1]*procDims[2];
		int hosts = (int)Math.ceil(((double)cores)/32);
		if (riq.getSgtString().equals("awp_gpu")) {
			hosts = cores;
		}
		
		if (riq.getSgtString().equals("awp")) {
			awpJob.addArgument("" + cores);
		}
		//awpJob.addArgument(in3DFile);
		awpJob.addArgument(in3DFilename);
		
//		File awpStrainFile = new File("comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component);
//		awpStrainFile.setTransfer(TRANSFER.FALSE);
//		awpStrainFile.setRegister(false);
		
//		awpJob.uses(in3DFile, LINK.INPUT);
//		awpJob.uses(awpStrainFile, LINK.OUTPUT);
		
		//awpJob.addProfile("globus", "hostcount", "" + hosts);
		//awpJob.addProfile("globus", "count", "" + cores);
		//awpJob.addProfile("pegasus", "cores", "" + cores);
		
		return awpJob;
	}
		
	private static Job addVMeshSingle(double spacing, double min_vs, double h_frac_val, String taperMode, double taperDepth, String taperModels) {
		String id = "UCVMMesh_" + riq.getSiteName();
		Job vMeshJob = new Job(id, "scec", "UCVMMesh", "1.0");
		
		File gridoutFile = new File("gridout_" + riq.getSiteName());
		File coordFile = new File("model_coords_GC_" + riq.getSiteName());
		
		vMeshJob.addArgument("--site " + riq.getSiteName());
		vMeshJob.addArgument("--gridout " + gridoutFile.getName());
		vMeshJob.addArgument("--coordfile " + coordFile.getName());
		vMeshJob.addArgument("--models " + riq.getVelModelString());
		
		String sgtType = riq.getSgtString();
		if (sgtType.contains("rwg")) {
			vMeshJob.addArgument("--format rwg");
		} else if (sgtType.contains("awp")) {
			//Use awpz if we're only selecting a subset of taper models
			if (taperModels.equals("none") || taperModels.equals("all") || taperModels.equals("")) {
				vMeshJob.addArgument("--format awp");
			} else {
				vMeshJob.addArgument("--format awpz");
			}
		} else {
			System.err.println("SGT type " + sgtType + " is not RWG or AWP.  Not sure what velocity mesh format to use, aborting.");
			System.exit(1);
		}
		
		vMeshJob.addArgument("--frequency " + riq.getLowFrequencyCutoffString());

		if (spacing>0.0) {
			vMeshJob.addArgument("--spacing " + spacing);
		}
		
		if (min_vs>0.0) {
			vMeshJob.addArgument("--min_vs " + min_vs);
		}
	
		vMeshJob.addArgument("--h_fraction " + h_frac_val);
	
		vMeshJob.addArgument("--ely-taper " + taperMode);
		
		vMeshJob.addArgument("--taper-depth " + taperDepth);
		
		vMeshJob.addArgument("--taper-models " + taperModels);
		
		gridoutFile.setTransfer(File.TRANSFER.TRUE);
		coordFile.setTransfer(File.TRANSFER.TRUE);
		
		gridoutFile.setRegister(false);
		coordFile.setRegister(false);
		
		vMeshJob.uses(gridoutFile, File.LINK.INPUT);
		vMeshJob.uses(coordFile, File.LINK.INPUT);
		
		File awpMediaFile = new File("awp." + riq.getSiteName() + ".media");
		
		
		awpMediaFile.setTransfer(TRANSFER.FALSE);
		awpMediaFile.setRegister(false);
		
		vMeshJob.uses(awpMediaFile, LINK.OUTPUT);
				
		if (riq.getSiteName().equals("TEST")) {
			vMeshJob.addProfile("globus", "hostcount", "10");
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
	
	private static Job addPostAWP(String component, boolean separateVel, boolean separateMD5, boolean smoothing) {
		String id = "PostAWP_" + riq.getSiteName() + "_" + component;
		Job postAWPJob = new Job(id, "scec", "PostAWP", "1.0");
		
		//Try not telling Pegasus about IN3D or awp-strain files: this way they won't get transferred between
		//titan and titan-pilot, which results in the files being zeroed
//		File awpStrainInFile = new File("comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component);
		String awpStrainInFilename = "comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component;
		//We swap the component value in the output file, because AWP X = RWG Y
		String rwgComponent = "z";
		if (component.equals("x")) {
			rwgComponent = "y";
		} else if (component.equals("y")) {
			rwgComponent = "x";
		}
		File awpStrainOutFile = new File(riq.getSiteName() + "_f" + rwgComponent + "_" + riq.getRunID() + ".sgt");
		File modelboxFile = new File(riq.getSiteName() + ".modelbox");
		File cordFile = new File(riq.getSiteName() + ".cordfile");
		File fdlocFile = new File(riq.getSiteName() + ".fdloc");
		File gridoutFile = new File("gridout_" + riq.getSiteName());
//		File in3DFile = new File("IN3D." + riq.getSiteName() + "." + component);
		String in3DFilename = "IN3D." + riq.getSiteName() + "." + component;
		File awpMediaFile = new File("awp." + riq.getSiteName() + ".media");
		if (separateVel) {
			awpMediaFile = new File("v_sgt-" + riq.getSiteName());
		}
		if (smoothing) {
			awpMediaFile = new File("awp." + riq.getSiteName() + ".smoothed.media");
		}
		File headerFile = new File(riq.getSiteName() + "_f" + rwgComponent + "_" + riq.getRunID() + ".sgthead");
		
//		awpStrainInFile.setTransfer(TRANSFER.TRUE);
		modelboxFile.setTransfer(TRANSFER.TRUE);
		cordFile.setTransfer(TRANSFER.TRUE);
		fdlocFile.setTransfer(TRANSFER.TRUE);
		gridoutFile.setTransfer(TRANSFER.TRUE);
//		in3DFile.setTransfer(TRANSFER.TRUE);
		//Changing to true to facilitate cleanup
		awpMediaFile.setTransfer(TRANSFER.TRUE);
		
		awpStrainOutFile.setTransfer(TRANSFER.TRUE);
		headerFile.setTransfer(TRANSFER.TRUE);
		
		if (!separateMD5) {
			File md5OutFile = new File(awpStrainOutFile.getName() + ".md5");
			md5OutFile.setTransfer(TRANSFER.TRUE);
			md5OutFile.setRegister(true);
			postAWPJob.uses(md5OutFile, LINK.OUTPUT);
		}
		
//		awpStrainInFile.setRegister(false);
		modelboxFile.setRegister(false);
		cordFile.setRegister(false);
		fdlocFile.setRegister(false);
		gridoutFile.setRegister(false);
//		in3DFile.setRegister(false);
		awpMediaFile.setRegister(false);
		
		awpStrainOutFile.setRegister(true);
		headerFile.setRegister(true);

		postAWPJob.addArgument(riq.getSiteName());
//		postAWPJob.addArgument(awpStrainInFile);
		postAWPJob.addArgument(awpStrainInFilename);
		postAWPJob.addArgument(awpStrainOutFile);
		postAWPJob.addArgument(modelboxFile);
		postAWPJob.addArgument(cordFile);
		postAWPJob.addArgument(fdlocFile);
		postAWPJob.addArgument(gridoutFile);
//		postAWPJob.addArgument(in3DFile);
		postAWPJob.addArgument(in3DFilename);
		postAWPJob.addArgument(awpMediaFile);
		postAWPJob.addArgument(component);
		postAWPJob.addArgument(riq.getRunID() + "");
		postAWPJob.addArgument(headerFile);
		postAWPJob.addArgument(riq.getLowFrequencyCutoffString());
		if (separateMD5) {
			postAWPJob.addArgument("-n");
		}
		if (riq.getSourceFrequency()!=riq.getLowFrequencyCutoff()) {
			postAWPJob.addArgument("-s " + riq.getSourceFrequency());
		}

		if (riq.getLowFrequencyCutoff()>0.5) {
			postAWPJob.addArgument(riq.getLowFrequencyCutoff() + "");
		}
		
//		postAWPJob.uses(awpStrainInFile, LINK.INPUT);
		postAWPJob.uses(awpStrainOutFile, LINK.OUTPUT);
		postAWPJob.uses(modelboxFile, LINK.INPUT);
		postAWPJob.uses(cordFile, LINK.INPUT);
		postAWPJob.uses(fdlocFile, LINK.INPUT);
		postAWPJob.uses(gridoutFile, LINK.INPUT);
//		postAWPJob.uses(in3DFile, LINK.INPUT);
		postAWPJob.uses(awpMediaFile, LINK.INPUT);
		postAWPJob.uses(headerFile, LINK.OUTPUT);
		
		return postAWPJob;
	}

	private static Job addMD5(String component) {
		String id = "MD5_" + component;
		Job md5Job = new Job(id, "scec", "MD5", "1.0");

		//We swap the component value in the output file, because AWP X = RWG Y
		String rwgComponent = "z";
		if (component.equals("x")) {
			rwgComponent = "y";
		} else if (component.equals("y")) {
			rwgComponent = "x";
		}
		File awpStrainOutFile = new File(riq.getSiteName() + "_f" + rwgComponent + "_" + riq.getRunID() + ".sgt");
		md5Job.addArgument(awpStrainOutFile.getName());
		
		File md5File = new File(awpStrainOutFile.getName() + ".md5");
		
		awpStrainOutFile.setRegister(false);
		awpStrainOutFile.setTransfer(TRANSFER.TRUE);
		md5File.setRegister(true);
		md5File.setTransfer(TRANSFER.TRUE);
		
		md5Job.uses(awpStrainOutFile, LINK.INPUT);
		md5Job.uses(md5File, LINK.OUTPUT);
		
		return md5Job;
	}

	
	private static Job addAWPNanCheck(String component) {
		String id = "AWP_NaN_Check_" + component;
		Job awpNaNJob = new Job(id, "scec", "AWP_NaN_Check", "1.0");
		
		//Try not telling Pegasus about IN3D or awp-strain files: this way they won't get transferred between
		//titan and titan-pilot, which results in the files being zeroed
//		File awpStrainInFile = new File("comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component);
//		awpStrainInFile.setTransfer(TRANSFER.TRUE);
//		awpStrainInFile.setRegister(false);
		String awpStrainInFilename = "comp_" + component + "/output_sgt/awp-strain-" + riq.getSiteName() + "-f" + component;
		
		File sgtcordFile = new File(riq.getSiteName() + ".cordfile");
		sgtcordFile.setTransfer(TRANSFER.TRUE);
		sgtcordFile.setRegister(false);
		
//		File in3DFile = new File("IN3D." + riq.getSiteName() + "." + component);
//		in3DFile.setTransfer(TRANSFER.TRUE);
//		in3DFile.setRegister(false);
		String in3DFilename = "IN3D." + riq.getSiteName() + "." + component;
		
//		awpNaNJob.addArgument(awpStrainInFile.getName());
		awpNaNJob.addArgument(awpStrainInFilename);
		awpNaNJob.addArgument(sgtcordFile.getName());
//		awpNaNJob.addArgument(in3DFile.getName());
		awpNaNJob.addArgument(in3DFilename);
//		awpNaNJob.uses(awpStrainInFile, LINK.INPUT);
		awpNaNJob.uses(sgtcordFile, LINK.INPUT);
//		awpNaNJob.uses(in3DFile, LINK.INPUT);

		return awpNaNJob;
	}
	
	private static Job addHandoff() {
		String id = "Handoff";
		Job handoffJob = new Job(id, "scec", "Handoff", "1.0");
		
		handoffJob.addArgument("-r " + riq.getRunID());
		handoffJob.addArgument("-s LF");
		
		return handoffJob;
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
	 
	 private static Job addInsertVelocityParams() {
		 String id = "InsertVelocityParams_" + riq.getSiteName();
		 Job insertJob = new Job(id, "scec", "InsertVelocityarams", "1.0");
		 
		 File velParamFile = new File(riq.getSiteName() + "_" + riq.getRunID() + "_velocity_params.txt");
		 velParamFile.setTransfer(TRANSFER.TRUE);
		 
		 insertJob.addArgument("-i " + velParamFile.getName());
		 insertJob.addArgument("-d " + "/home/shock/scottcal/runs/config/db_pass.txt");
		 insertJob.addArgument("-r " + riq.getRunID());
		 
		 insertJob.uses(velParamFile, LINK.INPUT);
		 
		 return insertJob;
	 }
}
