package org.scec.cme.cybershake.dax3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
import edu.isi.pegasus.planner.dax.AbstractJob;
import edu.isi.pegasus.planner.dax.DAX;
import edu.isi.pegasus.planner.dax.File;
import edu.isi.pegasus.planner.dax.Job;

/* Creates a CyberShake workflow which consists of an SGT workflow and a PP workflow.
 */
public class CyberShake_Integrated_DAXGen {
    private final static String DAX_FILENAME_PREFIX = "CyberShake_Integrated";
    private final static String SGT_DAX_FILENAME_PREFIX = "CyberShake_SGT";
    private final static String PP_DAX_FILENAME_PREFIX = "CyberShake";
    private final static String DAX_FILENAME_EXTENSION = ".dax";
    
    private final static String SGT_OUTPUT_DIR_ROOT = "/gpfs/alpine/scratch/callag/geo112/SgtFiles";
    private final static String PP_OUTPUT_DIR_ROOT = "/project/scec_608/cybershake/results/PPFiles";

	private final static String NAMESPACE = "scec";
    
    private final static String UPDATERUN_NAME = "UpdateRun";
    
    private static boolean runBroadband = false;
    private static String bbRunID = "-1";
    
    private static String servername = "moment.usc.edu";
    
    private static ArrayList<RunIDQuery> runIDQueries;
	
	public static void main(String[] args) {
		ArrayList<String[]> cmdLineSubArgs = parseCommandLine(args);
		
		long timestamp = System.currentTimeMillis();
		//By this point we've checked to make sure these args exist
        String daxFilename = args[0];
        String directory = args[1];
        String topLevelDAXName = DAX_FILENAME_PREFIX + "_" + timestamp;
		//Construct args for SGT_DAXGen:  need to add output filename
		ArrayList<String> sgtArgsList = new ArrayList<String>();
		if (runIDQueries.size()>1) {
			sgtArgsList.add("CyberShake_SGT_" + timestamp + ".dax");
		} else {
			sgtArgsList.add("CyberShake_SGT_" + runIDQueries.get(0).getSiteName() + ".dax");
			topLevelDAXName = DAX_FILENAME_PREFIX + "_" + runIDQueries.get(0).getSiteName();
		}
		sgtArgsList.add(directory);
		//Add --sgtargs arguments
		if (cmdLineSubArgs.get(0)!=null) {
			for (String a: cmdLineSubArgs.get(0)) {
				sgtArgsList.add(a);
			}
		}

		//Add run ID arguments
		sgtArgsList.add("-r");
		for (RunIDQuery riq: runIDQueries) {
			sgtArgsList.add("" + riq.getRunID());
		}
		//Pass server argument through
		sgtArgsList.add("--server");
		sgtArgsList.add(servername);
		
		String[] sgtArgs = sgtArgsList.toArray(new String[]{});

		ADAG topLevelDax = new ADAG(topLevelDAXName, 0, 1);
		
		//Print sgtArgs
		System.out.print("Calling SGT DAXGen with args: ");
		for (String a: sgtArgs) {
			System.out.print(" " + a);
		}
		System.out.println();

		if (runIDQueries.size()==1) {
			//If only 1 site, then remove a layer of the hierarchy and put SGT jobs right into top-level DAX
			
			//Only 1 arrayList since there's only 1 site, so use that
			ArrayList<AbstractJob> jobRefs = CyberShake_SGT_DAXGen.subMain(sgtArgs, topLevelDax).get(0);
			//Get the update job
			AbstractJob lastJob = null;
			ArrayList<AbstractJob> sgtJobs = new ArrayList<AbstractJob>();
			for (AbstractJob job : jobRefs) {
				if (runIDQueries.get(0).getSgtString().contains("awp")) {
					//there is no update job, since in AWP we have the SGT generation job instead
					if (job.getName().equals("AWP_SGT_" + runIDQueries.get(0).getSiteName() + ".dax")) {
						lastJob = job;
					}
				} else if (job.getName().contains("Update")) {
					lastJob = job;
				} else if (job.getName().contains("Merge")) {
					sgtJobs.add(job);
				}
			}
			
			
			//Create get job ID job - since the last job in the SGT workflow clears out the Job ID
			Job getJobIDJob = createJobIDJob(runIDQueries.get(0), directory);
			topLevelDax.addJob(getJobIDJob);
			topLevelDax.addDependency(lastJob, getJobIDJob);
			
			//create post-processing workflow for each
			//Remove run argument - need to send <runID> <directory> <other args>
			ArrayList<String> ppArgs = new ArrayList<String>();
			ppArgs.add(runIDQueries.get(0).getRunID() + "");
			ppArgs.add(directory);
			for (String a: cmdLineSubArgs.get(1)) {
				ppArgs.add(a);
			}
			ppArgs.add("--server");
			ppArgs.add(servername);
			String[] ppArgArray = ppArgs.toArray(new String[]{});
			System.out.print("Post-processing args: ");
			for (String a: ppArgArray) {
				System.out.print(" " + a);
			}
			System.out.println();
			
			CyberShake_Workflow_Container cont = CyberShake_PP_DAXGen.subMain(ppArgArray, false);
			
			//Create BB workflow, if requested
			if (runBroadband) {
				//CyberShake_Stochastic_DAXGen <runID> <directory> <low-frequency id>;
				ArrayList<String> bbArgs = new ArrayList<String>();
				bbArgs.add(bbRunID);
				bbArgs.add(directory);
				bbArgs.add(runIDQueries.get(0).getRunID() + "");
				for (String a: cmdLineSubArgs.get(2)) {
					bbArgs.add(a);
				}
				bbArgs.add("--server");
				bbArgs.add(servername);
				
				cont.setBBWorkflow(
						CyberShake_Stochastic_DAXGen.subMain(bbArgs.toArray(new String[]{})));
			}

			//Set up dependencies
			
	    	String siteName = cont.getRIQ().getSiteName();
	    	
	    	//PRE workflow
	    	String preDAXFilename = cont.getPreWorkflow();
	    	DAX preD = new DAX(cont.getRIQ().getSiteName() + "_preDAX", preDAXFilename);
			//Combine arguments into one to deal with 5.0 planning bug
			/*preD.addArgument("--force");
			preD.addArgument("-q");
			preD.addArgument("--cleanup none");
			if (cont.getParams().getPPSite()!=null) {
				preD.addArgument("-s " + cont.getParams().getPPSite() + ",shock,local");
			}*/
			StringBuffer argString = new StringBuffer("--force");
			argString.append(" -q");
			argString.append(" --cleanup none");
			if (cont.getParams().getPPSite()!=null) {
            	argString.append(" -s " + cont.getParams().getPPSite() + ",shock,local");
            }
			preD.addArgument(argString.toString());
			//Add the dax to the top-level dax like a job
			topLevelDax.addDAX(preD);
			//Create a file object
			File preDFile = new File(preDAXFilename);
			preDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + preDAXFilename, "local");
			topLevelDax.addFile(preDFile);
			topLevelDax.addDependency(getJobIDJob, preD);
			//If we didn't use the AWP sub-SGT workflow, then the SGT jobs are in the top-level wf, and the PRE workflow needs to be a child of them to pick up the SGT files
			if (runIDQueries.get(0).getSgtString().contains("rwg")) {
				for (AbstractJob sgtJob : sgtJobs) {
					topLevelDax.addDependency(sgtJob, preD);
				}
			} else {
				//Otherwise, make this dependent on the AWP sub-SGT workflow so we pick up the SGT files correctly
				topLevelDax.addDependency(lastJob, preD);
			}
			
			//subWfs
			ArrayList<String> subWfs = cont.getSubWorkflows();
			for (int j=0; j<subWfs.size(); j++) {
				String filename = subWfs.get(j);
				DAX jDax = new DAX(cont.getRIQ().getSiteName() + "_dax_" + j, filename);
				//Create single arg string for Pegasus 5.0 issue
				argString = new StringBuffer("");
				if (cont.getParams().isMPICluster()) {
					//jDax.addArgument("--cluster label");
					argString.append("--cluster label");
				} else {
					//jDax.addArgument("--cluster horizontal");
					argString.append("--cluster horizontal");
				}
				//Makes sure it doesn't prune workflow elements
				/*jDax.addArgument("--force");
				jDax.addArgument("-q");
				jDax.addArgument("--cleanup none");*/
				argString.append(" --force");
				argString.append(" -q");
				argString.append(" --cleanup none");
				if (cont.getParams().getPPSite()!=null) {
					//jDax.addArgument("-s " + cont.getParams().getPPSite());
					argString.append(" -s " + cont.getParams().getPPSite());
				}
				//Force stage-out of zip files
				//jDax.addArgument("--output-sites shock");
				argString.append(" --output-sites shock");
				//jDax.addArgument("--output-dir " + PP_OUTPUT_DIR_ROOT + "/" + siteName + "/" + cont.getRIQ().getRunID());
				argString.append(" --output-dir " + PP_OUTPUT_DIR_ROOT + "/" + siteName + "/" + cont.getRIQ().getRunID());
				jDax.addArgument(argString.toString());
				jDax.addProfile("dagman", "category", "subwf");
				topLevelDax.addDAX(jDax);
				//Only add a dependency if we're not using the no-blocking MD5 sums
				if (cont.getParams().isNonblockingMD5()==false) {
					topLevelDax.addDependency(preD, jDax);
				} else {
					//Add dependencies on job ID job, since we don't want these to start until it's post-processing time
					topLevelDax.addDependency(getJobIDJob, jDax);
					//Also add dependency on AWP workflow job, so we pick up the SGTs appropriately
					topLevelDax.addDependency(lastJob, jDax);
				}
				File jDaxFile = new File(filename);
				jDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + filename, "local");
				topLevelDax.addFile(jDaxFile);
			}
			//BB if we have it
			DAX bbDax = null;
			if (runBroadband) {
				/*Because we're running at the same time, it has its own Run ID
				* but the state is stuck in SGT_INIT/Initial
				* Must update to PP_INIT so that the stochastic workflow's update job succeeds
				* Have to do this as SGT_INIT->SGT_START->PP_INIT
				*/
				
				//SGT_INIT->SGT_START
		    	String id = UPDATERUN_NAME + bbRunID + "_SGT_START";
		    	Job updateJob0 = new Job(id, NAMESPACE, UPDATERUN_NAME, "1.0");
			    updateJob0.addArgument(bbRunID);
			    updateJob0.addArgument("SGT_INIT");
		    	updateJob0.addArgument("SGT_START");
		    	topLevelDax.addJob(updateJob0);
		    	topLevelDax.addDependency(preD, updateJob0);
		    	//SGT_START->PP_INIT
		    	id = UPDATERUN_NAME + bbRunID + "_PP_INIT";
		    	Job updateJob1 = new Job(id, NAMESPACE, UPDATERUN_NAME, "1.0");
			    updateJob1.addArgument(bbRunID);
			    updateJob1.addArgument("SGT_START");
		    	updateJob1.addArgument("PP_INIT");
		    	topLevelDax.addJob(updateJob1);
		    	topLevelDax.addDependency(updateJob0, updateJob1);
				
				String bbDAXFile = cont.getBBWorkflow();
				String bbDAXName = cont.getRIQ().getSiteName() + "_stochDax";
				bbDax = new DAX(bbDAXName, bbDAXFile);
				//Makes sure it doesn't prune workflow elements
				argString = new StringBuffer("");
				//bbDax.addArgument("--force");
				//bbDax.addArgument("-q");
				//bbDax.addArgument("--cleanup none");
				argString.append("--force");
				argString.append(" -q");
				argString.append(" --cleanup none");
				bbDax.addArgument(argString.toString());
				topLevelDax.addDAX(bbDax);
				//Broadband workflow is dependent on post-processing and the update job
				for (int j=0; j<subWfs.size(); j++) {
					topLevelDax.addDependency(cont.getRIQ().getSiteName() + "_dax_" + j,  bbDAXName);
				}
				topLevelDax.addDependency(updateJob1.getId(), bbDAXName);
				File bbDaxFile = new File(bbDAXFile);
				bbDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + bbDAXFile, "local");
				topLevelDax.addFile(bbDaxFile);
			}
			
			//DB
			String dbDAXFile = cont.getDBWorkflow();
			DAX dbDax = new DAX(cont.getRIQ().getSiteName() + "_dbDax", dbDAXFile);
			argString = new StringBuffer("");
			//dbDax.addArgument("--force");
			//dbDax.addArgument("-q");
			//dbDax.addArgument("--cleanup none");
			argString.append(" --force");
			argString.append(" -q");
			argString.append(" --cleanup none");
			dbDax.addArgument(argString.toString());
			topLevelDax.addDAX(dbDax);
			for (int j=0; j<subWfs.size(); j++) {
				topLevelDax.addDependency(cont.getRIQ().getSiteName() + "_dax_" + j, cont.getRIQ().getSiteName() + "_dbDax");
			}	
			File dbDaxFile = new File(dbDAXFile);
			dbDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + dbDAXFile, "local");
			topLevelDax.addFile(dbDaxFile);
			
			//Post
			String postDAXFile = cont.getPostWorkflow();
			DAX postD = new DAX(cont.getRIQ().getSiteName() + "_postDax", postDAXFile);
			argString = new StringBuffer("");
			//postD.addArgument("--force");
			//postD.addArgument("-q");
			//postD.addArgument("--cleanup none");
			argString.append(" --force");
			argString.append(" -q");
			argString.append(" --cleanup none");
			postD.addArgument(argString.toString());
			topLevelDax.addDAX(postD);
			if (cont.getParams().getInsert()) {
				topLevelDax.addDependency(dbDax, postD);
				if (runBroadband) {
					topLevelDax.addDependency(bbDax, postD);
				}
			} else {
				for (int j=0; j<subWfs.size(); j++) {
					topLevelDax.addDependency(cont.getRIQ().getSiteName() + "_dax_" + j, cont.getRIQ().getSiteName() + "_postDax");
				}	
			}
			File postDFile = new File(postDAXFile);
			postDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + postDAXFile, "local");
			topLevelDax.addFile(postDFile);

			String topLevelDaxName = DAX_FILENAME_PREFIX + "_" + siteName + DAX_FILENAME_EXTENSION;
			topLevelDax.writeToFile(topLevelDaxName);
			
		} else {
			//Multiple CyberShake sites in wf, need extra level of hierarchy
		
			ADAG[] sgtDAXes = CyberShake_SGT_DAXGen.subMain(sgtArgs);
			DAX[] sgtDaxJobs = new DAX[sgtDAXes.length];
			for (int i=0; i<sgtDAXes.length; i++) {
				String sgtDaxFileName = SGT_DAX_FILENAME_PREFIX + "_" + runIDQueries.get(i).getSiteName() + "_" + i + DAX_FILENAME_EXTENSION;
				sgtDAXes[i].writeToFile(sgtDaxFileName);
				sgtDaxJobs[i] = new DAX("SGT_" + runIDQueries.get(i).getSiteName(), sgtDaxFileName);
				//Avoid pruning of jobs
				sgtDaxJobs[i].addArgument("--force");
				//Force stage-out and registration
				sgtDaxJobs[i].addArgument("--output-sites bluewaters");
				sgtDaxJobs[i].addArgument("--output-dir " + SGT_OUTPUT_DIR_ROOT + "/" + runIDQueries.get(i).getSiteName());
				topLevelDax.addDAX(sgtDaxJobs[i]);

				File sgtDaxFile = new File(sgtDaxFileName);
				sgtDaxFile.addPhysicalFile("file://" + directory + "/" + sgtDaxFileName, "local");
				topLevelDax.addFile(sgtDaxFile);

				//Create get job ID job - since the last job in the SGT workflow clears out the Job ID
				Job getJobIDJob = createJobIDJob(runIDQueries.get(i), directory);
				topLevelDax.addJob(getJobIDJob);
				topLevelDax.addDependency(sgtDaxJobs[i], getJobIDJob);

				//create post-processing workflow for each
				//Remove run argument - need to send <runID> <directory> <other args>
				ArrayList<String> ppArgs = new ArrayList<String>();
				ppArgs.add(runIDQueries.get(i).getRunID() + "");
				ppArgs.add(directory);
				for (String a: cmdLineSubArgs.get(1)) {
					ppArgs.add(a);
				}
				String[] ppArgArray = ppArgs.toArray(new String[]{});
				System.out.print("Post-processing args: ");
				for (String a: ppArgArray) {
					System.out.print(" " + a);
				}
				System.out.println();
				CyberShake_Workflow_Container cont = CyberShake_PP_DAXGen.subMain(ppArgArray, false);
				//Set up dependencies

				String siteName = cont.getRIQ().getSiteName();

				//PRE workflow
				String preDAXFilename = cont.getPreWorkflow();
				DAX preD = new DAX(cont.getRIQ().getSiteName() + "_preDAX", preDAXFilename);
				preD.addArgument("--force");
				preD.addArgument("-q");
				//Add the dax to the top-level dax like a job
				topLevelDax.addDAX(preD);
				//Create a file object
				File preDFile = new File(preDAXFilename);
				preDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + preDAXFilename, "local");
				topLevelDax.addFile(preDFile);
				topLevelDax.addDependency(sgtDaxJobs[i], preD);
				topLevelDax.addDependency(getJobIDJob, preD);

				//subWfs
				ArrayList<String> subWfs = cont.getSubWorkflows();
				for (int j=0; j<subWfs.size(); j++) {
					String filename = subWfs.get(j);
					DAX jDax = new DAX(cont.getRIQ().getSiteName() + "_dax_" + j, filename);
					if (cont.getParams().isMPICluster()) {
						jDax.addArgument("--cluster label");
					} else {
						jDax.addArgument("--cluster horizontal");
					}
					//Makes sure it doesn't prune workflow elements
					jDax.addArgument("--force");
					jDax.addArgument("-q");
					//Force stage-out of zip files
					jDax.addArgument("--output-sites shock");
					jDax.addArgument("--output-dir " + PP_OUTPUT_DIR_ROOT + "/" + siteName + "/" + cont.getRIQ().getRunID());
					jDax.addProfile("dagman", "category", "subwf");
					topLevelDax.addDAX(jDax);
					topLevelDax.addDependency(preD, jDax);
					File jDaxFile = new File(filename);
					jDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + filename, "local");
					topLevelDax.addFile(jDaxFile);
					topLevelDax.addDependency(sgtDaxJobs[i], jDax);
				}

				//DB
				String dbDAXFile = cont.getDBWorkflow();
				DAX dbDax = new DAX(cont.getRIQ().getSiteName() + "_dbDax", dbDAXFile);
				dbDax.addArgument("--force");
				dbDax.addArgument("-q");
				topLevelDax.addDAX(dbDax);
				for (int j=0; j<subWfs.size(); j++) {
					topLevelDax.addDependency(cont.getRIQ().getSiteName() + "_dax_" + j, cont.getRIQ().getSiteName() + "_dbDax");
				}	
				File dbDaxFile = new File(dbDAXFile);
				dbDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + dbDAXFile, "local");
				topLevelDax.addFile(dbDaxFile);

				//Post
				String postDAXFile = cont.getPostWorkflow();
				DAX postD = new DAX(cont.getRIQ().getSiteName() + "_postDax", postDAXFile);
				postD.addArgument("--force");
				postD.addArgument("-q");
				topLevelDax.addDAX(postD);
				if (cont.getParams().getInsert()) {
					topLevelDax.addDependency(dbDax, postD);
				} else {
					for (int j=0; j<subWfs.size(); j++) {
						topLevelDax.addDependency(cont.getRIQ().getSiteName() + "_dax_" + j, cont.getRIQ().getSiteName() + "_postDax");
					}	
				}
				File postDFile = new File(postDAXFile);
				postDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + postDAXFile, "local");
				topLevelDax.addFile(postDFile);

				String topLevelDaxName = DAX_FILENAME_PREFIX + "_" + siteName + DAX_FILENAME_EXTENSION;
				topLevelDax.writeToFile(topLevelDaxName);

				System.gc();
			}
		}
		//Write topLevelDAX using command-line arg as filename
		topLevelDax.writeToFile(daxFilename);
	}
	
	private static Job createJobIDJob(RunIDQuery riq, String directory) {
		String id = "SetJobID_" + riq.getSiteName() + "_" + riq.getRunID();
		Job jobID = new Job(id, "scec", "SetJobID", "1.0");

		jobID.addArgument(directory);
		jobID.addArgument("" + riq.getRunID());
		
		return jobID;
	}

	private static ArrayList<String[]> parseCommandLine(String[] args) {
		//Only pull out integrated options; send most along to the SGT and PP generators
        Options cmd_opts = new Options();
        Option help = new Option("h", "help", false, "Print help for CyberShake_Integrated_DAXGen");
        Option runIDFile = OptionBuilder.withArgName("runID_file").hasArg().withDescription("File containing list of Run IDs to use.").create("rf");
        Option runIDList = OptionBuilder.withArgName("runID_list").hasArgs().withDescription("List of Run IDs to use.").create("rl");
        Option sgtArgs = OptionBuilder.withArgName("sgtargs").hasArgs().withDescription("Arguments to pass through to SGT workflow.").withLongOpt("sgtargs").create();
        Option postProcessingArgs = OptionBuilder.withArgName("ppargs").hasArgs().withDescription("Arguments to pass through to post-processing.").withLongOpt("ppargs").create();
        Option server = OptionBuilder.withArgName("server").hasArg().withDescription("Server to use for site parameters and to insert PSA values into").withLongOpt("server").create();
        Option broadband = new Option("bb", "broadband", false, "Run stochastic high-frequency calculations in addition to deterministic low-frequency.");
        Option broadbandArgs = OptionBuilder.withArgName("bbargs").hasArgs().withDescription("Arguments to pass through to BB workflow.").withLongOpt("bbargs").create();
        Option broadbandID = OptionBuilder.withArgName("bbrun").hasArg().withDescription("Run ID for stochastic results.").withLongOpt("broadband-runid").create();
        Option force_vs30 = OptionBuilder.withArgName("fvs30").hasArg().withDescription("Force Target_Vs30 value.").withLongOpt("force_vs30").create("fvs");
        OptionGroup runIDGroup = new OptionGroup();
        runIDGroup.addOption(runIDFile);
        runIDGroup.addOption(runIDList);
        runIDGroup.setRequired(true);
        cmd_opts.addOption(help);
        cmd_opts.addOptionGroup(runIDGroup);
        cmd_opts.addOption(postProcessingArgs);
        cmd_opts.addOption(sgtArgs);
        cmd_opts.addOption(server);
        cmd_opts.addOption(broadband);
        cmd_opts.addOption(broadbandArgs);
        cmd_opts.addOption(broadbandID);
        cmd_opts.addOption(force_vs30);

        String usageString = "CyberShake_Integrated_DAXGen <output filename> <destination directory> [options] [-f <runID file, one per line> | -r <runID1> <runID2> ... ]";
        CommandLineParser parser = new GnuParser();
        if (args.length<4) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	//Get help messages from SGT and PP dax generation, too
        	System.out.println("SGT DAX Generator arguments:");
        	CyberShake_SGT_DAXGen.parseCommandLine(new String[]{"-h"});
        	System.out.println("PP DAX Generator arguments:");
        	CyberShake_PP_DAXGen.parseCommandLine(new String[]{"-h"}, null);
        	System.out.println("Stochastic DAX Generator arguments:");
        	CyberShake_Stochastic_DAXGen.parseCommandLine(new String[]{"-h"});
            System.exit(1);
        }
        
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(2);
        }
        
        if (line.hasOption(server.getLongOpt())) {
        	servername = line.getOptionValue(server.getLongOpt());
        }
        
        //Pull out run information
		if (line.hasOption(runIDFile.getOpt())) {
			runIDQueries = runIDsFromFile(line.getOptionValue(runIDFile.getOpt()));
		} else {		
			runIDQueries = runIDsFromArgs(line.getOptionValues(runIDList.getOpt()));
		}
		
		ArrayList<String[]> subArgs = new ArrayList<String[]>();
		
		String[] sgtArgStrings = null;
		if (line.hasOption(sgtArgs.getLongOpt())) {
			sgtArgStrings = line.getOptionValues(sgtArgs.getLongOpt());
		}
		subArgs.add(sgtArgStrings);
		
		//Get post-processing arguments
		String[] ppArgs = null;
		if (line.hasOption(postProcessingArgs.getLongOpt())) {
			ppArgs = line.getOptionValues(postProcessingArgs.getLongOpt());
			for (String p : ppArgs ) {
				System.out.println(p);
			}
		}
		
		subArgs.add(ppArgs);
		
		if (line.hasOption(broadband.getOpt())) {
			runBroadband = true;
		}
		
		if (runBroadband) {
			if (line.hasOption(broadbandArgs.getLongOpt())) {
				String[] bbArgs = line.getOptionValues(broadbandArgs.getLongOpt());
				//Check to see if force vs30 option needs to be added to BB args
				if (line.hasOption(force_vs30.getOpt())) {
					double vs30 = Double.parseDouble(line.getOptionValue(force_vs30.getLongOpt()));
					String[] tmpBBArgs = new String[bbArgs.length + 2];
					for (int i=0; i<bbArgs.length; i++) {
						tmpBBArgs[i] = bbArgs[i];
					}
					tmpBBArgs[bbArgs.length] = "-fvs";
					tmpBBArgs[bbArgs.length+1] = "" + vs30;
					subArgs.add(tmpBBArgs);
				} else {
					subArgs.add(bbArgs);
				}
			}
			if (line.hasOption(broadbandID.getLongOpt())) {
				bbRunID = line.getOptionValue(broadbandID.getLongOpt());
			}
		}
		
		if (line.hasOption(force_vs30.getOpt())) {
			double vs30 = Double.parseDouble(line.getOptionValue(force_vs30.getLongOpt()));
			for (RunIDQuery r: runIDQueries) {
				r.setVs30(vs30);
			}
		}
		
		return subArgs;
	}

	private static ArrayList<RunIDQuery> runIDsFromFile(String inputFile) {
		ArrayList<RunIDQuery> runIDQueries = new ArrayList<RunIDQuery>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String entry = br.readLine();
			while (entry!=null) {
				runIDQueries.add(new RunIDQuery(Integer.parseInt(entry), servername));
			}
			br.close();
		} catch (IOException iex) {
			iex.printStackTrace();
			System.exit(1);
		}
		
		return runIDQueries;
	}
	
	private static ArrayList<RunIDQuery> runIDsFromArgs(String[] runIDs) {
		ArrayList<RunIDQuery> runIDQueries = new ArrayList<RunIDQuery>();
		
		for (String runID: runIDs) {
			runIDQueries.add(new RunIDQuery(Integer.parseInt(runID), servername));
		}
		return runIDQueries;
	}
}
