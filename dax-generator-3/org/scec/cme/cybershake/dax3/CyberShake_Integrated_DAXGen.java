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
    
    private final static String PP_OUTPUT_DIR_ROOT = "/home/scec-04/tera3d/CyberShake/data/PPFiles";
    
    private static ArrayList<RunIDQuery> runIDQueries;
	
	public static void main(String[] args) {
		String[] cmdLinePPArgs = parseCommandLine(args);
		
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
		sgtArgsList.add("-r");
		for (RunIDQuery riq: runIDQueries) {
			sgtArgsList.add("" + riq.getRunID());
		}
		
		String[] sgtArgs = sgtArgsList.toArray(new String[]{});

		ADAG topLevelDax = new ADAG(topLevelDAXName, 0, 1);
		
		//Print sgtArgs
		System.out.print("Calling SGT DAXGen with args: ");
		for (String a: sgtArgs) {
			System.out.print(" " + a);
		}
		System.out.println();
		
		ADAG[] sgtDAXes = CyberShake_SGT_DAXGen.subMain(sgtArgs);
		DAX[] sgtDaxJobs = new DAX[sgtDAXes.length];
		for (int i=0; i<sgtDAXes.length; i++) {
			String sgtDaxFileName = SGT_DAX_FILENAME_PREFIX + "_" + runIDQueries.get(i).getSiteName() + "_" + i + DAX_FILENAME_EXTENSION;
			sgtDAXes[i].writeToFile(sgtDaxFileName);
			sgtDaxJobs[i] = new DAX("SGT_" + runIDQueries.get(i).getSiteName(), sgtDaxFileName);
			//Avoid pruning of jobs
			sgtDaxJobs[i].addArgument("--force");
			//Force stage-out and registration
//			sgtDaxJobs[i].addArgument("--output-site bluewaters");
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
			for (String a: cmdLinePPArgs) {
				ppArgs.add(a);
			}
			String[] ppArgArray = ppArgs.toArray(new String[]{});
			System.out.print("Post-processing args: ");
			for (String a: ppArgArray) {
				System.out.print(" " + a);
			}
			System.out.println();
			CyberShake_ADAG_Container cont = CyberShake_PP_DAXGen.subMain(ppArgArray, false);
			//Set up dependencies
			
	    	String siteName = cont.getRIQ().getSiteName();
	    	
	    	//PRE workflow
	    	String preDAXFilename = cont.getFilename(cont.getPreWorkflow());
	    	DAX preD = new DAX("preDAX", preDAXFilename);
			preD.addArgument("--force");
			preD.addArgument("-q");
			//Add the dax to the top-level dax like a job
			topLevelDax.addDAX(preD);
			//Create a file object
			File preDFile = new File(preDAXFilename);
			preDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + preDAXFilename, "local");
			topLevelDax.addFile(preDFile);
//			topLevelDax.addDependency(sgtDaxJobs[i], preD);
			topLevelDax.addDependency(getJobIDJob, preD);
			
			//subWfs
			ArrayList<ADAG> subWfs = cont.getSubWorkflows();
			for (int j=0; j<subWfs.size(); j++) {
				String filename = cont.getFilename(subWfs.get(j));
				DAX jDax = new DAX("dax_" + j, filename);
				if (cont.getParams().isMPICluster()) {
					jDax.addArgument("--cluster label");
				} else {
					jDax.addArgument("--cluster horizontal");
				}
				//Makes sure it doesn't prune workflow elements
				jDax.addArgument("--force");
				jDax.addArgument("-q");
				//Force stage-out of zip files
				jDax.addArgument("--output shock");
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
			String dbDAXFile = cont.getFilename(cont.getDBWorkflow());
			DAX dbDax = new DAX("dbDax", dbDAXFile);
			dbDax.addArgument("--force");
			dbDax.addArgument("-q");
			topLevelDax.addDAX(dbDax);
			for (int j=0; j<subWfs.size(); j++) {
				topLevelDax.addDependency("dax_" + j, "dbDax");
			}	
			File dbDaxFile = new File(dbDAXFile);
			dbDaxFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + dbDAXFile, "local");
			topLevelDax.addFile(dbDaxFile);
			
			//Post
			String postDAXFile = cont.getFilename(cont.getPostWorkflow());
			DAX postD = new DAX("postDax", postDAXFile);
			postD.addArgument("--force");
			postD.addArgument("-q");
			topLevelDax.addDAX(postD);
			if (cont.getParams().getInsert()) {
				topLevelDax.addDependency(dbDax, postD);
			} else {
				for (int j=0; j<subWfs.size(); j++) {
					topLevelDax.addDependency("dax_" + j, "postDax");
				}	
			}
			File postDFile = new File(postDAXFile);
			postDFile.addPhysicalFile("file://" + cont.getParams().getPPDirectory() + "/" + postDAXFile, "local");
			topLevelDax.addFile(postDFile);

			String topLevelDaxName = DAX_FILENAME_PREFIX + "_" + siteName + DAX_FILENAME_EXTENSION;
			topLevelDax.writeToFile(topLevelDaxName);
			
			System.gc();
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

	private static String[] parseCommandLine(String[] args) {
		//Only pull out integrated options; send most along to the SGT and PP generators
        Options cmd_opts = new Options();
        Option help = new Option("h", "help", false, "Print help for CyberShake_Integrated_DAXGen");
        Option runIDFile = OptionBuilder.withArgName("runID_file").hasArg().withDescription("File containing list of Run IDs to use.").create("rf");
        Option runIDList = OptionBuilder.withArgName("runID_list").hasArgs().withDescription("List of Run IDs to use.").create("rl");
        Option postProcessingArgs = OptionBuilder.withArgName("ppargs").hasArgs().withDescription("Arguments to pass through to post-processing.").withLongOpt("ppargs").create();
        OptionGroup runIDGroup = new OptionGroup();
        runIDGroup.addOption(runIDFile);
        runIDGroup.addOption(runIDList);
        runIDGroup.setRequired(true);
        cmd_opts.addOption(help);
        cmd_opts.addOptionGroup(runIDGroup);
        cmd_opts.addOption(postProcessingArgs);

        String usageString = "CyberShake_Integrated_DAXGen <output filename> <destination directory> [options] [-f <runID file, one per line> | -r <runID1> <runID2> ... ]";
        CommandLineParser parser = new GnuParser();
        if (args.length<4) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(usageString, cmd_opts);
        	//Get help messages from SGT and PP dax generation, too
        	CyberShake_SGT_DAXGen.main(args);
        	CyberShake_PP_DAXGen.main(args);
            System.exit(1);
        }
        
        CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
            pe.printStackTrace();
            System.exit(2);
        }

        
        //Pull out run information
		if (line.hasOption(runIDFile.getOpt())) {
			runIDQueries = runIDsFromFile(line.getOptionValue(runIDFile.getOpt()));
		} else {		
			runIDQueries = runIDsFromArgs(line.getOptionValues(runIDList.getOpt()));
		}

		//Get post-processing arguments
		String[] ppArgs = new String[0];
		if (line.hasOption(postProcessingArgs.getLongOpt())) {
			ppArgs = line.getOptionValues(postProcessingArgs.getLongOpt());
		}
		return ppArgs;
	}

	private static ArrayList<RunIDQuery> runIDsFromFile(String inputFile) {
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
	
	private static ArrayList<RunIDQuery> runIDsFromArgs(String[] runIDs) {
		ArrayList<RunIDQuery> runIDQueries = new ArrayList<RunIDQuery>();
		
		for (String runID: runIDs) {
			runIDQueries.add(new RunIDQuery(Integer.parseInt(runID), false));
		}
		return runIDQueries;
	}
}
