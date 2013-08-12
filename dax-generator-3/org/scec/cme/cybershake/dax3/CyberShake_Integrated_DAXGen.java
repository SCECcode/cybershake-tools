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

/* Creates a CyberShake workflow which consists of an SGT workflow and a PP workflow.
 */
public class CyberShake_Integrated_DAXGen {
    private final static String DAX_FILENAME_PREFIX = "CyberShake_Integrated";
    private final static String SGT_DAX_FILENAME_PREFIX = "CyberShake_SGT";
    private final static String PP_DAX_FILENAME_PREFIX = "CyberShake";
    private final static String DAX_FILENAME_EXTENSION = ".dax";
    
    private static ArrayList<RunIDQuery> runIDQueries;
	
	public static void main(String[] args) {
		parseCommandLine(args);
		
		//By this point we've checked to make sure these args exist
        String daxFilename = args[0];
        String directory = args[1];
		//Construct args for SGT_DAXGen:  need to add output filename
		String[] sgtArgs = new String[args.length+1];
		sgtArgs[0] = "CyberShake_SGT.dax";
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-rl")) {
				//Translate from "rl" to "r"
				sgtArgs[i+1] = "-r";
			} else if (args[i].equals("-rf")){
				//Translate from "rf" to "f"
				sgtArgs[i+1] = "-f";
			} else {
				sgtArgs[i+1] = args[i];
			}
		}
		
		long timestamp = System.currentTimeMillis();
		ADAG topLevelDAX = new ADAG(DAX_FILENAME_PREFIX + "_" + timestamp, 0, 1);
		
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
			sgtDaxJobs[i].addArgument("--output bluewaters");
			topLevelDAX.addDAX(sgtDaxJobs[i]);
		
			File sgtDaxFile = new File(sgtDaxFileName);
			sgtDaxFile.addPhysicalFile("file://" + directory + "/" + sgtDaxFileName, "local");
			topLevelDAX.addFile(sgtDaxFile);
			
			//create post-processing workflow for each
			//Remove run argument - need to send <runID> <directory> <other args>
			ArrayList<String> ppArgs = new ArrayList<String>();
			ppArgs.add(runIDQueries.get(i).getRunID() + "");
			ppArgs.add(directory);
			int j = 2;
			while (j<args.length) {
				if (args[j].equals("-rf")) {
					//Skip this one and the next one - it's the file argument
					j += 2;
				} else if (args[j].equals("-rl")) {
					//Skip this one and each of the next until we get one with a new option argument
					j++;
					while (j<args.length && !args[j].startsWith("-")) {
						j++;
					}
				} else {
					ppArgs.add(args[j]);
					j++;
				}
			}
			String[] ppArgArray = ppArgs.toArray(new String[]{});
			System.out.print("Post-processing args: ");
			for (String a: ppArgArray) {
				System.out.print(" " + a);
			}
			System.out.println();
			ADAG topPPDax = CyberShake_PP_DAXGen.subMain(ppArgArray, false);
			//Set up dependencies
			String ppFileName = PP_DAX_FILENAME_PREFIX + "_" + runIDQueries.get(i).getSiteName() + DAX_FILENAME_EXTENSION;
			topPPDax.writeToFile(ppFileName);
			DAX topPPDaxJob = new DAX("PP_" + runIDQueries.get(i).getSiteName(), ppFileName);
			topLevelDAX.addDAX(topPPDaxJob);
			File ppDaxFile = new File(ppFileName);
			ppDaxFile.addPhysicalFile("file://" + directory + "/" + ppFileName, "local");
			topLevelDAX.addFile(ppDaxFile);
			topLevelDAX.addDependency(sgtDaxJobs[i], topPPDaxJob);
		}
		//Write topLevelDAX using command-line arg as filename
		topLevelDAX.writeToFile(daxFilename);
	}
	
	private static void parseCommandLine(String[] args) {
		//Only pull out integrated options; send most along to the SGT and PP generators
        Options cmd_opts = new Options();
        Option help = new Option("h", "help", false, "Print help for CyberShake_Integrated_DAXGen");
        Option runIDFile = OptionBuilder.withArgName("runID_file").hasArg().withDescription("File containing list of Run IDs to use.").create("rf");
        Option runIDList = OptionBuilder.withArgName("runID_list").hasArgs().withDescription("List of Run IDs to use.").create("rl");
        OptionGroup runIDGroup = new OptionGroup();
        runIDGroup.addOption(runIDFile);
        runIDGroup.addOption(runIDList);
        runIDGroup.setRequired(true);
        cmd_opts.addOptionGroup(runIDGroup);

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
