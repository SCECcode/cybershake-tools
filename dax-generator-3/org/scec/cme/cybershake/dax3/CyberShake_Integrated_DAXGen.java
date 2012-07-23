package org.scec.cme.cybershake.dax3;

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
    private final static String DAX_FILENAME_PREFIX = "CyberShake_full";
    private final static String SGT_DAX_FILENAME_PREFIX = "CyberShake_SGT";
    private final static String PP_DAX_FILENAME_PREFIX = "CyberShake";
    private final static String DAX_FILENAME_EXTENSION = ".dax";
	
	public static void main(String[] args) {
		Options cmd_opts = new Options();
		//Help
		Option help = new Option("h", "Get usage");
		cmd_opts.addOption(help);
		//SGT options
		//Velocity models
		Option cvms = new Option("v4", "Use CVM-S4 velocity model");
		Option cvmh = new Option("vh", "use CVM-H velocity model");
		OptionGroup vModelGroup = new OptionGroup();
		vModelGroup.addOption(cvms);
		vModelGroup.addOption(cvmh);
		cmd_opts.addOptionGroup(vModelGroup);
		
		//AWP
		Option awp = new Option("awp", "Use AWP-ODC-SGT to generate the SGTs");
		cmd_opts.addOption(awp);
		
		//Get run IDs
		Option runIDFile = OptionBuilder.withArgName("runID_file").hasArg().withDescription("File containing list of Run IDs to use.").create("f");
		Option runIDList = OptionBuilder.withArgName("runID_list").hasArgs().withDescription("List of Run IDs to use.").create("r");
		OptionGroup runIDGroup = new OptionGroup();
		runIDGroup.setRequired(true);
		runIDGroup.addOption(runIDFile);
		runIDGroup.addOption(runIDList);
		cmd_opts.addOptionGroup(runIDGroup);
		
		//PP options
		Option partition = OptionBuilder.withArgName("num_partitions").hasArg().withDescription("Number of partitions to create.").create("p");
		cmd_opts.addOption(partition);
		Option no_insert = new Option("noinsert", "Don't insert ruptures into database (used for testing)");
		cmd_opts.addOption(no_insert);
		Option sqlIndex = new Option("sql", "Create sqlite file containing (source, rupture, rv) to sub workflow mapping");
		cmd_opts.addOption(sqlIndex);
		Option memcached = new Option("mc", "memcached", false, "Use memcached implementation of all executables, if available");
		cmd_opts.addOption(memcached);
		Option rv_mem = new Option("rm", "rv_mem", false, "Use in-memory rupture variation implementation of all executables, if available");
		cmd_opts.addOption(rv_mem);
		Option mpi_cluster = new Option("mpi_cluster", "Use pegasus-mpi-cluster");
		cmd_opts.addOption(mpi_cluster);
		Option no_zip = new Option("nz", "No zip jobs (transfer files individually, zip after transfer)");
		cmd_opts.addOption(no_zip);
		
		//Low-frequency options
		Option seisPSA = new Option("sp", "seis_psa", false, "Use a single executable for both synthesis and PSA");
		cmd_opts.addOption(seisPSA);
		
		//High-frequency options
		Option high_frequency = OptionBuilder.withArgName("frequency_cutoff").hasOptionalArg().withDescription("Lower cutoff in Hz for stochastic high-frequency seismograms (default 1.0)").create("hf");
		cmd_opts.addOption(high_frequency);
		Option hf_synth = new Option("hs", "hf_synth", false, "Use a single executable for high-frequency srf2stoch and hfsim");
		cmd_opts.addOption(hf_synth);
		Option merge_psa = new Option("mp", "merge_psa", false, "Use a single executable for merging broadband seismograms and PSA");
		cmd_opts.addOption(merge_psa);

		CommandLineParser parser = new GnuParser();
		CommandLine line = null;
        try {
            line = parser.parse(cmd_opts, args);
        } catch (ParseException pe) {
//        	System.err.println(pe.getMessage());
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp("CyberShake_Integrated_DAXGen", cmd_opts);
        	System.exit(1);
            System.exit(2);
        }
		
        if (args.length < 3 || line.hasOption(help.getOpt())) {
        	HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp("CyberShake_Integrated_DAXGen", cmd_opts);
        	System.exit(1);
        }
        
		String outputFilename = args[0]; 
		String directory = args[1];
		String velocityModel = "1D";
		if (line.hasOption(cvms.getOpt())) {
			velocityModel = "cvms";
		} else if (line.hasOption(cvmh.getOpt())) {
			velocityModel = "cvmh";
		} else {
			System.err.println("Must select a velocity model, cvms or cvmh.");
			System.exit(3);
		}
		
		boolean awp_bool = false;
		if (line.hasOption(awp.getOpt())) {
			awp_bool = true;
		}
		
		ArrayList<RunIDQuery> runIDQueries = null;
		
		if (line.hasOption(runIDFile.getOpt())) {
			runIDQueries = CyberShake_SGT_DAXGen.runIDsFromFile(awp, line.getOptionValue(runIDFile.getOpt()));
		} else {		
			runIDQueries = CyberShake_SGT_DAXGen.runIDsFromArgs(awp, line.getOptionValues(runIDList.getOpt()));
		}
		
		//SGTs
		ADAG[] sgtDAXes = CyberShake_SGT_DAXGen.makeWorkflows(runIDQueries, velocityModel, awp_bool, directory, outputFilename);
        
		
		long timeStamp = System.currentTimeMillis();
		ADAG topLevelDAX = new ADAG(DAX_FILENAME_PREFIX + "_" + timeStamp + DAX_FILENAME_EXTENSION);
		DAX[] sgtDaxJobs = new DAX[sgtDAXes.length];
		for (int i=0; i<sgtDAXes.length; i++) {
			String daxFileName = SGT_DAX_FILENAME_PREFIX + "_" + runIDQueries.get(i).getSiteName() + "_" + i + ".dax";
			sgtDAXes[i].writeToFile(daxFileName);
			sgtDaxJobs[i] = new DAX("SGT_" + runIDQueries.get(i).getSiteName(), daxFileName);
			//Avoid pruning of jobs
			sgtDaxJobs[i].addArgument("--force");
			//Copy results to ranger unpurged directory
//			sgtDaxJob.addArgument("-o ranger");
			topLevelDAX.addDAX(sgtDaxJobs[i]);
		
			File sgtDaxFile = new File(daxFileName);
			sgtDaxFile.addPhysicalFile("file://" + directory + "/" + daxFileName, "local");
			topLevelDAX.addFile(sgtDaxFile);
		}
		
		//Post-processing
        CyberShake_PP_DAXGen daxGen = new CyberShake_PP_DAXGen();
        PP_DAXParameters pp_params = new PP_DAXParameters();

        pp_params.setPPDirectory(directory);
        if (line.hasOption(partition.getOpt())) {
            pp_params.setNumOfDAXes(Integer.parseInt(line.getOptionValue("p")));
        }

        if (line.hasOption(no_insert.getOpt())) {
        	pp_params.setInsert(false);
        }
        
        if (line.hasOption(seisPSA.getOpt())) {
        	pp_params.setMergedExe(true);
        }
        
        if (line.hasOption(memcached.getOpt())) {
        	pp_params.setUseMemcached(true);
        	pp_params.setUseMergedMemcached(true);
        }
        
        if (line.hasOption(high_frequency.getOpt())) {
        	if (pp_params.isMergedExe()) {
        		System.out.println("Only 1 of -mr, -hf option is supported at this time.");
        		System.exit(3);
        	} else if (pp_params.isMergedMemcached()) {
        		System.out.println("Only 1 of -mmr, -hf option is supported at this time.");
        		System.exit(3);
        	}
        	pp_params.setHighFrequency(true);
        	if (line.getOptionValue("hf")!=null) {
        		pp_params.setHighFrequencyCutoff(Double.parseDouble(line.getOptionValue("hf")));
        	} else {
        		//use 1.0 as default
        		pp_params.setHighFrequencyCutoff(1.0);
        	}
        	if (line.hasOption(hf_synth.getOpt())) {
        		pp_params.setHfsynth(true);
        	}
        	if (line.hasOption(merge_psa.getOpt())) {
        		pp_params.setMergePSA(true);
        	}
        } else {
        	//only running low frequency
    		pp_params.setHighFrequencyCutoff(0.5);	
        }
        if (line.hasOption(sqlIndex.getOpt())) {
        	pp_params.setRvDB(true);
        }
        
        if (line.hasOption(rv_mem.getOpt())) {
        	if (pp_params.isHfsynth()==false && pp_params.isHighFrequency()==true) {
        		System.err.println("Can't use in-memory rupture variations in HF_Synth if you're not running high frequency with HF_Synth.");
        		System.exit(-4);
        	}
        	pp_params.setHfsynthRVMem(true);
        	pp_params.setJbsimRVMem(true);
        }
        if (line.hasOption(awp.getOpt())) {
        	pp_params.setUseAWP(true);
        }
        
        if (line.hasOption(mpi_cluster.getOpt())) {
        	pp_params.setMPICluster(true);
        }
        if (line.hasOption(no_zip.getOpt())) {
        	pp_params.setZip(false);
        }
        
        for (int i=0; i<runIDQueries.size(); i++) {
        	ADAG ppADAX = daxGen.makeDAX(runIDQueries.get(i).getRunID(), pp_params);
        	
			String daxFile = PP_DAX_FILENAME_PREFIX + "_" + runIDQueries.get(i).getSiteName() + DAX_FILENAME_EXTENSION;
			ppADAX.writeToFile(daxFile);
			//Add to topLevelDax
			DAX ppDax = new DAX("dax_" + runIDQueries.get(i).getSiteName(), daxFile);
			//Makes sure it doesn't prune workflow elements
			ppDax.addArgument("--force");
			ppDax.addArgument("-qqqqq");
			//Force stage-out of zip files
			ppDax.addArgument("--output shock");
			ppDax.addProfile("dagman", "category", "ppwf");
			topLevelDAX.addDAX(ppDax);
			//Dependencies between SGT dax and PP dax
			topLevelDAX.addDependency(sgtDaxJobs[i], ppDax);
			File ppDaxFile = new File(daxFile);
			ppDaxFile.addPhysicalFile("file://" + directory + "/" + daxFile, "local");
			topLevelDAX.addFile(ppDaxFile);
        }
		topLevelDAX.writeToFile(outputFilename);
	}
	
}
