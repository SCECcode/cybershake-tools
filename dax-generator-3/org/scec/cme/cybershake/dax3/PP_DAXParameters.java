package org.scec.cme.cybershake.dax3;

public class PP_DAXParameters {
	private static final int MAX_NUM_OF_DAXES = 100;
	private static final int MAX_NUM_EMAILS = 5;
	
	private int numOfDAXes;
	private int currentSGTRep;
	private boolean databaseInsert;

	//Defaults
	private boolean seisPSAexe = true;
	private boolean stochastic = false;
	//Maximum deterministic frequency
	private double detFrequency = 0.5;
	private double stochasticCutoff = 0.0;
	private double stochasticFrequency = 10.0;
	private boolean hfsynth = true;
	private boolean mergePSA = true;
	
	private boolean jbsimRVMem = true;
	private boolean hfsynthRVMem = true;
	private boolean largeMemSynth = false;
		
	private int numVarsPerDAX;
	private int maxVarsPerDAX;
	
	private int notifyGroupSize;
	
	private String ppDirectory;
	
	private boolean rvDB = false;
	private boolean mpi_cluster = true;
	private boolean zip = false;
	private boolean separateZip = false;
	private boolean dirHierarchy = true;
	
	private boolean fileForward = false;
	private boolean pipeForward = true;

	private boolean extractSGTMPI = true;
	private boolean globalExtractSGTMPI = false;
	
	private boolean useMultiSeisPSA = false;
	private int multiSeisPSAFactor = 1;
	
	private boolean sourceForward = false;
	
	private boolean calculateRotD = false;
	private boolean calculateDurations = false;
	
	private boolean skipMD5 = false;
	private boolean nonblockingMD5 = false;
	
	private boolean useDirectSynth = false;
	
	private String ppSite = null;
	
	private double spacing = -1.0;
	//BW has 56 GB permitted per node
	private int seisPSAMemCutoff = 56*1024;

	public PP_DAXParameters() {
		numOfDAXes = 1;
		notifyGroupSize = 1;
		ppDirectory = ".";
		databaseInsert = true;
	}

	public int getNumOfDAXes() {
		return numOfDAXes;
	}

	public void setNumOfDAXes(int numOfDAXes) {
		if (numOfDAXes>MAX_NUM_OF_DAXES) {
			System.out.println("The number of DAXes can't be larger than " + MAX_NUM_OF_DAXES + 
					" Reducing from your request of " + numOfDAXes + " DAXes.");
			this.numOfDAXes = MAX_NUM_OF_DAXES;
		} else if (numOfDAXes<1) {
			System.out.println("The number of DAXes must be at least 1.  Running with 1 DAX.");
			numOfDAXes = 1;
		} else {
			this.numOfDAXes = numOfDAXes;
		}
      	
      	// Setup notifications
      	// If numOfDAXes < MAX_NUM_EMAILS, send a notification for every DAX
      	// Otherwise, send a notification every MAX_NUM_EMAILS(th) DAX
		
		//Add this to disable notifications
		this.notifyGroupSize = numOfDAXes+1; 
//		if (this.numOfDAXes>MAX_NUM_EMAILS) {
//      		this.notifyGroupSize = (int)Math.round(this.numOfDAXes / MAX_NUM_EMAILS);
//		}
	}

	public int getNumVarsPerDAX() {
		return numVarsPerDAX;
	}

	public void setNumVarsPerDAX(int numVarsPerDAX) {
		this.numVarsPerDAX = numVarsPerDAX;
		this.maxVarsPerDAX = (int)Math.round(1.15*numVarsPerDAX);
	}
	
	public int getMaxVarsPerDAX() {
		return maxVarsPerDAX;
	}

	public int getNotifyGroupSize() {
		return notifyGroupSize;
	}

	public void setNotifyGroupSize(int groupSize) {
		notifyGroupSize = groupSize;
	}
	
	public int getCurrentSGTRep() {
		return currentSGTRep;
	}

	public String getPPDirectory() {
		return ppDirectory;
	}

	public void setPPDirectory(String ppDirectory) {
		this.ppDirectory = ppDirectory;
	}
	
	public void setInsert(boolean insert) {
		this.databaseInsert = insert;
	}
	
	public boolean getInsert() {
		return databaseInsert;
	}

	public void setSeisPSA(boolean b) {
		this.seisPSAexe = b;
	}
	
	public boolean isSeisPSA() {
		return this.seisPSAexe;
	}

	public boolean isStochastic() {
		return stochastic;
	}

	public void setStochastic(boolean stochastic) {
		this.stochastic = stochastic;
	}

	public double getStochasticCutoff() {
		return stochasticCutoff;
	}

	public void setStochasticCutoff(double stochasticCutoff) {
		System.out.println("Setting stochastic cutoff to " + stochasticCutoff);
		this.stochasticCutoff = stochasticCutoff;
	}

	public double getStochasticFrequency() {
		return stochasticFrequency;
	}

	public void setStochasticFrequency(double stochasticFrequency) {
		this.stochasticFrequency = stochasticFrequency;
	}

	public boolean isHfsynth() {
		return hfsynth;
	}

	public void setHfsynth(boolean hfsynth) {
		this.hfsynth = hfsynth;
	}

	public boolean isMergePSA() {
		return mergePSA;
	}

	public void setMergePSA(boolean mergePSA) {
		this.mergePSA = mergePSA;
	}

	public boolean isRvDB() {
		return rvDB;
	}

	public void setRvDB(boolean rvDB) {
		this.rvDB = rvDB;
	}

	public boolean isJbsimRVMem() {
		return jbsimRVMem;
	}

	public void setJbsimRVMem(boolean jbsimRVMem) {
		this.jbsimRVMem = jbsimRVMem;
	}

	public boolean isHfsynthRVMem() {
		return hfsynthRVMem;
	}

	public void setHfsynthRVMem(boolean hfsynthRVMem) {
		this.hfsynthRVMem = hfsynthRVMem;
	}	
	
	public void setMPICluster(boolean b) {
		mpi_cluster = b;
	}

	public boolean isMPICluster() {
		return mpi_cluster;
	}

	public boolean isZip() {
		return zip;
	}

	public void setZip(boolean zip) {
		this.zip = zip;
	}

	public void setSeparateZip(boolean b) {
		this.separateZip  = b;
	}
	
	public boolean isSeparateZip() {
		return this.separateZip;
	}

	public void setDirHierarchy(boolean dirHierarchy) {
		this.dirHierarchy = dirHierarchy;
	}

	public boolean isDirHierarchy() {
		return dirHierarchy;
	}

	public boolean isFileForward() {
		return fileForward;
	}

	public void setFileForward(boolean fileForward) {
		this.fileForward = fileForward;
	}

	public boolean isPipeForward() {
		return pipeForward;
	}

	public void setPipeForward(boolean pipeForward) {
		this.pipeForward = pipeForward;
	}
	
	public boolean isExtractSGTMPI() {
		return extractSGTMPI;
	}

	public void setExtractSGTMPI(boolean extractSGTMPI) {
		this.extractSGTMPI = extractSGTMPI;
	}

	public boolean isGlobalExtractSGTMPI() {
		return globalExtractSGTMPI;
	}

	public void setGlobalExtractSGTMPI(boolean extractMPI) {
		this.globalExtractSGTMPI = extractMPI;
	}

	public double getDetFrequency() {
		return detFrequency;
	}

	public void setDetFrequency(double detFrequency) {
		this.detFrequency = detFrequency;
	}

	public int getSeisPSAMemCutoff() {
		return seisPSAMemCutoff;
	}

	public void setSeisPSAMemCutoff(int seisPSAMemCutoff) {
		this.seisPSAMemCutoff = seisPSAMemCutoff;
	}

	public boolean isLargeMemSynth() {
		return largeMemSynth;
	}

	public void setLargeMemSynth(boolean largeMemSynth) {
		this.largeMemSynth = largeMemSynth;
	}

	public boolean isUseMultiSeisPSA() {
		return useMultiSeisPSA;
	}

	public void setUseMultiSeisPSA(boolean useMultiSeisPSA) {
		this.useMultiSeisPSA = useMultiSeisPSA;
	}

	public int getMultiSeisPSAFactor() {
		return multiSeisPSAFactor;
	}

	public void setMultiSeisPSAFactor(int multiSeisPSAFactor) {
		this.multiSeisPSAFactor = multiSeisPSAFactor;
	}

	public boolean isSourceForward() {
		return sourceForward;
	}

	public void setSourceForward(boolean sourceForward) {
		this.sourceForward = sourceForward;
	}

	public boolean isCalculateRotD() {
		return calculateRotD;
	}

	public void setCalculateRotD(boolean calculateRotD) {
		this.calculateRotD = calculateRotD;
	}

	public boolean isSkipMD5() {
		return skipMD5;
	}

	public void setSkipMD5(boolean skipMD5) {
		this.skipMD5 = skipMD5;
	}

	public boolean isNonblockingMD5() {
		return nonblockingMD5;
	}

	public void setNonblockingMD5(boolean nonblockingMD5) {
		this.nonblockingMD5 = nonblockingMD5;
	}

	public boolean isUseDirectSynth() {
		return useDirectSynth;
	}

	public void setUseDirectSynth(boolean useDirectSynth) {
		this.useDirectSynth = useDirectSynth;
	}

	public String getPPSite() {
		return ppSite;
	}

	public void setPPSite(String ppSite) {
		this.ppSite = ppSite;
	}

	public double getSpacing() {
		return spacing;
	}

	public void setSpacing(double spacing) {
		this.spacing = spacing;
	}
	
	public void setCalculateDurations(boolean dur) {
		this.calculateDurations = dur;
	}
	
	public boolean isCalculateDurations() {
		return this.calculateDurations;
	}
}
