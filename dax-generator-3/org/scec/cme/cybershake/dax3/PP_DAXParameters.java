package org.scec.cme.cybershake.dax3;

public class PP_DAXParameters {
	private static final int MAX_NUM_OF_DAXES = 100;
	private static final int MAX_NUM_EMAILS = 5;
	
	private int numOfDAXes;
	private int currentSGTRep;
	private boolean databaseInsert;

	//Defaults
	private boolean seisPSAexe = true;
	private boolean highFrequency = false;
	//Maximum deterministic frequency
	private double detFrequency = 0.5;
	private double highFrequencyCutoff = 0.0;
	private double maxHighFrequency = 10.0;
	private boolean hfsynth = true;
	private boolean mergePSA = true;
	
	private boolean jbsimRVMem = true;
	private boolean hfsynthRVMem = true;
		
	private int numVarsPerDAX;
	private int maxVarsPerDAX;
	
	private boolean useAWP = false;

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
	
	//27 GB
	private int seisPSAMemCutoff = 27*1024;

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

	public boolean isHighFrequency() {
		return highFrequency;
	}

	public void setHighFrequency(boolean highFrequency) {
		this.highFrequency = highFrequency;
	}

	public double getHighFrequencyCutoff() {
		return highFrequencyCutoff;
	}

	public void setHighFrequencyCutoff(double highFrequencyCutoff) {
		System.out.println("Setting HF cutoff to " + highFrequencyCutoff);
		this.highFrequencyCutoff = highFrequencyCutoff;
	}

	public double getMaxHighFrequency() {
		return maxHighFrequency;
	}

	public void setMaxHighFrequency(double maxHighFrequency) {
		this.maxHighFrequency = maxHighFrequency;
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
	
	public boolean isUseAWP() {
		return useAWP;
	}

	public void setUseAWP(boolean useAWP) {
		this.useAWP = useAWP;
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
}
