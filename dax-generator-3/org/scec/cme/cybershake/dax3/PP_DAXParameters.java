package org.scec.cme.cybershake.dax3;

public class PP_DAXParameters {
	private static final int MAX_NUM_OF_DAXES = 200;
	private static final int MAX_SGT_REPLICATION = 50;
	private static final int MAX_NUM_EMAILS = 5;
	
	private int numOfDAXes;
	private boolean usePriorities;
	private int sgtReplication;
	private int currentSGTRep;
	private boolean sortRuptures;
	private boolean databaseInsert;
	private boolean useMemcached = false;
	private boolean seisPSAexe = false;
	private boolean highFrequency = false;
	private double highFrequencyCutoff = 0.0;
	private double maxHighFrequency = 10.0;
	private boolean hfsynth = false;
	private boolean mergePSA = false;
	private boolean mergedMemcached = false;
	
	private boolean jbsimRVMem = false;
	private boolean hfsynthRVMem = false;
		
	private int numVarsPerDAX;
	private int maxVarsPerDAX;
	
	private int notifyGroupSize;
	
	private String ppDirectory;
	
	private boolean rvDB = false;

	public PP_DAXParameters() {
		numOfDAXes = 1;
		usePriorities = false;
		sgtReplication = 1;
		notifyGroupSize = 1;
		sortRuptures = false;
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
		if (this.numOfDAXes>MAX_NUM_EMAILS) {
      		this.notifyGroupSize = (int)Math.round(this.numOfDAXes / MAX_NUM_EMAILS);
		}
	}

	public boolean isUsePriorities() {
		return usePriorities;
	}

	public void setUsePriorities(boolean usePriorities) {
		this.usePriorities = usePriorities;
	}

	public int getSgtReplication() {
		return sgtReplication;
	}

	public void setSgtReplication(int sgtReplication) {
		if (sgtReplication>MAX_SGT_REPLICATION) {
			System.out.println("The number of replicated SGTs can't be larger than " + MAX_SGT_REPLICATION + 
					" Reducing from your request of " + sgtReplication + " DAXes.");
			this.sgtReplication = MAX_SGT_REPLICATION;
		} else if (sgtReplication<1) {
			System.out.println("The number of SGTs must be at least 1.  Running with 1 copy of the SGTs.");
			sgtReplication = 1;
		} else {
			this.sgtReplication = sgtReplication;
		}
		currentSGTRep = 0;
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

	public int getCurrentSGTRep() {
		return currentSGTRep;
	}

	public void incrementSGTRep() {
		currentSGTRep = (currentSGTRep+1) % sgtReplication;
	}

	public boolean isSortRuptures() {
		return sortRuptures;
	}

	public void setSortRuptures(boolean sortRuptures) {
		this.sortRuptures = sortRuptures;
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

	public boolean isUseMemcached() {
		return useMemcached;
	}

	public void setUseMemcached(boolean useMemcached) {
		this.useMemcached = useMemcached;
	}

	public void setMergedExe(boolean b) {
		this.seisPSAexe = b;
	}
	
	public boolean isMergedExe() {
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
		this.highFrequencyCutoff = highFrequencyCutoff;
	}

	public double getMaxHighFrequency() {
		return maxHighFrequency;
	}

	public void setMaxHighFrequency(double maxHighFrequency) {
		this.maxHighFrequency = maxHighFrequency;
	}

	public void setUseMergedMemcached(boolean b) {
		this.mergedMemcached = b;
	}

	public boolean isMergedMemcached() {
		return mergedMemcached;
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

}
