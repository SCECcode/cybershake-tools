package org.scec.cme.cybershake.dax3;

import java.util.ArrayList;

public class SGT_DAXParameters {
	private String daxFilename;
	private String directory;
	private int maxCores = -1;
	private ArrayList<RunIDQuery> runIDQueries;
	
	private boolean separateVelocityJobs = false;
	private boolean separateMD5Jobs = false;
	private boolean handoffJob = false;
	
	private String sgtSite = null;
	
	public SGT_DAXParameters(String filename) {
		this.daxFilename = filename;
		this.directory = ".";
	}

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public String getDaxFilename() {
		return daxFilename;
	}

	public ArrayList<RunIDQuery> getRunIDQueries() {
		return runIDQueries;
	}

	public void setRunIDQueries(ArrayList<RunIDQuery> runIDQueries) {
		this.runIDQueries = runIDQueries;
	}

	public boolean isSeparateVelocityJobs() {
		return separateVelocityJobs;
	}

	public void setSeparateVelocityJobs(boolean separateVelocityJobs) {
		this.separateVelocityJobs = separateVelocityJobs;
	}

	public int getMaxSGTCores() {
		return this.maxCores;
	}
	
	public void setMaxSGTCores(int maxCores) {
		this.maxCores = maxCores;
	}

	public boolean isSeparateMD5Jobs() {
		return separateMD5Jobs;
	}

	public void setSeparateMD5Jobs(boolean separateMD5Jobs) {
		this.separateMD5Jobs = separateMD5Jobs;
	}

	public boolean isHandoffJob() {
		return handoffJob;
	}

	public void setHandoffJob(boolean handoffJob) {
		this.handoffJob = handoffJob;
	}

	public String getSgtSite() {
		return sgtSite;
	}

	public void setSgtSite(String sgtSite) {
		this.sgtSite = sgtSite;
	}
	
	
}
