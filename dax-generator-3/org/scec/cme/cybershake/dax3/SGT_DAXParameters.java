package org.scec.cme.cybershake.dax3;

import java.util.ArrayList;

public class SGT_DAXParameters {
	private String daxFilename;
	private String directory;
	private ArrayList<RunIDQuery> runIDQueries;
	
	private boolean separateVelocityJobs = false;
	
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
	
	
}
