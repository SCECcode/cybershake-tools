package org.scec.cme.cybershake.dax3;

public class Stochastic_DAXParameters {
	private double mergeFrequency = 1.0;
	private int lfRunID = -1;
	private boolean runRotd = true;
	private String directory;
	private boolean runSiteResponse = true;
	private boolean debug = false;
	private RunIDQuery lowFreqRIQ;
	private double tlen = 300.0;
	
	public double getMergeFrequency() {
		return mergeFrequency;
	}
	public void setMergeFrequency(double mergeFrequency) {
		this.mergeFrequency = mergeFrequency;
	}
	public String getMergeFrequencyString() {
		return String.format("%.1f", mergeFrequency);
	}
	public int getLfRunID() {
		return lfRunID;
	}
	public void setLfRunID(int lfRunID) {
		this.lfRunID = lfRunID;
		this.lowFreqRIQ = new RunIDQuery(this.lfRunID);
	}
	public RunIDQuery getLowFreqRIQ() {
		return lowFreqRIQ;
	}
	public boolean isRunRotd() {
		return runRotd;
	}
	public void setRunRotd(boolean runRotd) {
		this.runRotd = runRotd;
	}
	public String getDirectory() {
		return directory;
	}
	public void setDirectory(String directory) {
		this.directory = directory;
	}
	public boolean isDebug() {
		return debug;
	}
	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	public boolean isRunSiteResponse() {
		return runSiteResponse;
	}
	public void setRunSiteResponse(boolean runSiteResponse) {
		this.runSiteResponse = runSiteResponse;
	}
	public double getTlen() {
		return tlen;
	}
	public void setTlen(double tlen) {
		this.tlen = tlen;
	}
}
