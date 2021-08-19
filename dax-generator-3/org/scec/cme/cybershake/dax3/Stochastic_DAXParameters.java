package org.scec.cme.cybershake.dax3;

public class Stochastic_DAXParameters {
	private double stochFrequency = 10.0;
	private double mergeFrequency = 1.0;
	private int lfRunID = -1;
	private boolean runRotd = true;
	private String directory;
	private boolean runSiteResponse = true;
	private boolean runLFSiteResponse = true;
	private boolean runDuration = true;
	private boolean debug = false;
	private RunIDQuery lowFreqRIQ;
	private double tlen = 300.0;
	private String velocityInfoFile = null;
	//vref and vpga are based on 1D velocity model
	//Set to 500 m/s for now
	private double vref = 500.0;
	private double vpga = vref;
	
	public double getStochFrequency() {
		return stochFrequency;
	}
	public void setStochFrequency(double stochFrequency) {
		this.stochFrequency = stochFrequency;
	}
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
	public void setLfRunID(int lfRunID, String server) {
		this.lfRunID = lfRunID;
		this.lowFreqRIQ = new RunIDQuery(this.lfRunID, server);
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
	public boolean isRunDuration() {
		return runDuration;
	}
	public void setRunDuration(boolean runDuration) {
		this.runDuration = runDuration;
	}
	public boolean isRunLFSiteResponse() {
		return runLFSiteResponse;
	}
	public void setRunLFSiteResponse(boolean runLFSiteResponse) {
		this.runLFSiteResponse = runLFSiteResponse;
	}
	public String getVelocityInfoFile() {
		return velocityInfoFile;
	}
	public void setVelocityInfoFile(String velocityInfoFile) {
		this.velocityInfoFile = velocityInfoFile;
	}
	public double getVref() {
		return vref;
	}
	public void setVref(double vref) {
		this.vref = vref;
	}
	public double getVpga() {
		return vpga;
	}
	public void setVpga(double vpga) {
		this.vpga = vpga;
	}
}
