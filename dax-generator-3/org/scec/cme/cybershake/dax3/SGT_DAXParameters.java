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
	
	private String server = "moment.usc.edu";
	private double spacing = -1.0;
	private double minvs = -1.0;
	
	private boolean smoothing = true;
	private boolean boundingBox = false;
	
	private double depth = -1.0;
	//Change default to h/4
	private double h_frac = 0.25;
	//Default rotation of -55 degrees
	private double rotation = -55.0;
	
	private String taperMode = "none";
	private double taperDepth = 700.0;
	private String taperModels = "all";
	
	private boolean zComp = false;
	private boolean localVelParamInsert = false;
	
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

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public double getSpacing() {
		return spacing;
	}

	public void setSpacing(double spacing) {
		this.spacing = spacing;
	}

	public double getMinvs() {
		return minvs;
	}

	public void setMinvs(double minvs) {
		this.minvs = minvs;
	}

	public boolean isSmoothing() {
		return smoothing;
	}

	public void setSmoothing(boolean smoothing) {
		this.smoothing = smoothing;
	}

	public boolean isBoundingBox() {
		return boundingBox;
	}

	public void setBoundingBox(boolean boundingBox) {
		this.boundingBox = boundingBox;
	}

	public double getDepth() {
		return depth;
	}

	public void setDepth(double depth) {
		this.depth = depth;
	}

	public double getH_frac() {
		return h_frac;
	}

	public void setH_frac(double h_frac) {
		this.h_frac = h_frac;
	}

	public double getRotation() {
		return rotation;
	}

	public void setRotation(double rotation) {
		this.rotation = rotation;
	}

	public String getTaperMode() {
		return taperMode;
	}

	public void setTaperMode(String taperMode) {
		this.taperMode = taperMode;
	}

	public double getTaperDepth() {
		return taperDepth;
	}

	public void setTaperDepth(double taperDepth) {
		this.taperDepth = taperDepth;
	}

	public void setTaperModels(String taperModelString) {
		this.taperModels = taperModelString;
	}
	
	public String getTaperModels() {
		return this.taperModels;
	}

	public boolean isZComp() {
		return zComp;
	}

	public void setZComp(boolean zComp) {
		this.zComp = zComp;
	}

	public boolean isLocalVelParamInsert() {
		return localVelParamInsert;
	}

	public void setLocalVelParamInsert(boolean localVelParamInsert) {
		this.localVelParamInsert = localVelParamInsert;
	}
	
	
}
