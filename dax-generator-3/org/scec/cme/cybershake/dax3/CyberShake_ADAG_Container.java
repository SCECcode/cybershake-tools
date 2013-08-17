package org.scec.cme.cybershake.dax3;

import java.util.ArrayList;
import java.util.HashMap;

import edu.isi.pegasus.planner.dax.ADAG;

public class CyberShake_ADAG_Container {
	private RunIDQuery riq;
	private PP_DAXParameters params;
	private HashMap<ADAG, String> adagToFilename;
	private ADAG preWorkflow;
	private ADAG dbWorkflow;
	private ADAG postWorkflow;
	private ArrayList<ADAG> subWorkflows;
	
	
	public CyberShake_ADAG_Container(RunIDQuery riq, PP_DAXParameters params) {
		this.riq = riq;
		this.params = params;
		adagToFilename = new HashMap<ADAG, String>();
		subWorkflows = new ArrayList<ADAG>();
	}
	
	public RunIDQuery getRIQ() {
		return riq;
	}
	
	public PP_DAXParameters getParams() {
		return params;
	}
	
	public void setPreWorkflow(ADAG preWf, String filename) {
		preWorkflow = preWf;
		adagToFilename.put(preWf, filename);
	}
	
	public void setDBWorkflow(ADAG dbWf, String filename) {
		dbWorkflow = dbWf;
		adagToFilename.put(dbWf, filename);
	}
	
	public void setPostWorkflow(ADAG postWf, String filename) {
		postWorkflow = postWf;
		adagToFilename.put(postWf, filename);
	}
	
	public void addSubWorkflow(ADAG subWf, String filename) {
		subWorkflows.add(subWf);
		adagToFilename.put(subWf, filename);
	}

	public ADAG getPreWorkflow() {
		return preWorkflow;
	}

	public ADAG getDBWorkflow() {
		return dbWorkflow;
	}

	public ADAG getPostWorkflow() {
		return postWorkflow;
	}
	
	public ArrayList<ADAG> getSubWorkflows() {
		return subWorkflows;
	}
	
	public String getFilename(ADAG wf) {
		return adagToFilename.get(wf);
	}
}
