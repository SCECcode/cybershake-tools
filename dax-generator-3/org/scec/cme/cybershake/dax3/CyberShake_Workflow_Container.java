package org.scec.cme.cybershake.dax3;

import java.util.ArrayList;
import java.util.HashMap;

import edu.isi.pegasus.planner.dax.ADAG;

public class CyberShake_Workflow_Container {
	private RunIDQuery riq;
	private PP_DAXParameters params;
//	private HashMap<ADAG, String> adagToFilename;
	private String preWorkflow;
	private String dbWorkflow;
	private String postWorkflow;
	private ArrayList<String> subWorkflows;
	
	
	public CyberShake_Workflow_Container(RunIDQuery riq, PP_DAXParameters params) {
		this.riq = riq;
		this.params = params;
		subWorkflows = new ArrayList<String>();
	}
	
	public RunIDQuery getRIQ() {
		return riq;
	}
	
	public PP_DAXParameters getParams() {
		return params;
	}
	
	public void setPreWorkflow(String filename) {
		preWorkflow = filename;
	}
	
	public void setDBWorkflow(String filename) {
		dbWorkflow = filename;
	}
	
	public void setPostWorkflow(String filename) {
		postWorkflow = filename;
	}
	
	public void addSubWorkflow(String filename) {
		subWorkflows.add(filename);
	}

	public String getPreWorkflow() {
		return preWorkflow;
	}

	public String getDBWorkflow() {
		return dbWorkflow;
	}

	public String getPostWorkflow() {
		return postWorkflow;
	}
	
	public ArrayList<String> getSubWorkflows() {
		return subWorkflows;
	}

}
