package org.scec.cme.cybershake.dax3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.Gson;

/* This class is populated by the DAX generator and then serialized to a JSON object
 * by GSON.
 * 
 * It also verifies that the resulting JSON document matches the schema.
 */

public class JSON_Specification {
	//Volume information
	private double depth; // From SGT_DAXParameters
	private double grid_spacing; // From SGT_DAXParameters
	private String projection; // ?
	private double rotation_angle; // From PreCVM
	private double[][] volume_corners = new double[4][2]; // From PreCVM
	private double[] volume_dimensions = new double[3]; // From PreCVM
	private double[] volume_grid_point_dimensions = new double[3]; // From PreCVM
	
	//Velocity model information
	private double minimum_vs; // From SGT_DAXParameters or UCVM
	private String velocity_model_list; // From SGT_DAXParameters
	private boolean smoothing; // From 
	private double smoothing_dist;
	
	//Simulation information
	private int[] processors = new int[3];
	private double sgt_dt;
	private int sgt_nt;
	private double source_frequency;
	private int sgt_time_decimation;
	private String components;
	
	//Seismogram information
	private double seismogram_dt;
	private int seismogram_nt;
	
	//Other stuff
	private double deterministic_frequency;
	private String server;
	private int run_id;
	private String version = "1.0";
	
	public void populateWithSGTParameters(SGT_DAXParameters sdp) {
		server = sdp.getServer();
		minimum_vs = sdp.getMinvs();
		grid_spacing = sdp.getSpacing();
		smoothing = sdp.isSmoothing();		
	}

	public void populateWithRIQ(RunIDQuery riq) {
		run_id = riq.getRunID();
		velocity_model_list = riq.getVelModelString();
		source_frequency = riq.getSourceFrequency();
		if (riq.getNumComponents()==2) {
			components = "XY";
		} else if (riq.getNumComponents()==3) {
			components = "XYZ";
		}
		server = riq.getHost();
	}
	
	public void populateWithPPParameters(PP_DAXParameters pdp) {
		grid_spacing = pdp.getSpacing();
		

	}
	
	public void toJsonFile(String filename) {
		Gson g = new Gson();
		String jsonString = g.toJson(this);
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(filename));
			bw.write(jsonString);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	public double getDepth() {
		return depth;
	}

	public void setDepth(double depth) {
		this.depth = depth;
	}

	public double getGrid_spacing() {
		return grid_spacing;
	}

	public void setGrid_spacing(double grid_spacing) {
		this.grid_spacing = grid_spacing;
	}

	public String getProjection() {
		return projection;
	}

	public void setProjection(String projection) {
		this.projection = projection;
	}

	public double getRotation_angle() {
		return rotation_angle;
	}

	public void setRotation_angle(double rotation_angle) {
		this.rotation_angle = rotation_angle;
	}

	public double[][] getVolume_corners() {
		return volume_corners;
	}

	public void setVolume_corners(double[][] volume_corners) {
		this.volume_corners = volume_corners;
	}

	public double[] getVolume_dimensions() {
		return volume_dimensions;
	}

	public void setVolume_dimensions(double[] volume_dimensions) {
		this.volume_dimensions = volume_dimensions;
	}

	public double[] getVolume_grid_point_dimensions() {
		return volume_grid_point_dimensions;
	}

	public void setVolume_grid_point_dimensions(double[] volume_grid_point_dimensions) {
		this.volume_grid_point_dimensions = volume_grid_point_dimensions;
	}

	public double getMinimum_vs() {
		return minimum_vs;
	}

	public void setMinimum_vs(double minimum_vs) {
		this.minimum_vs = minimum_vs;
	}

	public String getVelocity_model_list() {
		return velocity_model_list;
	}

	public void setVelocity_model_list(String velocity_model_list) {
		this.velocity_model_list = velocity_model_list;
	}

	public boolean isSmoothing() {
		return smoothing;
	}

	public void setSmoothing(boolean smoothing) {
		this.smoothing = smoothing;
	}

	public double getSmoothing_dist() {
		return smoothing_dist;
	}

	public void setSmoothing_dist(double smoothing_dist) {
		this.smoothing_dist = smoothing_dist;
	}

	public int[] getProcessors() {
		return processors;
	}

	public void setProcessors(int[] processors) {
		this.processors = processors;
	}

	public double getSgt_dt() {
		return sgt_dt;
	}

	public void setSgt_dt(double sgt_dt) {
		this.sgt_dt = sgt_dt;
	}

	public int getSgt_nt() {
		return sgt_nt;
	}

	public void setSgt_nt(int sgt_nt) {
		this.sgt_nt = sgt_nt;
	}

	public double getSource_frequency() {
		return source_frequency;
	}

	public void setSource_frequency(double source_frequency) {
		this.source_frequency = source_frequency;
	}

	public int getSgt_time_decimation() {
		return sgt_time_decimation;
	}

	public void setSgt_time_decimation(int sgt_time_decimation) {
		this.sgt_time_decimation = sgt_time_decimation;
	}

	public String getComponents() {
		return components;
	}

	public void setComponents(String components) {
		this.components = components;
	}

	public double getSeismogram_dt() {
		return seismogram_dt;
	}

	public void setSeismogram_dt(double seismogram_dt) {
		this.seismogram_dt = seismogram_dt;
	}

	public int getSeismogram_nt() {
		return seismogram_nt;
	}

	public void setSeismogram_nt(int seismogram_nt) {
		this.seismogram_nt = seismogram_nt;
	}

	public double getDeterministic_frequency() {
		return deterministic_frequency;
	}

	public void setDeterministic_frequency(double deterministic_frequency) {
		this.deterministic_frequency = deterministic_frequency;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public int getRun_id() {
		return run_id;
	}

	public void setRun_id(int run_id) {
		this.run_id = run_id;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}
	

}
