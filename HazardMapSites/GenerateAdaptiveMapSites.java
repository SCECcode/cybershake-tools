import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.scec.cme.ConversionServices;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;


public class GenerateAdaptiveMapSites {
	//All-california settings
	public static double[] westCorner = {40.5, -127.75};
	public static double[] eastCorner = {34.3, -112.1};
	public static double[] northCorner = {44.05, -122.00};
	public static double[] southCorner = {30.53, -118.125};
	
	public static final int SPHEROID = 20;
	public static final int UTM_ZONE = 11;
	
	public static final int ERF_ID = 35;
	public static final int CUTOFF = 200;
	
	private static double[][] caOutline = null;
	
	public static double SPACING = 40000.0; //m
	
	public static String FILENAME = "CyberShake_CA_adaptive_map_sites";
	
	public static Polygon caPoly = null;
	
	public static class Site {
		public double lat;
		public double lon;
		public int spacing;
		
		public Site(double la, double lo, int s) {
			lat = la;
			lon = lo;
			spacing = s;
		}
	}
	
	public static class Gridbox {
		public double lat;
		public double lon;
		public double value;
		public int spacing;
		public double easting;
		public double northing;
		
		public Gridbox(double la, double lo, double v) {
			lat = la;
			lon = lo;
			value = v;
		}
		
	}
	
	public static ArrayList<Gridbox> boxes = new ArrayList<Gridbox>();
	
	public static void main(String[] args) {
		Document xmlOut = null;
		try {
			xmlOut = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		
//		for (int i=0; i<3; i++) {
			Element root = xmlOut.createElementNS("http://earth.google.com/kml/2.2", "kml");
			Element doc = xmlOut.createElement("Document");
			printHeader(xmlOut, doc);
//		}
		
		importPPData();

		
		
		double[] westCornerUTM = new double[3];
		double[] eastCornerUTM = new double[3];
		double[] northCornerUTM = new double[3];
		double[] southCornerUTM = new double[3];
		
		double[] tmp = ConversionServices.getUTMwithZone(westCorner[0], westCorner[1], SPHEROID, UTM_ZONE);
		for (int i=0; i<tmp.length; i++) {
			westCornerUTM[i] = tmp[i];
		}
		tmp = ConversionServices.getUTMwithZone(eastCorner[0], eastCorner[1], SPHEROID, UTM_ZONE);
		for (int i=0; i<tmp.length; i++) {
			eastCornerUTM[i] = tmp[i];
		}
		tmp = ConversionServices.getUTMwithZone(northCorner[0], northCorner[1], SPHEROID, UTM_ZONE);
		for (int i=0; i<tmp.length; i++) {
			northCornerUTM[i] = tmp[i];
		}
		tmp = ConversionServices.getUTMwithZone(southCorner[0], southCorner[1], SPHEROID, UTM_ZONE);
		for (int i=0; i<tmp.length; i++) {
			southCornerUTM[i] = tmp[i];
		}
		
		double xDist = computeDistance(westCornerUTM, southCornerUTM);
		double yDist = computeDistance(westCornerUTM, northCornerUTM);
		
		int xSteps = (int)(xDist/SPACING+.5);
		int ySteps = (int)(yDist/SPACING+.5);
		
		double ENratio = (northCornerUTM[0]-westCornerUTM[0])/(northCornerUTM[1]-westCornerUTM[1]);
		double deltaN = Math.sqrt((SPACING*SPACING)/(ENratio*ENratio+1));
		double deltaE = Math.sqrt(SPACING*SPACING-deltaN*deltaN);
		System.out.println("ratio: " + ENratio + " dn: " + deltaN + " de: " + deltaE);
		
		double[] start = {westCornerUTM[0], westCornerUTM[1]};
		Element placemark, name, styleUrl, point, coordinates;
		Text nameText, styleUrlText, coordinatesText;
		
		ArrayList<Site> sites = new ArrayList<Site>();
		ArrayList<String> lines = new ArrayList<String>();
		
		int count = 0;
		int index = -1;
		
		int[] counts = {0, 0, 0};
		
//		String[] styles = {"#Gridpoints10", "#Gridpoints20", "#Gridpoints40"};
		String style = "#Gridpoints";
		
		double s;
		double n;
		double e;
		double w;

		for(int i=1; i<xSteps; i++) {
			start[0] += deltaN;
			start[1] -= deltaE;
			for(int j=1; j<ySteps; j++) {
				System.out.println("east: " + (start[0]+j*deltaE) + " north: " + (start[1]+j*deltaN));
				double east = start[0]+j*deltaE;
				double north = start[1]+j*deltaN;
				double[] ll = ConversionServices.getLatLong(start[0]+j*deltaE, start[1]+j*deltaN, SPHEROID, UTM_ZONE, ConversionServices.NORTHERN_HEMISPHERE);
				
				//Construct polygon for testing
				//east, north
				Polygon sitePoly = new Polygon();
				//west corner
				sitePoly.addPoint((int)(east - deltaE/2 - deltaN/2), (int)(north + deltaE/2 - deltaN/2));
				//north corner
				sitePoly.addPoint((int)(east + deltaE/2 - deltaN/2), (int)(north + deltaE/2 + deltaN/2));
				//east corner
				sitePoly.addPoint((int)(east + deltaE/2 + deltaN/2), (int)(north - deltaE/2 + deltaN/2));
				//south corner
				sitePoly.addPoint((int)(east - deltaE/2 + deltaN/2), (int)(north - deltaE/2 - deltaN/2));
				
				int numBoxes = 0;
				double tot = 0;
				double peakVal = -10;
				//Average all boxes which fall inside
				for (Gridbox gb: boxes) {
					if (sitePoly.contains(gb.easting, gb.northing)) {
						numBoxes++;
						//weight peak values more
						tot += gb.value;
						if (gb.value>peakVal) {
							peakVal = gb.value;
						}
					}
				}
				if (tot==0) {
					//nothing was inside the box, skip
					continue;
				}
				
				double avgVal = tot/numBoxes;
				int spacing = 40;
				if (avgVal > 0.55 || peakVal > 0.75) {
					spacing = 10;
				} else if (avgVal > 0.25 || peakVal > 0.45) {
					spacing = 20;
				}
//				System.out.println("spacing: " + spacing);
				
				//add site in the middle for testing
//				sites.add(new Site(ll[0], ll[1], 40));
				
				if (spacing==40) {
					//1 site, in the middle
					if (insideBoundary(ll)) {
						sites.add(new Site(ll[0], ll[1], 40));
					}
				} else if (spacing==20) {
					//4 sites, each 10 km ns/ew from center
					double[] offsets = {-1*deltaE/4-deltaN/4, deltaE/4-deltaN/4, deltaE/4+deltaN/4, -1*deltaE/4+deltaN/4};
					for (int k=0; k<offsets.length; k++) {
						double[] lltmp = ConversionServices.getLatLong(east + offsets[k], north+offsets[(k+1)%offsets.length], SPHEROID, UTM_ZONE, ConversionServices.NORTHERN_HEMISPHERE);
						if (insideBoundary(lltmp)) {
							sites.add(new Site(lltmp[0], lltmp[1], 20));
						}
					}
				} else if (spacing==10) {
					//16 sites, on grid 5 km in from edges
					double[] outerOffsets = {-1*deltaE/4-deltaN/4, deltaE/4-deltaN/4, deltaE/4+deltaN/4, -1*deltaE/4+deltaN/4};
					double[] innerOffsets = {-1*deltaE/8-deltaN/8, deltaE/8-deltaN/8, deltaE/8+deltaN/8, -1*deltaE/8+deltaN/8};
					for (int k=0; k<4; k++) {
						for (int l=0; l<4; l++) {
							double[] lltmp = ConversionServices.getLatLong(east + outerOffsets[k] + innerOffsets[l], north + outerOffsets[(k+1)%outerOffsets.length] + innerOffsets[(l+1)%innerOffsets.length], SPHEROID, UTM_ZONE, ConversionServices.NORTHERN_HEMISPHERE);
							if (insideBoundary(lltmp)) {
								sites.add(new Site(lltmp[0], lltmp[1], 10));
							}
						}
					}
				}
			}
		}
			
		//Write sites to file
		for (Site site: sites) {
			count++;
			if (site.spacing==40) {
				counts[0]++;
			} else if (site.spacing==20) {
				counts[1]++;
			} else if (site.spacing==10) {
				counts[2]++;
			}
			placemark = xmlOut.createElement("Placemark");
//					name = xmlOuts[k].createElement("name");
//						nameText = xmlOuts[k].createTextNode(String.format("s%05d", count));
//						name.appendChild(nameText);
//					placemark.appendChild(name);
				styleUrl = xmlOut.createElement("styleUrl");
					styleUrlText = xmlOut.createTextNode(style);
					styleUrl.appendChild(styleUrlText);
				placemark.appendChild(styleUrl);
				point = xmlOut.createElement("Point");
					coordinates = xmlOut.createElement("coordinates");
						coordinatesText = xmlOut.createTextNode(String.format("%.5f, %.5f, 0", site.lon, site.lat));
						coordinates.appendChild(coordinatesText);
						point.appendChild(coordinates);
				placemark.appendChild(point);
			doc.appendChild(placemark);
					
			lines.add(String.format("s%05d", count) + " \"" + String.format("s%05d", count) + "\" " + String.format("%.5f", site.lat) + " " + String.format("%.5f", site.lon) + " " + CUTOFF + " " + ERF_ID + " " + site.spacing);
			
			count++;
		}
		
		root.appendChild(doc);
		xmlOut.appendChild(root);
		
		try {
			TransformerFactory factory = TransformerFactory.newInstance();
			factory.setAttribute("indent-number", 4);
			Transformer t = factory.newTransformer();
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			
			Source src = new DOMSource(xmlOut);
			StreamResult out = new StreamResult(new FileWriter(FILENAME + ".kml"));
			t.transform(src, out);
			
		} catch (TransformerConfigurationException ex) {
			ex.printStackTrace();
		} catch (TransformerFactoryConfigurationError ex) {
			ex.printStackTrace();
		} catch (TransformerException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		Collections.sort(lines, new LineComparator());
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(FILENAME + ".txt"));
			for (String str: lines) {
				bw.write(str + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		System.out.println("40 km: " + counts[0] + " sites (" + counts[0] + " squares)\n20 km: " + counts[1] + " sites (" + counts[1]/4 + " squares)\n10 km: " + counts[2] + " sites (" + counts[2]/16 + " squares)");
	}

	private static void importPPData() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("HazardMapSites/imrs1_2percent_in_30.txt"));
			String line = br.readLine();
			while (line!=null) {
				String[] pieces = line.split("\\s+");
				double lat = Double.parseDouble(pieces[0]);
				double lon = Double.parseDouble(pieces[1]);
				double value = Double.parseDouble(pieces[2]);
				Gridbox gb = new Gridbox(lat, lon, value);
				if(insideBoundary(gb)) {
						double[] tmp = ConversionServices.getUTMwithZone(lat, lon, SPHEROID, UTM_ZONE);
						gb.easting = tmp[0];
						gb.northing = tmp[1];
				}
				boxes.add(gb);
				line = br.readLine();
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}		
	}
	
	private static boolean insideBoundary(Gridbox gb) {
		return insideBoundary(new double[]{gb.lat, gb.lon});
	}

	private static boolean insideBoundary(double[] ll) {
		if (caPoly==null) {
			ArrayList<double[]> edge = new ArrayList<double[]>();
			try {
				BufferedReader in = new BufferedReader(new FileReader("HazardMapSites/allCalifornia.txt"));
				String line = in.readLine();
				while (line!=null) {
					String[] pieces = line.split(" ");
					double[] points = new double[2];
					points[0] = Double.parseDouble(pieces[0]);
					points[1] = Double.parseDouble(pieces[1]);
					edge.add(points);
					line = in.readLine();
				}
				in.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			caPoly = new Polygon();
			for (double[] point: edge) {
				caPoly.addPoint((int)(point[0]*100), (int)(point[1]*100));
			}
		}
	
		if (caPoly.contains((int)(ll[0]*100), (int)(ll[1]*100))) {
			return true;
		}
		return false;
	}

	private static void printHeader(Document doc, Element root) {
		Element gridpointsStyle = doc.createElement("Style");
		gridpointsStyle.setAttribute("id", "Gridpoint");
			Element iconStyle = doc.createElement("IconStyle");
				Element scale = doc.createElement("scale");
					Text scaleText = doc.createTextNode("0.1");
					scale.appendChild(scaleText);
				iconStyle.appendChild(scale);
				Element icon = doc.createElement("Icon");
					Element url = doc.createElement("href");
						Text urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/red-dot.png");
//						Text urlText = doc.createTextNode("HazardMapSites/red_dot.png");
						url.appendChild(urlText);
					icon.appendChild(url);
				iconStyle.appendChild(icon);
				gridpointsStyle.appendChild(iconStyle);
		root.appendChild(gridpointsStyle);
		
//		gridpointsStyle = doc.createElement("Style");
//		gridpointsStyle.setAttribute("id", "Gridpoints10");
//			iconStyle = doc.createElement("IconStyle");
//				scale = doc.createElement("scale");
//					scaleText = doc.createTextNode("0.07");
//					scale.appendChild(scaleText);
//				iconStyle.appendChild(scale);
//				icon = doc.createElement("Icon");
//					url = doc.createElement("href");
//						urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/yellow-dot.png");
//						url.appendChild(urlText);
//					icon.appendChild(url);
//				iconStyle.appendChild(icon);
//				gridpointsStyle.appendChild(iconStyle);
//		root.appendChild(gridpointsStyle);
//		
//		gridpointsStyle = doc.createElement("Style");
//		gridpointsStyle.setAttribute("id", "Gridpoints5");
//			iconStyle = doc.createElement("IconStyle");
//				scale = doc.createElement("scale");
//					scaleText = doc.createTextNode("0.04");
//					scale.appendChild(scaleText);
//				iconStyle.appendChild(scale);
//				icon = doc.createElement("Icon");
//					url = doc.createElement("href");
//						urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/green-dot.png");
//						url.appendChild(urlText);
//					icon.appendChild(url);
//				iconStyle.appendChild(icon);
//				gridpointsStyle.appendChild(iconStyle);
//		root.appendChild(gridpointsStyle);
		
		Element blacklineStyle = doc.createElement("Style");
		blacklineStyle.setAttribute("id", "blackLine");
			Element lineStyle = doc.createElement("LineStyle");
				Element color = doc.createElement("color");
					Text colorText = doc.createTextNode("c0000000");
					color.appendChild(colorText);
				Element width = doc.createElement("width");
					Text widthText = doc.createTextNode("3");
					width.appendChild(widthText);
				lineStyle.appendChild(color);
				lineStyle.appendChild(width);
			blacklineStyle.appendChild(lineStyle);
		root.appendChild(blacklineStyle);
		
		Element placemark = doc.createElement("Placemark");
			Element name = doc.createElement("name");
				Text nameText = doc.createTextNode("Region");
				name.appendChild(nameText);
			placemark.appendChild(name);
			Element description = doc.createElement("description");
				Text descriptionText = doc.createTextNode("CyberShake map region, 200 km x 120 km");
				description.appendChild(descriptionText);
			placemark.appendChild(description);
			Element styleURL = doc.createElement("styleUrl");
				Text styleURLText = doc.createTextNode("#blackLine");
				styleURL.appendChild(styleURLText);
			placemark.appendChild(styleURL);
			Element lineString = doc.createElement("LineString");
				Element extrude = doc.createElement("extrude");
					Text extrudeText = doc.createTextNode("1");
					extrude.appendChild(extrudeText);
				lineString.appendChild(extrude);
				Element tessellate = doc.createElement("tessellate");
					Text tessellateText = doc.createTextNode("1");
					tessellate.appendChild(tessellateText);
				lineString.appendChild(tessellate);
				Element altitudeMode = doc.createElement("altitudeMode");
					Text altitudeModeText = doc.createTextNode("relative");
					altitudeMode.appendChild(altitudeModeText);
				lineString.appendChild(altitudeMode);
				Element coordinates = doc.createElement("coordinates");
					String text = eastCorner[1] + ", " + eastCorner[0] + ", 0,   " + 
								northCorner[1] + ", " + northCorner[0] + ", 0,   " + 
								westCorner[1] + ", " + westCorner[0] + ", 0,   " + 
								southCorner[1] + ", " + southCorner[0] + ", 0,   " + 
								eastCorner[1] + ", " + eastCorner[0] + ", 0";
					Text coordinatesText = doc.createTextNode(text);
					coordinates.appendChild(coordinatesText);
				lineString.appendChild(coordinates);
			placemark.appendChild(lineString);
		root.appendChild(placemark);
		
	}

	private static double computeDistance(double[] corner1, double[] corner2) {
		double eDiff = corner1[0]-corner2[0];
		double nDiff = corner1[1]-corner2[1];
		return Math.sqrt(eDiff*eDiff+nDiff*nDiff);
	}
	
	private static class LineComparator implements Comparator {

		public int compare(Object o1, Object o2) {
			String s1 = (String)o1;
			String s2 = (String)o2;
			
			String[] p1 = s1.split(" ");
			String[] p2 = s2.split(" ");
			
			int res1 = Integer.parseInt(p1[p1.length-1]);
			int res2 = Integer.parseInt(p2[p2.length-1]);
			
			if (res1>res2) {
				return res2-res1;
			} else if (res2>res1) {
				return res2-res1;
			} else {
				return p1[0].compareTo(p2[0]); 
			}
		}
		
	}
}
