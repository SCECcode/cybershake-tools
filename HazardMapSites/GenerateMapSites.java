import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
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


public class GenerateMapSites {
	//Southern California settings
	/*public static double[] westCorner = {34.13, -119.38};
	public static double[] eastCorner = {34.19, -116.85};
	public static double[] northCorner = {35.08, -118.75};
	public static double[] southCorner = {33.25, -117.5};*/
	
	//All-california settings
	public static double[] westCorner = {40.5, -128.00};
	public static double[] eastCorner = {34.3, -112.1};
	public static double[] northCorner = {44.05, -122.00};
	public static double[] southCorner = {30.53, -118.125};

	
	public static final int SPHEROID = 20;
	public static final int UTM_ZONE = 11;
	
	public static final int ERF_ID = 35;
	public static final int CUTOFF = 200;
	
	private static double[][] caOutline = null;
	
	public static double SPACING = 5000.0; //m
	
	public static String FILENAME = "CyberShake_CA_map_sites";
	
	public static Polygon caPoly = null;
	
	public static void main(String[] args) {
		Document[] xmlOuts = {null, null, null};
		try {
			for(int i=0; i<3; i++) {
				xmlOuts[i] = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
			}
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		Element[] roots = new Element[3];
		Element[] docs = new Element[3];
		
		for (int i=0; i<3; i++) {
			roots[i] = xmlOuts[i].createElementNS("http://earth.google.com/kml/2.2", "kml");
			docs[i] = xmlOuts[i].createElement("Document");
			printHeader(xmlOuts[i], docs[i]);
		}
		
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

		System.out.println(westCornerUTM[0] + " " + westCornerUTM[1]);
		System.out.println(northCornerUTM[0] + " " + northCornerUTM[1]);
		System.out.println(eastCornerUTM[0] + " " + eastCornerUTM[1]);
		
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
		
		
		ArrayList<String> lines = new ArrayList<String>();
		
		int count = 0;
		int index = -1;
		
		int[] dist = {5, 10, 20};
		int[] counts = {0, 0, 0};
		
		String[] styles = {"#Gridpoints5", "#Gridpoints10", "#Gridpoints20"};
		
		for(int i=1; i<xSteps; i++) {
			start[0] += deltaN;
			start[1] -= deltaE;
			for(int j=1; j<ySteps; j++) {
//				System.out.println("east: " + (start[0]+j*deltaE) + " north: " + (start[1]+j*deltaN));
				double[] ll = ConversionServices.getLatLong(start[0]+j*deltaE, start[1]+j*deltaN, SPHEROID, UTM_ZONE, ConversionServices.NORTHERN_HEMISPHERE);
				
				if (!insideBoundary(ll)) continue;
				
				if (i%4==0 && j%4==0) {
					index = 2;
				} else if (i%2==0 && j%2==0) {
					index = 1;						
				} else {
					index = 0;
				}
					
				for (int k=index; k>=0; k--) {
					
				counts[k]++;
				placemark = xmlOuts[k].createElement("Placemark");
//					name = xmlOuts[k].createElement("name");
//						nameText = xmlOuts[k].createTextNode(String.format("s%05d", count));
//						name.appendChild(nameText);
//					placemark.appendChild(name);
					styleUrl = xmlOuts[k].createElement("styleUrl");
						styleUrlText = xmlOuts[k].createTextNode(styles[k]);
						styleUrl.appendChild(styleUrlText);
					placemark.appendChild(styleUrl);
					point = xmlOuts[k].createElement("Point");
						coordinates = xmlOuts[k].createElement("coordinates");
							coordinatesText = xmlOuts[k].createTextNode(String.format("%.5f, %.5f, 0", ll[1], ll[0]));
							coordinates.appendChild(coordinatesText);
						point.appendChild(coordinates);
					placemark.appendChild(point);
					docs[k].appendChild(placemark);
					
				}

				lines.add(String.format("s%05d", count) + " \"" + String.format("s%05d", count) + "\" " + String.format("%.5f", ll[0]) + " " + String.format("%.5f", ll[1]) + " " + CUTOFF + " " + ERF_ID + " " + dist[index]);
				
				count++;
			}
		}
		
		for (int i=0; i<3; i++) {
			roots[i].appendChild(docs[i]);
			xmlOuts[i].appendChild(roots[i]);
		
		try {
			TransformerFactory factory = TransformerFactory.newInstance();
			factory.setAttribute("indent-number", 4);
			Transformer t = factory.newTransformer();
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			
			Source src = new DOMSource(xmlOuts[i]);
			StreamResult out = new StreamResult(new FileWriter(FILENAME + "_" + dist[i] + ".kml"));
			t.transform(src, out);
			
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerFactoryConfigurationError e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		} //close for loop
		
		Collections.sort(lines, new LineComparator());
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(FILENAME + ".txt"));
			for (String s: lines) {
				bw.write(s + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		System.out.println("20 km: " + counts[2] + " sites\n10 km: " + counts[1] + " sites\n5 km: " + counts[0] + " sites");
	}

	private static boolean insideBoundary(double[] ll) {
//		if (caOutline==null) {
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
//			caOutline = edge.toArray(new double[][]{});
			caPoly = new Polygon();
			for (double[] point: edge) {
				caPoly.addPoint((int)(point[0]*100), (int)(point[1]*100));
			}
		}
	
		if (caPoly.contains((int)(ll[0]*100), (int)(ll[1]*100))) {
			return true;
		}
		return false;
		
//		int compareIndex = 0;
//		int flag = 0;
//		for (int i=0; i<caOutline.length; i++) {
//			if (caOutline[i][0]>ll[0] && flag==0) {
//				flag = 1;
//				compareIndex = i;
//			} else if (caOutline[i][0]<ll[0] && flag==1 && caOutline[i][1]>ll[1]) {
//				flag = 0;
//			}
//		}
//		//try midpoint
//		double midLat = (caOutline[compareIndex][0] + caOutline[compareIndex-1][0]) * .5;
//		double midLon = (caOutline[compareIndex][1] + caOutline[compareIndex-1][1]) * .5;
//		if (ll[1]<midLon) {
//			return false;
//		}
//		return true;
	}

	private static void printHeader(Document doc, Element root) {
		Element gridpointsStyle = doc.createElement("Style");
		gridpointsStyle.setAttribute("id", "Gridpoints20");
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
		
		gridpointsStyle = doc.createElement("Style");
		gridpointsStyle.setAttribute("id", "Gridpoints10");
			iconStyle = doc.createElement("IconStyle");
				scale = doc.createElement("scale");
					scaleText = doc.createTextNode("0.07");
					scale.appendChild(scaleText);
				iconStyle.appendChild(scale);
				icon = doc.createElement("Icon");
					url = doc.createElement("href");
						urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/yellow-dot.png");
						url.appendChild(urlText);
					icon.appendChild(url);
				iconStyle.appendChild(icon);
				gridpointsStyle.appendChild(iconStyle);
		root.appendChild(gridpointsStyle);
		
		gridpointsStyle = doc.createElement("Style");
		gridpointsStyle.setAttribute("id", "Gridpoints5");
			iconStyle = doc.createElement("IconStyle");
				scale = doc.createElement("scale");
					scaleText = doc.createTextNode("0.04");
					scale.appendChild(scaleText);
				iconStyle.appendChild(scale);
				icon = doc.createElement("Icon");
					url = doc.createElement("href");
						urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/green-dot.png");
						url.appendChild(urlText);
					icon.appendChild(url);
				iconStyle.appendChild(icon);
				gridpointsStyle.appendChild(iconStyle);
		root.appendChild(gridpointsStyle);
		
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
