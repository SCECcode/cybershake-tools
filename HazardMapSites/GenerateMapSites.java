import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
	public static double[] westCorner = {34.13, -119.38};
	public static double[] eastCorner = {34.19, -116.85};
	public static double[] northCorner = {35.08, -118.75};
	public static double[] southCorner = {33.25, -117.5};
	
	public static final int SPHEROID = 20;
	public static final int UTM_ZONE = 11;
	
	public static double SPACING = 5000.0; //m
	
	public static String FILENAME = "CyberShake_map_sites.kml";
	
	public static void main(String[] args) {
		Document xmlOut = null;
		try {
			xmlOut = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		Element root = xmlOut.createElementNS("http://earth.google.com/kml/2.2", "kml");
		Element doc = xmlOut.createElement("Document");
		
		printHeader(xmlOut, doc);
		
		double[] westCornerUTM = ConversionServices.getUTMwithZone(westCorner[0], westCorner[1], SPHEROID, UTM_ZONE);
		double[] eastCornerUTM = ConversionServices.getUTMwithZone(eastCorner[0], eastCorner[1], SPHEROID, UTM_ZONE);
		double[] northCornerUTM = ConversionServices.getUTMwithZone(northCorner[0], northCorner[1], SPHEROID, UTM_ZONE);
		double[] southCornerUTM = ConversionServices.getUTMwithZone(southCorner[0], southCorner[1], SPHEROID, UTM_ZONE);

		System.out.println(westCornerUTM[0] + " " + westCornerUTM[1]);
		System.out.println(northCornerUTM[0] + " " + northCornerUTM[1]);
		
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
		
		for(int i=1; i<xSteps; i++) {
			start[0] += deltaN;
			start[1] -= deltaE;
			for(int j=1; j<ySteps; j++) {
//				System.out.println("east: " + (start[0]+j*deltaE) + " north: " + (start[1]+j*deltaN));
				double[] ll = ConversionServices.getLatLong(start[0]+j*deltaE, start[1]+j*deltaN, SPHEROID, UTM_ZONE, ConversionServices.NORTHERN_HEMISPHERE);
//				System.out.println("lat: " + ll[0] + " lon: " + ll[1]);
				placemark = xmlOut.createElement("Placemark");
					name = xmlOut.createElement("name");
						nameText = xmlOut.createTextNode(String.format("s%03d", ((i-1)*ySteps+(j-1))));
						name.appendChild(nameText);
					placemark.appendChild(name);
					styleUrl = xmlOut.createElement("styleUrl");
						styleUrlText = xmlOut.createTextNode("#Gridpoints");
						styleUrl.appendChild(styleUrlText);
					placemark.appendChild(styleUrl);
					point = xmlOut.createElement("Point");
						coordinates = xmlOut.createElement("coordinates");
							coordinatesText = xmlOut.createTextNode(String.format("%.5f, %.5f, 0", ll[1], ll[0]));
							coordinates.appendChild(coordinatesText);
						point.appendChild(coordinates);
					placemark.appendChild(point);
				doc.appendChild(placemark);
			}
		}
		
		root.appendChild(doc);
		xmlOut.appendChild(root);
		
		try {
			TransformerFactory factory = TransformerFactory.newInstance();
			factory.setAttribute("indent-number", 4);
			Transformer t = factory.newTransformer();
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			
			Source src = new DOMSource(xmlOut);
			StreamResult out = new StreamResult(new FileWriter(FILENAME));
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
		
	}

	private static void printHeader(Document doc, Element root) {
		Element gridpointsStyle = doc.createElement("Style");
		gridpointsStyle.setAttribute("id", "Gridpoints");
			Element iconStyle = doc.createElement("IconStyle");
				Element scale = doc.createElement("Scale");
					Text scaleText = doc.createTextNode("0.4");
					scale.appendChild(scaleText);
				iconStyle.appendChild(scale);
				Element icon = doc.createElement("Icon");
					Element url = doc.createElement("href");
						Text urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/red-dot.png");
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
}
