import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Text;


public class MakeKMLFromDBSites {
	private static String DB_SERVER = "focal.usc.edu";
	private static String DB = "CyberShake";
	private static String USERNAME = "cybershk_ro";
	private static String PASSWORD = "CyberShake2007";
	
	private static ArrayList<Site> sites = new ArrayList<Site>();
	
	private static class Site {
		public String name;
		public double lat;
		public double lon;
	}
	
	public static void main(String[] args) {
		Connection conn = dbConnect();
		populateSites(conn);
		createKML();
	}

	private static void createKML() {
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
		
		for (Site s: sites) {
		Element placemark = xmlOut.createElement("Placemark");
			Element name = xmlOut.createElement("name");
			Text nameText = xmlOut.createTextNode(s.name);
			name.appendChild(nameText);
		placemark.appendChild(name);
		Element styleUrl = xmlOut.createElement("styleUrl");
			Text styleUrlText = xmlOut.createTextNode("#Gridpoints");
			styleUrl.appendChild(styleUrlText);
		placemark.appendChild(styleUrl);
		Element point = xmlOut.createElement("Point");
			Element coordinates = xmlOut.createElement("coordinates");
				Text coordinatesText = xmlOut.createTextNode(String.format("%.5f, %.5f, 0", s.lon, s.lat));
				coordinates.appendChild(coordinatesText);
			point.appendChild(coordinates);
		placemark.appendChild(point);
		doc.appendChild(placemark);
		}
		
		root.appendChild(doc);
		xmlOut.appendChild(root);
		
		try {
			TransformerFactory factory = TransformerFactory.newInstance();
			factory.setAttribute("indent-number", 4);
			Transformer t = factory.newTransformer();
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			
			Source src = new DOMSource(xmlOut);
			StreamResult out = new StreamResult(new FileWriter("sites.kml"));
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
					Text scaleText = doc.createTextNode("1.5");
					scale.appendChild(scaleText);
				iconStyle.appendChild(scale);
				Element icon = doc.createElement("Icon");
					Element url = doc.createElement("href");
						Text urlText = doc.createTextNode("http://maps.google.com/mapfiles/ms/icons/blue-dot.png");
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
	}
		
	private static void populateSites(Connection conn) {
		try {
			Statement stat = conn.createStatement();
			String query = "select distinct S.CS_Short_Name, S.CS_Site_Lat, S.CS_Site_Lon " +
				"from CyberShake_Site_Types T, CyberShake_Sites S, CyberShake_Runs R, Hazard_Curves C " +
				"where C.Run_ID=R.Run_ID and R.SGT_Variation_ID=5 and R.Rup_Var_Scenario_ID=3 " +
				"and S.CS_Site_ID=R.Site_ID and S.CS_Site_Type_ID=T.CS_Site_Type_ID and T.CS_Site_Type_Short_Name!=\"TEST\"";
			ResultSet rs = stat.executeQuery(query);
			rs.first();
			while (!rs.isAfterLast()) {
				Site s = new Site();
				s.name = rs.getString("S.CS_Short_Name");
				s.lat = rs.getDouble("S.CS_Site_Lat");
				s.lon = rs.getDouble("S.CS_Site_Lon");
				sites.add(s);
				rs.next();
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	private static Connection dbConnect() {
	   String url = "jdbc:mysql://"+DB_SERVER + "/"+DB;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, USERNAME, PASSWORD);
			return conn;
		} catch (SQLException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
