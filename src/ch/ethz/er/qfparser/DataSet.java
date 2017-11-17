package ch.ethz.er.qfparser;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by anton on 21.01.16.
 */

public class DataSet {
    int seed = 0;
    String dataFolder;

    Coordinate[] sites;
    Quake[] quakes;
    int sampleRate;
    int numOfSites;
    int numOfEMA;
    int numOfQuakes;

    int[] rawData = null;
    byte[] result = null;
    double[] EMA = null;

    String gtfStartTime, gtfEQtime;
    double gtfMagnitude, gtfLatitude, gtfLongitude, gtfDistToEQ, gtfEQSec;
    int gtfEQHour, gtfSite;

    DocumentBuilderFactory docBuilderFactory = null;
    DocumentBuilder docBuilder = null;


    public DataSet(String folder, int currentSeed) throws Exception {
        dataFolder = folder;
        seed = currentSeed;

        docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilder = docBuilderFactory.newDocumentBuilder();
        loadSiteInfo(dataFolder + "SiteInfo.xml");
        loadEarthMagneticActivity(dataFolder + "Kp.xml");
        loadOtherQuakes(dataFolder + "Quakes.xml");
    }


    public void loadSiteInfo(String sXmlFile) throws Exception {
        // load site information
        Document doc = docBuilder.parse(new File(sXmlFile));
        doc.getDocumentElement().normalize();

        NodeList listOfSites = doc.getElementsByTagName("Site");
        numOfSites = listOfSites.getLength();
        QFParser.printMessage("Total number of sites: " + numOfSites);

        sites = new Coordinate[numOfSites];
        for (int s = 0; s < numOfSites; s++) {
            Element siteElement = (Element) listOfSites.item(s);
            if (s == 0) {
                sampleRate = Integer.parseInt(siteElement.getAttribute("sample_rate"));
                QFParser.printMessage("Sample Rate = " + sampleRate);
            }
            double lat = Double.parseDouble(siteElement.getAttribute("latitude"));
            double lon = Double.parseDouble(siteElement.getAttribute("longitude"));
            sites[s] = new Coordinate(lat, lon);
            QFParser.printMessage("site " + s + ": " + lat + "," + lon);
        }
        // allocate memory for hourly data
        rawData = new int[numOfSites * 3 * 3600 * sampleRate];
        result = new byte[3600 * sampleRate * 4];
    }

    public void loadEarthMagneticActivity(String sXmlFile) throws Exception {
        // load earth magnetic activity
        Document doc = docBuilder.parse(new File(sXmlFile));
        doc.getDocumentElement().normalize();

        NodeList listOfEMA = doc.getElementsByTagName("kp_hr");
        numOfEMA = listOfEMA.getLength();
        QFParser.printMessage("Total number of EM activities: " + numOfEMA);

        EMA = new double[numOfEMA];
        for (int i = 0; i < numOfEMA; i++) {
            EMA[i] = Double.parseDouble(listOfEMA.item(i).getFirstChild().getNodeValue());
        }
    }

    public void loadOtherQuakes(String sXmlFile) throws Exception {
        // load earth magnetic activity
        Document doc = docBuilder.parse(new File(sXmlFile));
        doc.getDocumentElement().normalize();

        NodeList listOfQuakes = doc.getElementsByTagName("Quake");
        numOfQuakes = listOfQuakes.getLength();
        QFParser.printMessage("Total number of other quakes: " + numOfQuakes);

        quakes = new Quake[numOfQuakes];
        for (int i = 0; i < numOfQuakes; i++) {
            Element quakeElement = (Element) listOfQuakes.item(i);
            int secs = Integer.parseInt(quakeElement.getAttribute("secs"));
            double lat = Double.parseDouble(quakeElement.getAttribute("latitude"));
            double lon = Double.parseDouble(quakeElement.getAttribute("longitude"));
            double depth = Double.parseDouble(quakeElement.getAttribute("depth"));
            double mag = Double.parseDouble(quakeElement.getAttribute("magnitude"));
            quakes[i] = new Quake(secs, new Coordinate(lat, lon), depth, mag);
        }
    }

    public double[] getOtherQuakes(int hour) {
        int hStart = hour * 3600;
        int hEnd = (hour + 1) * 3600;
        int numInHour = 0;
        for (int i = 0; i < numOfQuakes; i++) {
            if (quakes[i].timeSecs >= hStart && quakes[i].timeSecs < hEnd) numInHour++;
        }
        double[] oQuake = new double[numInHour * 5];
        int q = 0;
        for (int i = 0; i < numOfQuakes; i++) {
            if (quakes[i].timeSecs >= hStart && quakes[i].timeSecs < hEnd) {
                oQuake[q] = quakes[i].coordinate.latitude;
                oQuake[q + 1] = quakes[i].coordinate.longitude;
                oQuake[q + 2] = quakes[i].depth;
                oQuake[q + 3] = quakes[i].magnitude;
                oQuake[q + 4] = quakes[i].timeSecs;
                q += 5;
            }
        }
        return oQuake;
    }

    public void readGTF() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(dataFolder + "gtf.csv"));
        int numOfCases = Integer.parseInt(br.readLine());
        for (int i = 0; i < numOfCases; i++) {
            String s = br.readLine();
            String[] token = s.split(",");
            int setID = Integer.parseInt(token[0]);
            if (setID == QFParser.seed) {
                gtfStartTime = token[1];
                gtfEQtime = token[2];
                gtfMagnitude = Double.parseDouble(token[3]);
                gtfLatitude = Double.parseDouble(token[4]);
                gtfLongitude = Double.parseDouble(token[5]);
                gtfSite = Integer.parseInt(token[6]);
                gtfDistToEQ = Double.parseDouble(token[7]);
                // Calculate number of hours till EQ
                SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                ft.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date date1 = ft.parse(gtfStartTime);
                long startMSec = date1.getTime();
                Date date2 = ft.parse(gtfEQtime);
                long eqMSec = date2.getTime();
                gtfEQSec = (eqMSec - startMSec) / 1000;
                gtfEQHour = (int) (gtfEQSec / (60 * 60));
                QFParser.printMessage("Quake happened at hour = " + gtfEQHour + " and second = " + gtfEQSec + " at row " + (gtfEQSec - 3600.0 * gtfEQHour) * sampleRate);
                break;
            }
        }
        br.close();
    }

    public int[] loadHour(String sFolder, int h) throws Exception {
        String fname = sFolder + "test" + QFParser.seed + "_" + h + ".bin";
        QFParser.printMessage("loading " + fname);
        int[] dt = new int[numOfSites * 3 * 3600 * sampleRate];
        File file = new File(fname);
        InputStream input = new BufferedInputStream(new FileInputStream(file));
        DataInputStream din = new DataInputStream(input);
        int prev = -1;
        int diff;
        for (int i = 0; i < dt.length; i++) {
            int b1 = (int) din.readByte();
            if (b1 < 0) b1 += 256;
            if ((b1 & 3) == 1) {
                diff = (b1 >> 2) - (1 << 5);
                dt[i] = prev + diff;
            } else if ((b1 & 3) == 2) {
                int b2 = (int) din.readByte();
                if (b2 < 0) b2 += 256;
                diff = ((b1 + (b2 << 8)) >> 2) - (1 << 13);
                dt[i] = prev + diff;
            } else if ((b1 & 3) == 3) {
                int b2 = (int) din.readByte();
                if (b2 < 0) b2 += 256;
                int b3 = (int) din.readByte();
                if (b3 < 0) b3 += 256;
                diff = ((b1 + (b2 << 8) + (b3 << 16)) >> 2) - (1 << 21);
                dt[i] = prev + diff;
            } else {
                int b2 = (int) din.readByte();
                if (b2 < 0) b2 += 256;
                int b3 = (int) din.readByte();
                if (b3 < 0) b3 += 256;
                int b4 = (int) din.readByte();
                if (b4 < 0) b4 += 256;
                dt[i] = ((b1 + (b2 << 8) + (b3 << 16) + (b4 << 24)) >> 2) - 1;
            }
            prev = dt[i];
        }
        input.close();
        return dt;
    }
}
