package org.scec.cme.cybershake.vs30;

import org.opensha.commons.data.siteData.impl.ThompsonVs30_2020;
import org.opensha.commons.data.siteData.impl.WaldAllenGlobalVs30;
import org.opensha.commons.data.siteData.impl.WillsMap2006;
import org.opensha.commons.geo.Location;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Looks up Vs30 for a location with OpenSHA and prints one JSON line: {"vs30": 733.4, "source":
 * "Thompson et al. (2020)"}.
 *
 * <p>Usage: Vs30Cli [--source thompson|wills] [--wills-file FILE] LAT LON
 *
 * <p>Thompson (2020) is the default, falling back to the Wald &amp; Allen topographic slope where
 * Thompson has no value. Exit codes: 1 bad usage, 2 lookup failed.
 */
public class Vs30Cli {
    private static final String WILLS = "Wills (2006)";
    private static final String THOMPSON = "Thompson et al. (2020)";
    private static final String WALD_ALLEN = "Topographic Slope (Wald & Allen 2008)";

    public static void main(String[] args) {
        String source = "thompson";
        String willsFile = null;
        double[] latLon = new double[2];
        int nPos = 0;
        try {
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("--source")) {
                    source = args[++i];
                } else if (args[i].equals("--wills-file")) {
                    willsFile = args[++i];
                } else if (nPos < 2) {
                    latLon[nPos++] = Double.parseDouble(args[i]);
                } else {
                    nPos = 3;
                }
            }
        } catch (RuntimeException ex) {
            nPos = -1;
        }
        boolean willsOk = source.equals("wills") ? willsFile != null : source.equals("thompson");
        if (nPos != 2 || !willsOk) {
            System.err.println(
                    "Usage: Vs30Cli [--source thompson|wills] [--wills-file FILE (required for"
                            + " wills)] LAT LON");
            System.exit(1);
        }

        // OpenSHA may log to stdout; keep stdout clean for the JSON result.
        PrintStream stdout = System.out;
        System.setOut(System.err);
        double vs30 = Double.NaN;
        String sourceString = null;
        try {
            Location loc = new Location(latLon[0], latLon[1]);
            if (source.equals("wills")) {
                vs30 = new WillsMap2006(willsFile).getValue(loc);
                sourceString = WILLS;
            } else {
                vs30 = new ThompsonVs30_2020().getValue(loc);
                sourceString = THOMPSON;
                // If not in Thompson (2020), default to topo slope
                if (Double.isNaN(vs30)) {
                    vs30 = new WaldAllenGlobalVs30().getValue(loc);
                    sourceString = WALD_ALLEN;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(2);
        }
        if (Double.isNaN(vs30)) {
            System.err.println("No Vs30 value found for " + latLon[0] + ", " + latLon[1]);
            System.exit(2);
        }
        stdout.println("{\"vs30\": " + vs30 + ", \"source\": \"" + sourceString + "\"}");
    }
}
