package ch.ethz.er.qfparser;

import java.time.Duration;
import java.time.Instant;

public class QFParser {

	public static boolean debug = true;
	public static String folder = "data";
	public static String format = "csv";
	public static int seed = 0;

	public static void printMessage(String s) {
		if (debug) {
			System.out.println(s);
		}
	}

	public static void logException(Exception e) {
		System.out.println("ERROR: " + e.getMessage());
		e.printStackTrace();
	}


	public double doExec() throws Exception {

		String dataFolder = folder + seed + "/";

		try {

			DataSet dset = new DataSet(dataFolder, seed);
			dset.readGTF();

			switch (format) {
				case "postgresql": {
					PostgreSqlExporter exporter = new PostgreSqlExporter();
					exporter.init();
					exporter.export(dset);
					break;
				}

				case "csv": {
					CsvExporter exporter = new CsvExporter();
					exporter.init(dataFolder);
					exporter.export(dset);
					break;
				}
			}

			return 0;

		} catch (Exception e) {
			QFParser.logException(e);
		}
		return -1.0;
	}


	public static void main(String[] args) throws Exception {

		Instant start = Instant.now();

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-seed")) {
				seed = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-silent")) {
				debug = false;
			} else if (args[i].equals("-format")) {
				format = args[++i];
			} else {
				System.out.println("WARNING: unknown argument " + args[i] + ".");
			}
		}

		try {
			if (folder != null) {
				double score = new QFParser().doExec();
				System.out.println("Score = " + score);
			} else {
				System.out.println("WARNING: nothing to do for this combination of arguments.");
			}
		} catch (Exception e) {
			QFParser.logException(e);
		}

		Instant end = Instant.now();
		System.out.println(Duration.between(start, end)); // prints PT1M3.553S
	}
}
