package ch.ethz.er.qfparser;

import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by anton on 27.01.16.
 */

public class CsvExporter {

	private static final String DATASETS_FILENAME = "datasets.csv";
	private static final String SITES_FILENAME = "sites.csv";
	private static final String MEASUREMENTS_FILENAME = "measurements.csv";

	private String outputFolder;


	public CsvExporter() {
	}


	public void init(String folder) throws Exception {
		outputFolder = folder;
	}

	public void export(DataSet dset) throws Exception {
		String seedName = String.valueOf(dset.seed);

		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ft.setTimeZone(TimeZone.getTimeZone("UTC"));

		Date startDate = ft.parse(dset.gtfStartTime);
		Date endDate = ft.parse(dset.gtfEQtime);

		System.out.println(String.format("Sample rate: %d", dset.sampleRate));
		System.out.println(String.format("Number of sites: %d", dset.numOfSites));

		try (ICsvListWriter writer = new CsvListWriter(new FileWriter(outputFolder + DATASETS_FILENAME), CsvPreference.TAB_PREFERENCE)) {
			final String[] header = new String[] { "Seed", "StartDate", "EndDate" };
			writer.writeHeader(header);
			writer.write(seedName, startDate, endDate);
		}

		double[] sitesData = new double[dset.numOfSites * 2];
		int[] sitesIds = new int[dset.numOfSites];

		try (ICsvListWriter writer = new CsvListWriter(new FileWriter(outputFolder + SITES_FILENAME), CsvPreference.TAB_PREFERENCE)) {
			final String[] header = new String[] { "Seed", "Site", "SampleRate", "Latitude", "Longitude" };
			writer.writeHeader(header);

			for (int i = 0; i < dset.numOfSites; i++) {
				sitesData[i * 2] = dset.sites[i].latitude;
				sitesData[i * 2 + 1] = dset.sites[i].longitude;

				writer.write(seedName, i + 1, dset.sampleRate, dset.sites[i].latitude, dset.sites[i].longitude);
			}
		}

		try (ICsvListWriter writer = new CsvListWriter(new FileWriter(outputFolder + MEASUREMENTS_FILENAME), CsvPreference.TAB_PREFERENCE)) {
			final String[] header = new String[] { "Site", "Hour", "Index", "Channel1", "Channel2", "Channel3" };
			writer.writeHeader(header);

			for (int hour = 0; hour < dset.gtfEQHour; hour++) {
				String hourName = String.valueOf(hour);
				int[] hourlyData = null;
				try {
					hourlyData = dset.loadHour(dset.dataFolder, hour);
				} catch (Exception e) {
					System.out.println("WARNING: Missing data for hour " + hour);
					continue;
				}
				double[] otherQuakes = dset.getOtherQuakes(hour);

				System.out.println(String.format("Hour: %d", hour));
				System.out.println(String.format("Hour Array Lenght: %d", hourlyData.length));

				for (int site = 0; site < dset.numOfSites; site++) {
					Object[] channel1 = new Object[dset.sampleRate * 3600];
					Object[] channel2 = new Object[dset.sampleRate * 3600];
					Object[] channel3 = new Object[dset.sampleRate * 3600];

					for (int i = 0; i < dset.sampleRate * 3600; i++) {
						channel1[i] = hourlyData[site * (dset.sampleRate * 3600 * 3) + 0 * (dset.sampleRate * 3600) + i];
						channel2[i] = hourlyData[site * (dset.sampleRate * 3600 * 3) + 1 * (dset.sampleRate * 3600) + i];
						channel3[i] = hourlyData[site * (dset.sampleRate * 3600 * 3) + 2 * (dset.sampleRate * 3600) + i];

						writer.write(sitesIds[site], hour, i, channel1[i], channel2[i], channel3[i]);
					}
				}
			}
		}
	}

}
