package ch.ethz.er.qfparser;

import org.postgresql.geometric.PGpoint;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by anton on 27.01.16.
 */

public class PostgreSqlExporter {

	private static final String HOST = "localhost";
	private static final String DB_NAME = "quakefinder";
	private static final String USER = "anton";
	private static final String PASSWORD = "admin";

	private String url;


	public PostgreSqlExporter() {
		try {
			url = "jdbc:postgresql://" + HOST + "/" + DB_NAME;
			Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
			QFParser.logException(e);
		}
	}


	public void init() throws SQLException {
		try (Connection connection = DriverManager.getConnection(url, USER, PASSWORD)) {
			String sqlCreate = "CREATE TABLE IF NOT EXISTS datasets (\n" +
					"  id SERIAL PRIMARY KEY,\n" +
					"  name TEXT,\n" +
					"  description TEXT,\n" +
					"  start TIMESTAMP WITHOUT TIME ZONE,\n" +
					"  \"end\" TIMESTAMP WITHOUT TIME ZONE\n" +
					");\n" +
					"COMMENT ON TABLE datasets IS 'QuakeFinder datasets';\n" +
					"\n" +
					"CREATE TABLE IF NOT EXISTS sites (\n" +
					"  id SERIAL PRIMARY KEY,\n" +
					"  dataset_id INTEGER NOT NULL,\n" +
					"  coordinate POINT,\n" +
					"  sample_rate SMALLINT,\n" +
					"  name TEXT,\n" +
					"  description TEXT,\n" +
					"  FOREIGN KEY (dataset_id) REFERENCES datasets (id)\n" +
					"  MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE\n" +
					");\n" +
					"CREATE INDEX IF NOT EXISTS sites_dataset_id_index ON sites USING BTREE (dataset_id);\n" +
					"COMMENT ON TABLE sites IS 'QuakeFinder sensor site';\n" +
					"\n" +
					"CREATE TABLE IF NOT EXISTS em_signals (\n" +
					"  id SERIAL PRIMARY KEY,\n" +
					"  site_id INTEGER NOT NULL,\n" +
					"  hour SMALLINT NOT NULL,\n" +
					"  channel1 INTEGER[],\n" +
					"  channel2 INTEGER[],\n" +
					"  channel3 INTEGER[],\n" +
					"  FOREIGN KEY (site_id) REFERENCES sites (id)\n" +
					"  MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE\n" +
					");\n" +
					"CREATE INDEX IF NOT EXISTS em_signals_site_id_index ON em_signals USING BTREE (site_id);\n" +
					"CREATE INDEX IF NOT EXISTS em_signals_hour_index ON em_signals USING BTREE (hour);\n" +
					"COMMENT ON TABLE em_signals IS 'Electromagnetic measurements';";

			try (Statement statement = connection.createStatement()) {
				boolean result = statement.execute(sqlCreate);
			}
		}
	}

	public void export(DataSet dset) throws Exception {
		String seedName = String.valueOf(dset.seed);

		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ft.setTimeZone(TimeZone.getTimeZone("UTC"));

		Date startDate = ft.parse(dset.gtfStartTime);
		Date endDate = ft.parse(dset.gtfEQtime);

		try (Connection connection = DriverManager.getConnection(url, USER, PASSWORD)) {
			int datasetId = 0;
			String sqlInsertDataset = "INSERT INTO datasets (name, start, \"end\") VALUES (?, ?, ?)";
			try (PreparedStatement preparedStatement = connection.prepareStatement(sqlInsertDataset, Statement.RETURN_GENERATED_KEYS)) {
				preparedStatement.setString(1, seedName);
				preparedStatement.setTimestamp(2, new java.sql.Timestamp(startDate.getTime()));
				preparedStatement.setTimestamp(3, new java.sql.Timestamp(endDate.getTime()));
				preparedStatement.executeUpdate();

				try (ResultSet rs = preparedStatement.getGeneratedKeys()) {
					if (rs != null && rs.next()) {
						datasetId = rs.getInt(1);
					}
				}
			}

			System.out.println(String.format("Sample rate: %d", dset.sampleRate));
			System.out.println(String.format("Number of sites: %d", dset.numOfSites));

			double[] sitesData = new double[dset.numOfSites * 2];
			int[] sitesIds = new int[dset.numOfSites];

			String sqlInsertSite = "INSERT INTO sites (dataset_id, name, sample_rate, coordinate) VALUES (?, ?, ?, ?)";

			try (PreparedStatement preparedStatement = connection.prepareStatement(sqlInsertSite, Statement.RETURN_GENERATED_KEYS)) {
				for (int i = 0; i < dset.numOfSites; i++) {
					sitesData[i * 2] = dset.sites[i].latitude;
					sitesData[i * 2 + 1] = dset.sites[i].longitude;

					preparedStatement.setInt(1, datasetId);
					preparedStatement.setString(2, String.valueOf(i + 1));
					preparedStatement.setInt(3, dset.sampleRate);
					preparedStatement.setObject(4, new PGpoint(dset.sites[i].latitude, dset.sites[i].longitude));
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();
				try (ResultSet rs = preparedStatement.getGeneratedKeys()) {
					int i = 0;
					while (rs.next()) {
						sitesIds[i] = rs.getInt(1);
						i++;
					}
				}
			}

			for (int hour = 0; hour < dset.gtfEQHour; hour++) {
				String hourName = String.valueOf(hour);
				int[] hourlyData = null;
				try {
					hourlyData = dset.loadHour(dset.dataFolder, hour);
				} catch (Exception e) {
					System.err.println("WARNING: Missing data for hour " + hour);
					continue;
				}
				double[] otherQuakes = dset.getOtherQuakes(hour);
				System.out.println(String.format("Hour: %d", hour));
				System.out.println(String.format("Hour Array Lenght: %d", hourlyData.length));


				String sqlInsertEmSignals = "INSERT INTO em_signals (site_id, hour, channel1, channel2, channel3) VALUES (?, ?, ?, ?, ?)";

				try (PreparedStatement preparedStatement = connection.prepareStatement(sqlInsertEmSignals)) {
					for (int site = 0; site < dset.numOfSites; site++) {
						Object[] channel1 = new Object[dset.sampleRate * 3600];
						Object[] channel2 = new Object[dset.sampleRate * 3600];
						Object[] channel3 = new Object[dset.sampleRate * 3600];

						for (int i = 0; i < dset.sampleRate * 3600; i++) {
							channel1[i] = hourlyData[site * (dset.sampleRate * 3600 * 3) + 0 * (dset.sampleRate * 3600) + i];
							channel2[i] = hourlyData[site * (dset.sampleRate * 3600 * 3) + 1 * (dset.sampleRate * 3600) + i];
							channel3[i] = hourlyData[site * (dset.sampleRate * 3600 * 3) + 2 * (dset.sampleRate * 3600) + i];
						}

						preparedStatement.setInt(1, sitesIds[site]);
						preparedStatement.setInt(2, hour);
						preparedStatement.setArray(3, connection.createArrayOf("INTEGER", channel1));
						preparedStatement.setArray(4, connection.createArrayOf("INTEGER", channel2));
						preparedStatement.setArray(5, connection.createArrayOf("INTEGER", channel3));

						preparedStatement.executeUpdate();
					}
				}
			}
		}
	}

}
