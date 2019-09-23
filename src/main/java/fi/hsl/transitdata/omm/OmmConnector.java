package fi.hsl.transitdata.omm;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.PulsarClientException;
import fi.hsl.common.files.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class OmmConnector {

    private static final Logger log = LoggerFactory.getLogger(OmmConnector.class);

    private final Connection dbConnection;
    private OmmCancellationHandler handler;
    private final String queryString;
    private final CancellationSourceType sourceType;
    private final String timezone;

    private OmmConnector(PulsarApplicationContext context, Connection connection, CancellationSourceType type) {
        handler = new OmmCancellationHandler(context);
        dbConnection = connection;
        queryString = createQuery(type);
        sourceType = type;
        timezone = context.getConfig().getString("omm.timezone");
        log.info("Using timezone " + timezone);
    }

    public static OmmConnector newInstance(PulsarApplicationContext context, String jdbcConnectionString, CancellationSourceType sourceType) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmConnector(context, connection, sourceType);
    }

    private String createQuery(CancellationSourceType sourceType) {
        InputStream stream = (sourceType == CancellationSourceType.FROM_HISTORY)
                ? getClass().getResourceAsStream("/cancellations_history_current_future.sql")
                : (sourceType == CancellationSourceType.FROM_NOW)
                    ? getClass().getResourceAsStream("/cancellations_current_future.sql")
                    : null;
        try {
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }

    static String localDatetimeAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(instant.atZone(ZoneId.of(zoneId)));
    }

    static String localDateAsString(Instant instant, String zoneId) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(instant.atZone(ZoneId.of(zoneId)));
    }

    public void queryAndProcessResults(int pollIntervalInSeconds) throws SQLException, PulsarClientException {
        //Let's use Strings in the query since JDBC driver tends to convert timestamps automatically to local jvm time.
        Instant now = Instant.now();
        String nowDateTime = localDatetimeAsString(now, timezone);
        String nowDate = localDateAsString(now, timezone);

        log.info("Querying results from database with timestamp {}", now);
        long queryStartTime = System.currentTimeMillis();

        log.trace("Running query " + queryString);

        try (PreparedStatement statement = dbConnection.prepareStatement(queryString)) {
            statement.setString(1, nowDateTime);
            statement.setString(2, nowDate);
            if (sourceType == CancellationSourceType.FROM_HISTORY) {
                Instant pastNow = now.minusSeconds(pollIntervalInSeconds);
                String historyDateTime = localDatetimeAsString(pastNow, timezone);
                statement.setString(3, nowDateTime);
                statement.setString(4, nowDate);
                statement.setString(5, historyDateTime);
            }

            ResultSet resultSet = statement.executeQuery();
            handler.handleAndSend(resultSet);

            long elapsed = System.currentTimeMillis() - queryStartTime;
            log.info("Messages handled. Total query and processing time: {} ms", elapsed);
        }
        catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

}
