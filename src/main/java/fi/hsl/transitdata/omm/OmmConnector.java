package fi.hsl.transitdata.omm;

import fi.hsl.common.files.FileUtils;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static fi.hsl.transitdata.omm.CancellationSourceType.FROM_NOW;
import static fi.hsl.transitdata.omm.CancellationSourceType.FROM_PAST;

public class OmmConnector {

    private static final Logger log = LoggerFactory.getLogger(OmmConnector.class);

    private final Connection dbConnection;
    private final OmmCancellationHandler handler;
    private final String queryString;
    private final CancellationSourceType sourceType;
    private final String timezone;

    private OmmConnector(PulsarApplicationContext context, Connection connection, CancellationSourceType type, boolean useTestOmmQueries) {
        handler = new OmmCancellationHandler(context);
        dbConnection = connection;
        queryString = createQuery(type, useTestOmmQueries);
        sourceType = type;
        timezone = context.getConfig().getString("omm.timezone");
        log.info("Using timezone " + timezone);
    }

    public static OmmConnector newInstance(PulsarApplicationContext context, String jdbcConnectionString,
                                           CancellationSourceType sourceType, boolean useTestOmmQueries) throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcConnectionString);
        return new OmmConnector(context, connection, sourceType, useTestOmmQueries);
    }

    private String createQuery(CancellationSourceType sourceType, boolean useTestOmmQueries) {
        var resourcePath = queryResourcePath(sourceType, useTestOmmQueries);
        var stream = getClass().getResourceAsStream(resourcePath);
        try {
            return FileUtils.readFileFromStreamOrThrow(stream);
        } catch (Exception e) {
            log.error("Error in reading sql from file:", e);
            return null;
        }
    }

    private static String queryResourcePath(CancellationSourceType sourceType, boolean useTestOmmQueries) {
        if (sourceType == FROM_PAST) {
            return useTestOmmQueries
                    ? "/cancellations_past_current_future_test.sql"
                    : "/cancellations_past_current_future.sql";
        } else if (sourceType == FROM_NOW) {
            return useTestOmmQueries
                    ? "/cancellations_current_future_test.sql"
                    : "/cancellations_current_future.sql";
        } else {
            throw new IllegalArgumentException("sourceType is required");
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
            if (sourceType == FROM_PAST) {
                Instant pastNow = now.minusSeconds(pollIntervalInSeconds);
                String pastDateTime = localDatetimeAsString(pastNow, timezone);
                statement.setString(3, nowDateTime);
                statement.setString(4, nowDate);
                statement.setString(5, pastDateTime);
            }

            ResultSet resultSet = statement.executeQuery();
            handler.handleAndSend(resultSet);

            long elapsed = System.currentTimeMillis() - queryStartTime;
            if (elapsed > 4000) {
                log.warn("Slow querying & handling of cancellations. Total query and processing time was: {} ms",
                        elapsed);
            }
        } catch (Exception e) {
            log.error("Error while  querying and processing messages", e);
            throw e;
        }
    }

}
