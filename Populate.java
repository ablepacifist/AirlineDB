import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import java.util.Scanner;
public class Populate {
    // Connect to your database.
    // Replace server name, username, and password with your credentials
    public static void main(String[] args) {
        Properties prop = new Properties();
        String fileName = "auth.cfg";
        try {
            FileInputStream configFile = new FileInputStream(fileName);
            prop.load(configFile);
            configFile.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Could not find config file.");
            System.exit(1);
        } catch (IOException ex) {
            System.out.println("Error reading config file.");
            System.exit(1);
        }
        String username = (prop.getProperty("username"));
        String password = (prop.getProperty("password"));

        if (username == null || password == null) {
            System.out.println("Username or password not provided.");
            System.exit(1);
        }

        String connectionUrl = "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                + "database=cs3380;"
                + "user=" + username + ";"
                + "password=" + password + ";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        Connection connection;
        try {
            connection = DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            System.err.println("Error: Could not connect to database.");
            System.exit(1);
            return;
        }

        PreparedStatement statement;
        Scanner fileScanner;
        String fullLine;
        String[] tokens;

        /*{
            ////////// COUNTRIES //////////
            {
                System.out.println("\nPopulating countries...");
                int inserted = 0;

                try {
                    fileScanner = new Scanner(new File("countries.dat"), "UTF-8");
                } catch (FileNotFoundException e) {
                    System.err.println("Error: Data file countries.dat is not present.");
                    System.exit(1);
                    return;
                }

                while (fileScanner.hasNextLine()) {

                    fullLine = fileScanner.nextLine();
                    tokens = fullLine.split(",");

                    if (tokens.length != 3)
                        continue; // skip row

                    String countryName = tokens[0].replaceAll("\"", "");
                    String isoCode = tokens[1].replaceAll("\"", "");
                    String dafifCode = tokens[2].replaceAll("\"", "");

                    try {
                        statement = connection.prepareStatement("INSERT INTO countries VALUES( ?, ?, ? )");
                        statement.setString(1, dafifCode);
                        setNullableString(statement, 2, isoCode, Types.CHAR);
                        statement.setString(3, countryName);

                        statement.executeUpdate();
                    } catch (SQLException e) {
                        continue; // skip row
                    }

                    inserted++; // if successful
                }

                fileScanner.close();
                System.out.printf("Inserted %,d countries, of 260 expected.\n", inserted);
            }

            ////////// AIRLINES //////////
            {
                System.out.println("\nPopulating airlines...");
                int inserted = 0;

                try {
                    fileScanner = new Scanner(new File("airlines.dat"), "UTF-8");
                } catch (FileNotFoundException e) {
                    System.err.println("Error: Data file airlines.dat is not present.");
                    System.exit(1);
                    return;
                }

                // Scanner next line
                while (fileScanner.hasNextLine()) {

                    fullLine = fileScanner.nextLine();
                    tokens = fullLine.split(",");

                    if (tokens.length != 6)
                        continue; // skip row

                    String airlineName = tokens[0].replaceAll("\"", "");
                    String iataCode = tokens[1].replaceAll("\"", "");
                    String icaoCode = tokens[2].replaceAll("\"", "");
                    String callsign = tokens[3].replaceAll("\"", "");
                    String countryDafifCode = tokens[4].replaceAll("\"", "");
                    String isActive = tokens[5].replaceAll("\"", "");

                    // Skip line if IATA code (primary key), ICAO code (alternate key), or name
                    // aren't present
                    if (isNullString(iataCode) || isNullString(icaoCode) || isNullString(airlineName))
                        continue;

                    try {
                        statement = connection.prepareStatement("INSERT INTO airlines VALUES( ?, ?, ?, ?, ?, ? );");
                        statement.setString(1, iataCode); // primary key
                        statement.setString(2, icaoCode); // unique
                        statement.setString(3, airlineName);
                        setNullableString(statement, 4, callsign, Types.VARCHAR);
                        statement.setBoolean(5, isActive.equals("Y"));
                        statement.setString(6, countryDafifCode);

                        statement.executeUpdate();
                    } catch (SQLException e) {
                        // if insertion fails (most often due to existing primary key), skip row
                        continue;
                    }

                    inserted++;
                }

                fileScanner.close();
                System.out.printf("Inserted %,d airlines, of 964 expected.\n", inserted);
            }

            ////////// AIRPORTS AND CITIES //////////
            {
                System.out.println("\nPopulating airports and cities...");
                int airportsInserted = 0, citiesInserted = 0;

                try {
                    fileScanner = new Scanner(new File("airports.dat"), "UTF-8");
                } catch (FileNotFoundException e) {
                    System.err.println("Error: Data file airports.dat is not present.");
                    System.exit(1);
                    return;
                }

                ResultSet results;
                int cityId;

                while (fileScanner.hasNextLine()) {

                    fullLine = fileScanner.nextLine();
                    tokens = fullLine.split(",");

                    if (tokens.length != 14)
                        continue; // skip row

                    String cityName = tokens[2].replaceAll("\"", "");
                    String countryCode = tokens[3].replaceAll("\"", "");
                    String iataCode = tokens[4].replaceAll("\"", "");
                    String icaoCode = tokens[5].replaceAll("\"", "");
                    String latitude = tokens[6].replaceAll("\"", "");
                    String longitude = tokens[7].replaceAll("\"", "");
                    String altitude = tokens[8].replaceAll("\"", "");
                    String utcOffset = tokens[9].replaceAll("\"", "");
                    String timezoneName = tokens[11].replaceAll("\"", "");

                    // Skip line if IATA code (primary key), ICAO code (alternate key), city name,
                    // or country code aren't present
                    if (isNullString(iataCode) || isNullString(icaoCode) || isNullString(cityName)
                            || isNullString(countryCode))
                        continue;

                    try {
                        // city
                        statement = connection.prepareStatement(
                                "INSERT INTO cities(countryDafifCode, cityName, utcOffset, timezoneName) OUTPUT INSERTED.cityID VALUES(?,?,?,?)");
                        statement.setString(1, countryCode);
                        statement.setString(2, cityName);
                        statement.setString(3, utcOffset);
                        statement.setString(4, timezoneName);

                        // get autoincremented ID of city just inserted
                        results = statement.executeQuery();
                        results.next();
                        cityId = results.getInt(1);

                        citiesInserted++;

                    } catch (SQLException e) {
                        // will happen whenever we reach the second airport in any given city
                        // (duplicate unique constraint on cityName)

                        try {
                            // no new route was inserted, need to get existing ID with SELECT query
                            statement = connection.prepareStatement(
                                    "SELECT cityID FROM cities WHERE countryDafifCode=? AND cityName=?");
                            statement.setString(1, countryCode);
                            statement.setString(2, cityName);

                            results = statement.executeQuery();
                            results.next();
                            cityId = results.getInt(1);

                        } catch (SQLException er) {
                            // failed to insert new city AND retrieve existing,
                            // so we can't insert a new airport (depends on a valid city ID)
                            continue;
                        }
                    }

                    try {
                        statement = connection.prepareStatement(
                                "INSERT INTO airports VALUES( ?, ?, ?, ?, ?, ? );");
                        statement.setString(1, iataCode);
                        statement.setString(2, icaoCode);
                        statement.setInt(3, cityId);
                        statement.setString(4, latitude);
                        statement.setString(5, longitude);
                        statement.setString(6, altitude);
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        continue; // skip row
                    }

                    airportsInserted++;
                }

                fileScanner.close();
                System.out.printf("Inserted %,d cities, of 5,555 expected.\n", citiesInserted);
                System.out.printf("Inserted %,d airports, of 5,878 expected.\n", airportsInserted);
            }

            ////////// AIRCRAFT_TYPES //////////
            {
                System.out.println("\nPopulating aircraft types...");
                int inserted = 0;

                try {
                    fileScanner = new Scanner(new File("planes.dat"), "UTF-8");
                } catch (FileNotFoundException e) {
                    System.err.println("Error: Data file planes.dat is not present.");
                    System.exit(1);
                    return;
                }

                while (fileScanner.hasNextLine()) {

                    fullLine = fileScanner.nextLine();
                    tokens = fullLine.split(",");

                    if (tokens.length != 3)
                        continue;

                    String name = tokens[0].replaceAll("\"", "");
                    String iataCode = tokens[1].replaceAll("\"", "");
                    String icaoCode = tokens[2].replaceAll("\"", "");

                    if (isNullString(name) || isNullString(iataCode) || isNullString(icaoCode))
                        continue;

                    try {
                        statement = connection.prepareStatement("INSERT INTO aircraft_types VALUES( ?, ?, ? )");
                        statement.setString(1, iataCode);
                        statement.setString(2, icaoCode);
                        statement.setString(3, name);
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        continue; // skip row
                    }

                    inserted++;
                }

                fileScanner.close();
                System.out.printf("Inserted %,d aircraft types, of 206 expected.\n", inserted);
            }

            ////////// FLIGHTS AND ROUTES //////////
            {
                System.out.println("\nPopulating flights and routes...");
                System.out.println("(This might take a minute.)");
                int flightsInserted = 0, routesInserted = 0, flownWithInserted = 0;

                try {
                    fileScanner = new Scanner(new File("routes.dat"), "UTF-8");
                } catch (FileNotFoundException e) {
                    System.err.println("Error: Data file routes.dat is not present.");
                    System.exit(1);
                    return;
                }

                while (fileScanner.hasNextLine()) {
                    fullLine = fileScanner.nextLine();
                    tokens = fullLine.split(",");

                    if (tokens.length != 9)
                        continue; // skip row

                    String airlineIata = tokens[0];
                    String originIata = tokens[2];
                    String destIata = tokens[4];
                    String codeshare = tokens[6];
                    String[] aircraftTypes = tokens[8].split(" "); // space-separated list

                    ResultSet results;
                    int routeId, flightId; // stores route and flight IDs after insertion

                    ////////// ROUTES //////////
                    try {
                        statement = connection.prepareStatement(
                                "INSERT INTO routes( originIataCode, destinationIataCode ) OUTPUT INSERTED.numericID VALUES( ?, ? )");
                        statement.setString(1, originIata);
                        statement.setString(2, destIata);

                        results = statement.executeQuery();
                        results.next();
                        routeId = results.getInt(1);

                        routesInserted++;

                    } catch (SQLException e) {
                        // will happen often, since file contains multiple flights on the same route
                        // (we have a unique constraint on the origin/destination pairs)

                        try {
                            // no new route was inserted, need to get existing ID with SELECT query
                            statement = connection.prepareStatement(
                                    "SELECT numericID FROM routes WHERE originIataCode=? AND destinationIataCode=?");
                            statement.setString(1, originIata);
                            statement.setString(2, destIata);

                            results = statement.executeQuery();
                            results.next();
                            routeId = results.getInt(1);

                        } catch (SQLException er) {
                            // failed to insert new route AND retrieve existing,
                            // so we can't insert a new flight (depends on a valid route ID)
                            continue;
                        }
                    }

                    ////////// FLIGHTS //////////
                    try {
                        // insert flight, using route ID from above
                        statement = connection.prepareStatement(
                                "INSERT INTO flights( routeID, airlineIataCode, isCodeshare )"
                                        + "OUTPUT INSERTED.flightID VALUES( ?, ?, ? )");
                        statement.setInt(1, routeId);
                        statement.setString(2, airlineIata);
                        statement.setBoolean(3, codeshare.equalsIgnoreCase("y"));

                        results = statement.executeQuery();
                        results.next();
                        flightId = results.getInt(1);

                        flightsInserted++;

                    } catch (SQLException e) {
                        continue; // skip row
                    }

                    ////////// FLOWN_WITH //////////
                    for (String aircraftType : aircraftTypes) {
                        try {
                            // get ID of flight just inserted (autoincremented ID)
                            statement = connection.prepareStatement(
                                    "INSERT INTO flown_with( flightID, aircraftTypeID ) VALUES( ?, ? )");
                            statement.setInt(1, flightId);
                            statement.setString(2, aircraftType);

                            statement.executeUpdate();

                            flownWithInserted++;

                        } catch (SQLException e) {
                            continue;
                        }
                    }
                }

                fileScanner.close();
                System.out.printf("Inserted %,d routes, of 36,966 expected.\n", routesInserted);
                System.out.printf("Inserted %,d flights, of 63,437 expected.\n", flightsInserted);
                System.out.printf("Inserted %,d flight—aircraft type relationships, of 67,679 expected.\n",
                        flownWithInserted);
            }

            ////////// INCIDENTS //////////
            {
                System.out.println("\nPopulating incidents...");

                try {
                    fileScanner = new Scanner(new File("Aircraft_Incident_Dataset.csv"), "UTF-8");
                } catch (FileNotFoundException e) {
                    System.err.println("Error: Data file Aircraft_Incident_Dataset.csv is not present.");
                    System.exit(1);
                    return;
                }

                fileScanner.nextLine();

                while (fileScanner.hasNextLine()) {
                    fullLine = fileScanner.nextLine(); // Getting rid of catagories
                    tokens = fullLine.split(",");

                    if (tokens.length != 21)
                        continue;

                    String aircraftModel = tokens[1]; // Aircaft_Model
                    String companyOwnedBy = tokens[3]; // Aircaft_Operator
                    String category = tokens[5]; // Incident_Category
                    String description = tokens[6]; // Incident_Cause(es) (pblm list)
                    String date = tokens[0];
                    String fatalities = tokens[16]; // Fatalities
                    String departure = tokens[19]; // Departure_Airport
                    String destination = tokens[20]; // Destination_Airport

                    tokens = departure.split("\\("); // pulling out IATA and IACO codes
                    departure = tokens[tokens.length - 1];
                    tokens = departure.split("\\)");
                    departure = tokens[0];

                    String departIATA = "";

                    if (departure.length() == 3) {
                        departIATA = departure;
                    } else if (departure.length() == 8) {
                        tokens = departure.split("/");
                        departIATA = tokens[0];
                    } else
                        continue; // cannot determine departure airport

                    tokens = destination.split("\\(");
                    destination = tokens[tokens.length - 1];
                    tokens = destination.split("\\)");
                    destination = tokens[0];

                    String destinIATA = "";

                    if (destination.length() == 3) {
                        destinIATA = destination;
                    } else if (destination.length() == 8) {
                        tokens = destination.split("/");
                        destinIATA = tokens[0];
                    } else
                        continue; // cannot determine destination airport

                    try {
                        statement = connection.prepareStatement(
                                "INSERT INTO incidents( category, description, fatalities, occurredAt, companyOwnedBy,"
                                        + "aircraftModel, originIataCode, destinationIataCode )"
                                        + "VALUES( ?, ?, ?, CONVERT ( DATETIME, ? ), ?, ?, ?, ? )");

                        statement.setString(1, category);
                        statement.setString(2, description);
                        statement.setString(3, fatalities);
                        statement.setString(4, date);
                        statement.setString(5, companyOwnedBy);
                        statement.setString(6, aircraftModel);
                        statement.setString(7, departIATA);
                        statement.setString(8, destinIATA);

                        statement.executeUpdate();
                    } catch (SQLException e) {
                        continue; // skip row
                    }

                }

                fileScanner.close();
            }
        }*/

        FlightTerminal.runConsole(new MyDatabase(connection));
        System.out.println("Exiting... ");

    }

    private static boolean isNullString(String param) {
        // \N is the OpenFlights code for null
        return param.length() == 0 || param.equals("\\N");
    }

    private static void setNullableString(PreparedStatement statement, int index,
            String param, int type) throws SQLException {
        if (isNullString(param))
            statement.setNull(index, type);
        else
            statement.setString(index, param);
    }
}
