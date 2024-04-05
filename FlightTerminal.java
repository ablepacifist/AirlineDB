import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.Scanner;

public class FlightTerminal {
	static Connection connection;

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

		runConsole(new MyDatabase(connection));
		System.out.println("Exiting... ");
	}

	public static void runConsole(MyDatabase db) {

		Scanner console = new Scanner(System.in);
		System.out.print("Welcome! Type h for help. ");
		System.out.print("db > ");
		String line = console.nextLine();
		String[] parts;
		String arg = "";

		while (line != null && !line.equals("q")) {
			parts = line.split("\\s+");
			if (line.indexOf(" ") > 0)
				arg = line.substring(line.indexOf(" ")).trim();

			if (parts[0].equals("h"))
				printHelp();
			else if (parts[0].equals("at")) {
				if (parts.length >= 2)
					db.airportTraffic(arg);
				else
					System.out.println("Require an argument for this command");
			}

			else if (parts[0].equals("ta")) {
				try {
					if (parts.length >= 2)
						db.airportPlanes(arg);
					else
						System.out.println("Require an argument for this command");
				} catch (Exception e) {
					System.out.println("id must be an integer"); // change this
				}
			}

			else if (parts[0].equals("apl")) {
				try {
					if (parts.length >= 2)
						db.airportPopularity(arg);
					else
						System.out.println("Require an argument for this command");
				} catch (Exception e) {
					System.out.println("id must be an integer"); // change this
				}
			}

			else if (parts[0].equals("f")) {
				try {
					if (parts.length >= 3)
						db.flights(parts[1], parts[2]);
					else
						System.out.println("Requires 2 arguments for this command");
				} catch (Exception e) {
					System.out.println("id must be an integer"); // change this
				}
			}

			else if (parts[0].equals("a")) {
				try {
					if (parts.length >= 2)
						db.airports(parts[1].toUpperCase());
					else
						System.out.println("Require an argument for this command");
				} catch (Exception e) {
					System.out.println("dafif must be a 2 char String");
				}

			} else if (parts[0].equals("c")) {
				db.countries();
			}

			else if (parts[0].equals("ri")) {
				db.routeIncidents();
			}

			else if (parts[0].equals("ai")) {
				db.airlineIncidents();
			}

			else if (parts[0].equals("lc")) {
				db.countryArrivals();
			}

			else
				System.out.println("Read the help with h, or find help somewhere else.");

			System.out.print("db > ");
			line = console.nextLine();
		}

		console.close();
	}

	private static void printHelp() {
		System.out.println("Library database");
		System.out.println("Commands:");
		System.out.println("h - Get help");
		System.out.println("a <country dafif code> - prints out list of airports and their ICAO code in a country");
		System.out.println("c - prints out list of countries and their DAFIF code");
		System.out.println("ri - gives top 10 routes with most incidents");
		System.out.println("ai - gives airlines with least incidents");
		System.out.println("lc - list top 5 countrys with most arrivals");
		System.out.println("at <airport> - gives a list of flights leaving a airport. Search using ICAO code");
		System.out.println("ta <airport> - shows top five aircraft leaving an airport. Search using ICAO code");
		System.out.println(
				"apl <country> - lists airports by popularity in a country. Search using ICAO code. Search using DAFIF code");
		System.out.println("f <to> <from> - Search for flights going from airport to airport. Search using ICAO code");
		System.out.println("");

		System.out.println("q - Exit the program");

		System.out.println("---- end help ----- ");
	}

}

class MyDatabase {
	private Connection connection;
	private PreparedStatement statement;

	public MyDatabase(Connection connect) {
		// db name here
		// create a connection to the database
		connection = connect;

	}

	public void airports(String dafif) {
		try {
			statement = connection.prepareStatement(
					"SELECT cityname,iataCode,countries.fullName FROM airports JOIN cities ON airports.cityId = cities.cityID JOIN countries ON cities.countryDafifCode = countries.dafifCode WHERE countries.dafifCode = ? ORDER BY airports.iataCode");
			statement.setString(1, dafif);
			ResultSet RS = statement.executeQuery();
			RS.next();
			String countries = RS.getString("fullName");
			System.out.println("Airports in " + countries + "");

			String city = RS.getString("cityname");
			String iataCode = RS.getString("iataCode");
			System.out.println(iataCode + " airport in " + city + "");
			while (RS.next()) {
				city = RS.getString("cityname");
				iataCode = RS.getString("iataCode");
				System.out.println(iataCode + " airport in " + city + "");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void countries() {
		try {
			statement = connection.prepareStatement("SELECT fullName, dafifCode FROM countries order by dafifCode ");
			ResultSet RS = statement.executeQuery();
			while ( RS.next() ) {
                String country = RS.getString("fullName");
				String dafifCode = RS.getString("dafifCode");
				System.out.println(country +", dafifCode:" +dafifCode+"");
            }
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void routeIncidents() {
		try {
			statement = connection.prepareStatement(
					"SELECT TOP 10 numericID, count(incidentID) as numIn FROM routes JOIN incidents ON routes.originIataCode = incidents.originIataCode WHERE routes.destinationIataCode = incidents.destinationIataCode GROUP BY routes.numericID ORDER BY numIn DESC");
			ResultSet RS = statement.executeQuery();
			while (RS.next()) {
				String route = RS.getString("numericID");
				String numIncidents = RS.getString("numIn");
				System.out.println("routes#:"+route + " has " + numIncidents + " incidents");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void airlineIncidents() {
		try {
			statement = connection.prepareStatement(
					"SELECT TOP 10 name, count(incidentID) as numIn FROM incidents JOIN airlines ON incidents.companyOwnedBy = airlines.name GROUP BY airlines.name ORDER BY numIn DESC ");
			ResultSet RS = statement.executeQuery();
			while (RS.next()) {
				String airline = RS.getString("name");
				String numIncidents = RS.getString("numIn");
				System.out.println(airline + " has " + numIncidents + " incidents");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void countryArrivals() {
		System.out.println("in countryArrivals");
	}

	public void airportTraffic(String IACO) {
		System.out.println("in airportTraffic");
	}

	public void airportPlanes(String IACO) {
		System.out.println("in airportPlanes");
	}

	public void airportPopularity(String DAFIF) {
		System.out.println("in airportPopularity");
	}

	public void flights(String to, String from) {
		System.out.println("in flights");
	}
}
