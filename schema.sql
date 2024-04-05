USE cs3380;

-- in reverse order of creation
DROP TABLE IF EXISTS incidents;
DROP TABLE IF EXISTS flown_with;
DROP TABLE IF EXISTS aircraft_types;
DROP TABLE IF EXISTS flights;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS airlines;
DROP TABLE IF EXISTS airports;
DROP TABLE IF EXISTS cities;
DROP TABLE IF EXISTS countries;

CREATE TABLE countries (
    dafifCode CHAR(2) PRIMARY KEY,
    isoCode CHAR(2) NULL,
    fullName VARCHAR(255),
);

CREATE TABLE cities (
    cityID INT IDENTITY(0,1) PRIMARY KEY,
    countryDafifCode CHAR(2) FOREIGN KEY REFERENCES countries(dafifCode),
    cityName VARCHAR(255) NOT NULL,
    utcOffset DECIMAL(4,2),
    timezoneName VARCHAR(255),
    -- city names should hopefully be unique within a country
    -- (they aren't in practice, but duplicates will be few and far between)
    CONSTRAINT AK_CityNameCountry UNIQUE (countryDafifCode, cityName)
);

CREATE TABLE airports (
    iataCode CHAR(3) PRIMARY KEY,
    icaoCode CHAR(4) UNIQUE,
    cityId INT FOREIGN KEY REFERENCES cities(cityID),
    latitude REAL,
    longitude REAL,
    altitude INT, -- in metres
);

CREATE TABLE airlines (
    iataCode CHAR(2) PRIMARY KEY,
    icaoCode CHAR(3) UNIQUE,
    name VARCHAR(255) NOT NULL,
    callsign VARCHAR(255),
    isActive BIT,
    countryDafifCode CHAR(2) FOREIGN KEY REFERENCES countries(dafifCode),
);

CREATE TABLE routes (
    numericID INT PRIMARY KEY IDENTITY(0,1),
    originIataCode CHAR(3) FOREIGN KEY REFERENCES airports(iataCode),
    destinationIataCode CHAR(3) FOREIGN KEY REFERENCES airports(iataCode),
    -- only one route per airport pair (direction sensitive)
    CONSTRAINT AK_OriginDestCodes UNIQUE (originIataCode, destinationIataCode)
);

CREATE TABLE flights (
    flightID INT PRIMARY KEY IDENTITY(0,1),
    routeID INT FOREIGN KEY REFERENCES routes(numericID),
    airlineIataCode CHAR(2) FOREIGN KEY REFERENCES airlines(iataCode), -- operator
    avgDelay INT, -- in minutes
    isCodeshare BIT,
);

CREATE TABLE aircraft_types (
    iataCode CHAR(3) PRIMARY KEY,
    icaoCode CHAR(4) UNIQUE,
    name VARCHAR(255),
);
 
-- linking table between flights and aircraft_types
CREATE TABLE flown_with (
    flightID INT FOREIGN KEY REFERENCES flights(flightID),
    aircraftTypeID CHAR(3) FOREIGN KEY REFERENCES aircraft_types(iataCode),
    CONSTRAINT AK_FlightAircraftType UNIQUE (flightID, aircraftTypeID)
)

CREATE TABLE incidents (
    incidentID INT PRIMARY KEY IDENTITY(0,1),
    category VARCHAR(255),
    description VARCHAR(1023),
    fatalities INT,
    occurredAt DATETIME,
    companyOwnedBy VARCHAR(255),
    aircraftModel VARCHAR(255),
    originIataCode CHAR(3) FOREIGN KEY REFERENCES airports(iataCode),
    destinationIataCode CHAR(3) FOREIGN KEY REFERENCES airports(iataCode)
);

