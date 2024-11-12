##In this clean data model:

TRANSPORTATION_STATS table:

Stores the aggregated passenger counts for Luas, Dublin Bus, and cycle counts on a daily basis.
The DATE column serves as the primary key.


WEATHER_STATS table:

Stores the average weather metrics (rainfall, maximum temperature, minimum temperature) for each date.
The DATE column serves as the primary key.


BIKE_AVAILABILITY_STATS table:

Stores the average bike and dock availability for each station on a daily basis.
The combination of DATE and STATION_ID columns serves as the composite primary key.
Includes station details like name and location coordinates.



##Relationships:

The TRANSPORTATION_STATS table has a one-to-one relationship with the WEATHER_STATS table based on the DATE column, indicating that each transportation record is associated with the corresponding weather statistics for that date.
The TRANSPORTATION_STATS table has a one-to-many relationship with the BIKE_AVAILABILITY_STATS table, meaning that each transportation record can include multiple bike availability records for different stations on the same date.

This clean data model provides a simplified and aggregated view of the data, making it easier to perform analysis and derive insights.
