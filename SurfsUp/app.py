# Import the dependencies
import pandas as pd
from flask import Flask, jsonify
from sqlalchemy import create_engine, func, and_
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

#################################################
# Database Setup
#################################################

# Create the SQLAlchemy engine to connect to the SQLite database
engine = create_engine("sqlite:///./Resources/hawaii.sqlite")

# Reflect the database tables into classes using automap_base
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to the classes named station and measurement
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a session to link Python to the database
session = Session(engine)

#################################################
# Flask Setup
#################################################

# Create the Flask app
app = Flask(__name__)

# Global variable for one_year_ago
one_year_ago = None

# Close the session at the end of the request
@app.teardown_request
def teardown_request(exception=None):
    session.close()

# Precipitation Analysis
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Find the most recent date in the dataset
    most_recent_date = session.query(func.max(Measurement.date)).scalar()

    # Calculate one year ago from the most recent date
    one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)

    # Create a query to collect date and precipitation for the last year of data
    results = session.query(Measurement.date, Measurement.prcp).\
              filter(Measurement.date >= one_year_ago.strftime('%Y-%m-%d')).all()

    # Save the query results to a Pandas DataFrame
    precipitation_df = pd.DataFrame(results, columns=['date', 'precipitation'])

    # Sort the DataFrame by date
    precipitation_df = precipitation_df.sort_values(by='date')

    # Use Pandas to print summary statistics
    summary_stats = precipitation_df.describe()

    return jsonify({
        'precipitation_data': precipitation_df.to_dict(orient='records'),
        'summary_statistics': summary_stats.to_dict()
    })

# Station Analysis
@app.route("/api/v1.0/stations")
def stations():
    global one_year_ago 
    
    # Check if one_year_ago is None, calculate it if needed
    if one_year_ago is None:
        most_recent_date = session.query(func.max(Measurement.date)).scalar()
        one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)
    
    # Design a query to find the number of stations in the dataset
    station_count = session.query(func.count(Station.station)).scalar()

    # Design a query to list the stations and observation counts in descending order
    station_observation_counts = session.query(Measurement.station, func.count(Measurement.station)).\
                                group_by(Measurement.station).\
                                order_by(func.count(Measurement.station).desc()).all()

    # Find the most active station
    most_active_station = station_observation_counts[0][0]

    # Design a query to find min, max, and average temperatures for the most active station
    most_active_station_stats = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
                                filter(Measurement.station == most_active_station).all()

    # Design a query to get the previous 12 months of TOBS data for the most active station
    most_active_station_tobs = session.query(Measurement.date, Measurement.tobs).\
                               filter(Measurement.station == most_active_station).\
                               filter(Measurement.date >= one_year_ago.strftime('%Y-%m-%d')).all()

    # Save the TOBS query results to a Pandas DataFrame
    tobs_df = pd.DataFrame(most_active_station_tobs, columns=['date', 'tobs'])

    return jsonify({
        'station_count': station_count,
        'station_observation_counts': dict(station_observation_counts),
        'most_active_station_stats': {
            'min_temp': most_active_station_stats[0][0],
            'max_temp': most_active_station_stats[0][1],
            'avg_temp': most_active_station_stats[0][2]
        },
        'most_active_station_tobs': tobs_df.to_dict(orient='records')
    })

# Temperature Observations
@app.route("/api/v1.0/tobs")
def tobs():
    global one_year_ago

    # Check if one_year_ago is None, calculate it if needed
    if one_year_ago is None:
        most_recent_date = session.query(func.max(Measurement.date)).scalar()
        one_year_ago = pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)

    # Design a query to get the previous 12 months of TOBS data
    tobs_results = session.query(Measurement.date, Measurement.tobs).\
                   filter(Measurement.date >= one_year_ago.strftime('%Y-%m-%d')).all()

    # Save the TOBS query results to a Pandas DataFrame
    tobs_df = pd.DataFrame(tobs_results, columns=['date', 'tobs'])

    return jsonify({
        'temperature_observations': tobs_df.to_dict(orient='records')
    })


# Landing Page with Available Routes
@app.route("/")
def home():
    return (
        f"Welcome to the Climate App API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;"
    )

# API Dynamic Routes
@app.route("/api/v1.0/<start>")
def start_date_stats(start):
    # Design a query to get min, avg, and max temperatures from the start date to the end of the dataset
    start_date_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                         filter(Measurement.date >= start).all()

    return jsonify({
        'start_date': start,
        'temperature_stats': {
            'min_temp': start_date_results[0][0],
            'avg_temp': start_date_results[0][1],
            'max_temp': start_date_results[0][2]
        }
    })

@app.route("/api/v1.0/<start>/<end>")
def date_range_stats(start, end):
    # Create query to get average, minimum, and maximum temperatures for the given date range
    date_range_results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                         filter(and_(Measurement.date >= start, Measurement.date <= end)).all()

    return jsonify({
        'start_date': start,
        'end_date': end,
        'temperature_stats': {
            'min_temp': date_range_results[0][0],
            'avg_temp': date_range_results[0][1],
            'max_temp': date_range_results[0][2]
        }
    })

if __name__ == "__main__":
    app.run(debug=True)
