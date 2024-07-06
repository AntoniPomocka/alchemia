import csv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, inspect

engine = create_engine('sqlite:///example.db')
metadata = MetaData()

stations = Table(
    'stations', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('station', String, nullable=False),
    Column('latitude', Float, nullable=False),
    Column('longitude', Float, nullable=False),
    Column('elevation', Float, nullable=False),
    Column('name', String, nullable=False),
    Column('country', String, nullable=False),
    Column('state', String, nullable=False),
)

measurements = Table(
    'measurements', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('station', String, nullable=False),
    Column('date', String, nullable=False),
    Column('precip', Float, nullable=True),
    Column('tobs', Float, nullable=True),
)


inspector = inspect(engine)

with engine.connect() as conn:
    if 'stations' in inspector.get_table_names():
        conn.execute("DROP TABLE stations")
    if 'measurements' in inspector.get_table_names():
        conn.execute("DROP TABLE measurements")


metadata.create_all(engine)

def get_csv_headers(file_name):
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        return reader.fieldnames

stations_headers = get_csv_headers('clean_stations.csv')
measurements_headers = get_csv_headers('clean_measure.csv')

with open('clean_stations.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    with engine.begin() as conn:
        for row in reader:
            conn.execute(stations.insert().values(
                station=row['station'],
                latitude=float(row['latitude']),
                longitude=float(row['longitude']),
                elevation=float(row['elevation']),
                name=row['name'],
                country=row['country'],
                state=row['state']
            ))

with open('clean_measure.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    with engine.begin() as conn:
        for row in reader:
            conn.execute(measurements.insert().values(
                station=row['station'],
                date=row['date'],
                precip=float(row['precip']) if row['precip'] else None,
                tobs=float(row['tobs']) if row['tobs'] else None
            ))

with engine.connect() as conn:
    result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    for row in result:
        print(row)