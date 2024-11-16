import streamlit as st
import urllib.request
import json
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson 
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide")

st.title("DC Metrorail Real-time Map")
    
class Line:
    def __init__(self, name, code, color, trains, stations, trips):
        self.name = name
        self.code = code
        self.color = color
        self.trains = trains
        self.stations = stations  # A list of Station objects
        self.trips = trips
        #self.predictions = predictions

    def __str__(self):
        return f"Line: {self.name}, Color: {self.color}, Code:{self.code} Trains: {self.trains},  Stations: {len(self.stations)}, Trips: {self.trips}"

hdr ={
# Request headers
'Cache-Control': 'no-cache',
'api_key': st.secrets['api_key'],
}

px.set_mapbox_access_token(st.secrets['mapbox_token'])

def get():
    def get_realtime_update():
        url = "https://api.wmata.com/gtfs/rail-gtfsrt-vehiclepositions.pb"

        req = urllib.request.Request(url, headers=hdr)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)

        # Parse the protobuf response
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.read())

        # Convert the FeedMessage protobuf object to JSON format
        feed_json = MessageToJson(feed)
        return feed_json

    def get_predictions():
        url = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
        req = urllib.request.Request(url, headers=hdr)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
        return response.read()
    
    return json.loads(get_realtime_update()), json.loads(get_predictions())

feed_rt, predictions = get()

trains = feed_rt["entity"]
trips = pd.read_csv('trips.txt')
stops = pd.read_csv('stops.txt')
stop_times = pd.read_csv('stop_times.txt')

#Initialise line values
def lines(name, code):
    color = name.upper()
    linetrains = []
    for train in trains:
        if train["vehicle"]["trip"]["routeId"] == color:
            linetrains.append(train)
    linetrains = pd.json_normalize(linetrains)
    linetrains["vehicle.stopId"] = linetrains["vehicle.stopId"].str[:-2]

    trip = trips[trips["route_id"]==color]
    times = stop_times[stop_times["trip_id"].isin(trip["trip_id"])]
    stations = stops[stops["stop_id"].isin(times["stop_id"])]
    stations = stations.drop_duplicates(subset="parent_station", keep="first")
    stations["stop_name"] = stations["stop_name"].str.split(", ").str[0]
    stations["stop_id"] = stations["stop_id"].str[:-2]
    

    line = Line(name, code, color, linetrains, stations, trip)

    return line

orangeline = lines("Orange", "OR")
silverline = lines("Silver", "SV")
blueline = lines("Blue", "BL")


prediction = predictions["Trains"]
prediction = pd.json_normalize(prediction)
prediction = prediction[prediction["Line"].isin(["OR", "SV", "BL"])].drop_duplicates()

#Order station list
def order_stations(line, filename):
    df1 = line.stations
    df2 = pd.read_csv(filename)
    df2["order"] = df2.index
    df2 = df2.rename(columns={"Stations":"stop_name"})
    df2["stop_name"] = df2["stop_name"].str.upper()
    stations = pd.merge(df1, df2, on="stop_name").sort_values('order').reset_index().drop(columns=["index", "order"])

    return stations

silverline.stations = order_stations(silverline, 'ordered_silver_list.txt')
orangeline.stations = order_stations(orangeline, 'ordered_orange_list.txt')
blueline.stations = order_stations(blueline, 'ordered_blue_list.txt')

#Map station values to the next stop ID in trains
def next_stop(line):
    stations = line.stations

    # Create next stop columns using a single shift operation for both IDs and names
    stations[['next_stop_id_0', 'next_stop_id_1']] = stations[['stop_id', 'stop_name']].shift(-1).combine_first(stations[['stop_id', 'stop_name']].shift(1))

    # Rename columns after shifting to make it clear
    stations["next_stop_name_0"] = stations["stop_name"].shift(-1)
    stations["next_stop_name_1"] = stations["stop_name"].shift(1)

    stations.rename(columns={
            'stop_id': 'station_id',
            'stop_name': 'station_name',
            'next_stop_name_0': 'next_station_name_0',
            'next_stop_name_1': 'next_station_name_1',
        }, inplace=True)

    # Merge trains with stations, focusing on relevant columns to minimize memory usage
    trains = pd.merge(
        line.trains,
        stations[['station_id', 'station_name', 'next_station_name_0', 'next_station_name_1']],
        how='left',
        left_on='vehicle.stopId',
        right_on='station_id',
        suffixes=('', '')
    )
    
    return trains

orangeline.trains=next_stop(orangeline)
blueline.trains=next_stop(blueline)
silverline.trains=next_stop(silverline)

def minutes(line, prediction):
    stations = line.stations.copy()  # Avoid modifying the original DataFrame
    
    # Filter and process prediction data
    pred = (prediction[prediction["Line"] == line.code]
            .groupby(['Group', 'LocationCode', 'LocationName'])['Min']
            .apply(list)
            .reset_index())
    
    # Adjust Group values and rename columns for merging
    pred['Group'] = pred['Group'].astype(int) - 1
    pred = pred.pivot(index='LocationCode', columns='Group', values='Min').reset_index()
    pred.columns = ['LocationCode'] + [f'minute{group}' for group in pred.columns[1:]]

    # Extract LocationCode from stop_id
    stations['LocationCode'] = stations['station_id'].str[3:]

    # Merge prediction results with stations
    stations = stations.merge(pred, on='LocationCode', how='left', suffixes=('', ''))

    return stations

orangeline.stations = minutes(orangeline, prediction)
silverline.stations = minutes(silverline, prediction)
blueline.stations = minutes(blueline, prediction)

def initialize_map(line):
    stations = line.stations
    fig = px.scatter_mapbox(stations, lat = stations.stop_lat, lon=stations.stop_lon)

    # Set layout for Mapbox
    fig.update_layout(
        mapbox_style='carto-positron',  
        mapbox_center={"lat": 38.92, "lon": -77.07},
        mapbox_zoom=10, 
        mapbox_bearing=0,
        mapbox_pitch=0,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        showlegend=False  # Show legend if needed
    )


    return fig

def plot(line, direction):
    fig = initialize_map(line)

    def plot_stations(line, direction):
        fig.add_trace(go.Scattermapbox(
            lat=line.stations.stop_lat,
            lon=line.stations.stop_lon,
            mode='markers+text+lines',
            marker=dict(size=9, color=line.name.lower()),  # Specify a color explicitly
            text=line.stations.station_name,
            textfont=dict(size=8, color="black", family="Open Sans Bold")
        ))

        if direction:
            cur = line.stations.iloc[0]
            fig.add_trace(go.Scattermapbox(
                lat=[cur.stop_lat],
                lon=[cur.stop_lon],
                mode='markers+text',
                text="Destination: ",
                textfont=dict(size=11, color="black", family="Open Sans Bold"),
                marker=dict(size=25,color="red")
            ))
        else:
            cur = line.stations.iloc[-1]
            fig.add_trace(go.Scattermapbox(
                lat=[cur.stop_lat],
                lon=[cur.stop_lon],
                mode='markers+text',
                text="Destination",
                textfont=dict(size=11, color="black", family="Open Sans Bold"),
                marker=dict(size=25, color="red")
            ))


    def plot_trains(line, direction):
        trains = line.trains
        trains = trains[trains["vehicle.trip.directionId"]==direction]
        stopped_trains = trains[trains["vehicle.currentStatus"]=="STOPPED_AT"]
        trains = trains[trains["vehicle.currentStatus"]!="STOPPED_AT"]

        #For stopped trains
        fig.add_trace(go.Scattermapbox(
                lat=stopped_trains["vehicle.position.latitude"],
                lon=stopped_trains["vehicle.position.longitude"],
                mode='markers',
                text=stopped_trains["vehicle.currentStatus"]+" "+stopped_trains["station_name"],
                marker=dict(size= 16, symbol= "rail-metro")
            ))
        
        #For trains in transit
        lat = trains["vehicle.position.latitude"]
        lon = trains["vehicle.position.longitude"]
        if direction:
            fig.add_trace(go.Scattermapbox(
                lat=lat,
                lon=lon,
                mode='markers',
                text=trains["vehicle.currentStatus"]+" "+trains["next_station_name_1"],
                marker=dict(size= 16, color="black")
            ))
        else:
            fig.add_trace(go.Scattermapbox(
                lat=lat,
                lon=lon,
                mode='markers',
                text=trains["vehicle.currentStatus"]+" "+trains["next_station_name_0"],
                hoverinfo=['skip'],
                marker={"size": 16, "color": "black"}
            ))

    plot_stations(line, direction)
    plot_trains(line, direction)
    st.plotly_chart(fig)

if 'line' not in st.session_state:
    st.session_state.line = orangeline

if 'direction' not in st.session_state:
    st.session_state.direction = 1

if 'destination' not in st.session_state:
    st.session_state.destination = st.session_state.line.stations["station_name"].iloc[0] if st.session_state.direction else st.session_state.line.stations["station_name"].iloc[-1]


c1,c2, c3, c4 = st.columns([3,1,0.9,2.5], vertical_alignment="bottom")
linechoice = c1.selectbox("Select metro line", ("Orange [Vienna Fairfax-GMU to New Carrolton]", "Silver [Ashburn to Largo]", "Blue [Franconia-Springfield to Largo]"), index=0)

if linechoice == "Orange [Vienna Fairfax-GMU to New Carrolton]":
    st.session_state.line = orangeline
    st.session_state.destination = st.session_state.line.stations["station_name"].iloc[0] if st.session_state.direction else st.session_state.line.stations["station_name"].iloc[-1]

elif linechoice == "Silver [Ashburn to Largo]":
    st.session_state.line = silverline
    st.session_state.destination = st.session_state.line.stations["station_name"].iloc[0] if st.session_state.direction else st.session_state.line.stations["station_name"].iloc[-1]

elif linechoice == "Blue [Franconia-Springfield to Largo]":
    st.session_state.line = blueline
    st.session_state.destination = st.session_state.line.stations["station_name"].iloc[0] if st.session_state.direction else st.session_state.line.stations["station_name"].iloc[-1]

if c2.button("Switch Direction"):
    st.session_state.direction = 1 - st.session_state.direction  # Toggle direction
    st.session_state.destination = st.session_state.line.stations["station_name"].iloc[st.session_state.direction - 1]

if c3.button("Refresh Data"):
    feed_rt, predictions = get()

st.subheader(st.session_state.line.name+" Line trains going to "+st.session_state.destination)
c1, c2 = st.columns([2,1], vertical_alignment="center")
with c1:
    plot(st.session_state.line, st.session_state.direction)

with c2:
    if st.session_state.direction:
        st.write(st.session_state.line.stations[["station_name", "minute1"]].rename(columns={"station_name":"Station", "minute1":"Train arriving in (mins)"}))
    else:
        st.write(st.session_state.line.stations[["station_name", "minute0"]].rename(columns={"station_name":"Station", "minute0":"Train arriving in (mins)"}))
