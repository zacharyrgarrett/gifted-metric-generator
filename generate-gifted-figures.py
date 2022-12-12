import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import pandas as pd

from geopy.geocoders import Nominatim

from config import USER_DATA_PATH, FigurePaths

users_df = pd.read_csv(USER_DATA_PATH)



# User categories

def generate_user_categories():
    fig = go.Figure(data=[go.Pie(labels=users_df['Category'])])
    fig.update_traces(hoverinfo='label+percent', textinfo='label+value')
    fig.update_layout(
        title_text='User Categories',
        margin=dict(l=0,r=0,t=30,b=0)
    )
    fig.write_html(FigurePaths.USER_CATEGORIES)

def get_coordinates():
    lat, long = [], []
    formatted_location = []
    geolocator = Nominatim(user_agent="http")
    for index, row in users_df.iterrows():
        city = row.City
        country = "Turkey" if row.State == "TR" else "United States"
        loc = geolocator.geocode(f"{city},{country}")
        lat.append(loc.latitude)
        long.append(loc.longitude)
        formatted_location.append(loc.raw['display_name'])
    location_df = pd.DataFrame()
    location_df['Latitude'] = lat
    location_df['Longitude'] = long
    location_df['Location'] = formatted_location
    return location_df


def generate_user_socials():
    ig_count = users_df.groupby(['Instagram_Username']).ngroups
    tik_count = users_df.groupby(['TikTok_Username']).ngroups
    twit_count = users_df.groupby(['Twitter_Username']).ngroups
    socials = [
        dict(SocialMedia="Instagram", Users=ig_count),
        dict(SocialMedia="TikTok", Users=tik_count),
        dict(SocialMedia="Twitter", Users=twit_count),
    ]
    socials_df = pd.DataFrame(socials)
    fig = px.bar(socials_df, y='Users', x='SocialMedia', title='User Socials')
    fig.write_html(FigurePaths.USER_SOCIALS)

def generate_users_map():
    location_details = get_coordinates()
    city_grouped = location_details.groupby(['Location', 'Longitude', 'Latitude']).size().reset_index(name='counts')
    city_grouped['counts'] = city_grouped['counts'].map(str)
    
    fig1 = go.Figure(
            data=go.Scattergeo(
            lon = city_grouped['Longitude'],
            lat = city_grouped['Latitude'],
            text = "Total: " + city_grouped['counts'] + "<br>" + city_grouped['Location'],
            mode = 'markers'
            )
        )
    fig1.update_layout(
        title_text='User Location - USA',
        geo_scope='usa',
        margin=dict(l=0,r=0,t=30,b=0)
    )
    fig1.write_html(FigurePaths.USA_MAP_PATH)

    fig2 = go.Figure(
            data=go.Scattergeo(
            lon = city_grouped['Longitude'],
            lat = city_grouped['Latitude'],
            text = "Total: " + city_grouped['counts'] + "<br>" + city_grouped['Location'],
            mode = 'markers'
            )
        )
    fig2.update_layout(
        title_text='User Location - World',
        margin=dict(l=0,r=0,t=30,b=0),

        geo = dict(
            showland = True,
            showcountries = True,
            showocean = True,
            countrywidth = 0.5,
            landcolor = 'rgb(230, 145, 56)',
            lakecolor = 'rgb(0, 255, 255)',
            oceancolor = 'rgb(0, 255, 255)',
            projection = dict(
                type = 'orthographic',
                rotation = dict(
                    lon = -100,
                    lat = 40,
                    roll = 0
                )
            ),
            lonaxis = dict(
                showgrid = True,
                gridcolor = 'rgb(102, 102, 102)',
                gridwidth = 0.5
            ),
            lataxis = dict(
                showgrid = True,
                gridcolor = 'rgb(102, 102, 102)',
                gridwidth = 0.5
            )
        )   
    )
    fig2.write_html(FigurePaths.WORLD_MAP_PATH)


if __name__ == "__main__":
    #generate_user_categories()
    #generate_users_map()
    generate_user_socials()
