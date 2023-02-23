import os
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

import pandas as pd

from geopy.geocoders import Nominatim

from config import FEED_DATA_PATH, USER_DATA_PATH, BUSINESS_WEEKLY_PATH, BUSINESS_USER_LOCATIONS_PATH, BUSINESS_POSTS_USER_CATEGORY_PATH, FigurePaths

feed_df = pd.read_csv(FEED_DATA_PATH)
users_df = pd.read_csv(USER_DATA_PATH)


# User categories
def generate_user_categories():
    fig = go.Figure(data=[go.Pie(labels=users_df['Category'])])
    fig.update_traces(hoverinfo='label+percent', textinfo='label+value')
    fig.update_layout(
        title_text='User Categories',
        margin=dict(l=0,r=0,t=30,b=0)
    )
    fig.write_html(FigurePaths.USER_CATEGORIES, include_plotlyjs="directory")

def get_coordinates():
    lat, long = [], []
    formatted_location = []
    geolocator = Nominatim(user_agent="http")
    for index, row in users_df.iterrows():
        city = row.City
        country = "Turkey" if row.State == "TR" else "United States"
        state = "" if row.State == "TR" else f", {row.State}"
        loc = geolocator.geocode(f"{city}{state}, {country}")
        lat.append(loc.latitude)
        long.append(loc.longitude)
        formatted_location.append(loc.raw['display_name'])
    location_df = pd.DataFrame()
    location_df['Latitude'] = lat
    location_df['Longitude'] = long
    location_df['Location'] = formatted_location
    users_df['Location'] = formatted_location
    return location_df

# Influencer location by business
def find_user_location_by_business():
    user_business_grouped = feed_df.groupby(['UserName','BusinessName'])
    user_business_grouped = user_business_grouped.size().reset_index()
    user_business_grouped = user_business_grouped[['UserName','BusinessName']]
    user_business_grouped = user_business_grouped.set_index(['UserName'])

    user_business_grouped['Location'] = ""
    user_location = users_df[['UserName', 'Location']].set_index('UserName')
    for user in user_business_grouped.index.unique():
        user_business_grouped.loc[user, 'Location'] = user_location.loc[user, 'Location']
    
    user_business_grouped = user_business_grouped.reset_index().groupby(['BusinessName','Location']).count().reset_index()
    user_business_grouped.rename(columns={"UserName": "UserCount"}).to_csv(BUSINESS_USER_LOCATIONS_PATH, encoding='utf-8', index=False)

# Influencer types for posts by business
def find_influencer_type_per_deal_by_business():
    feed_entries = feed_df[['UserName','BusinessName']].to_dict('records')
    influencers = users_df.set_index('UserName')
    for i in range(0, len(feed_entries)):
        feed_entries[i] = feed_entries[i] | dict(Category=influencers.loc[feed_entries[i]['UserName'], 'Category'])
    category_summary = pd.DataFrame(feed_entries).groupby(['BusinessName', 'Category']).count().reset_index()
    category_summary.rename(columns={"UserName": "PostCount"}).to_csv(BUSINESS_POSTS_USER_CATEGORY_PATH, encoding='utf-8', index=False)

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
    fig.write_html(FigurePaths.USER_SOCIALS, include_plotlyjs="directory")

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
    fig1.write_html(FigurePaths.USA_MAP_PATH, include_plotlyjs="directory")

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
    fig2.write_html(FigurePaths.WORLD_MAP_PATH, include_plotlyjs="directory")

def convert_timestamp_to_date(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

def generate_feed_by_business_category():
    feed_df_grouped_weekly = feed_df.groupby([pd.Grouper(key='Week', freq='W-MON'), 'BusinessCategory'])['UserName']\
        .count()\
        .reset_index()\
        .sort_values('Week')\
        .rename(columns={"UserName": "Deal_Count"})
    fig = px.line(feed_df_grouped_weekly, x="Week", y="Deal_Count", color='BusinessCategory',\
        title="Weekly Deals Per Business Category")
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    fig.write_html(FigurePaths.FEED_BY_BUSINESS_CATEGORY, include_plotlyjs="directory")

def generate_first_post_summarization():
    username = []
    date = []
    for index, row in feed_df.iterrows():
        if row["UserName"] not in username:
            username.append(row["UserName"])
            date.append(row["TimePosted"])
    data = {
        'Username': username,
        'Date': date
        }
    first_post_df = pd.DataFrame(data)
    first_post_df['Week'] = pd.to_datetime(first_post_df['Date']) - pd.to_timedelta(7, unit='d')

    first_post_grouped_weekly = first_post_df.groupby([pd.Grouper(key='Week', freq='W-MON')])['Username']\
        .count()\
        .reset_index()\
        .sort_values('Week')\
        .rename(columns={"Username": "Weekly_First_Post_Count"})

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=first_post_grouped_weekly["Week"], y=first_post_grouped_weekly["Weekly_First_Post_Count"], name="Weekly First Post Count"),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(
            x=first_post_grouped_weekly["Week"],
            y=first_post_grouped_weekly["Weekly_First_Post_Count"].cumsum(), name="Cumulative First Posts"),
            secondary_y=True,
    )

    fig.update_layout(
        title_text="First Posts by Week + Cumulative Overlay",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    # Set x-axis title
    fig.update_xaxes(title_text="Week")

    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Weekly</b> First Posts", secondary_y=False)
    fig.update_yaxes(title_text="<b>Cumulative</b> First Posts", secondary_y=True)

    fig.write_html(FigurePaths.FIRST_POST_BY_WEEK, include_plotlyjs="directory")

def generate_business_weekly_summary():
    now = datetime.now()
    current_week = datetime.combine((now - timedelta(days = now.weekday())).date(), datetime.min.time())

    # Make feed copy and filter
    feed_copy = feed_df.copy()
    feed_grouped_weekly = feed_copy.groupby(['BusinessName', pd.Grouper(key='Week', freq='W-MON')]).agg({'BrandReview': ['mean'], 'ProductQuality': ['mean'], 'UserName': ['count']})
    feed_grouped_weekly = feed_grouped_weekly.reset_index()
    feed_grouped_weekly.columns = ["BusinessName", "Week", "BrandReview", "ProductReview", "DealCount"]

    # Multi-grouping to find percent of recommendations
    recommendation_grouped = feed_copy[feed_copy['Recommendation'] == True].groupby(['BusinessName', pd.Grouper(key='Week', freq='W-MON'), 'Recommendation'])
    recommendation_agg = recommendation_grouped.agg({'Recommendation': ['count']})
    recommendation_agg = recommendation_agg.reset_index()
    recommendation_agg.columns = ["BusinessName", "Week", "RecommendationGroup", "Recommendation_Yes"]

    # Format values
    feed_grouped_weekly['RecommendedPercentage'] = round(recommendation_agg['Recommendation_Yes'] / feed_grouped_weekly['DealCount'], 2)
    feed_grouped_weekly['BrandReview'] = feed_grouped_weekly['BrandReview'].round(2)
    feed_grouped_weekly['ProductReview'] = feed_grouped_weekly['ProductReview'].round(2)
    feed_grouped_weekly.loc[:, 'RecommendedPercentage'] = feed_grouped_weekly['RecommendedPercentage'].map('{:.2%}'.format)
    
    # Fill in empty time steps - will make graphs look better
    business_names = feed_grouped_weekly['BusinessName'].unique()
    week_inc = feed_grouped_weekly['Week'].min()
    feed_grouped_weekly = feed_grouped_weekly.set_index(['BusinessName', 'Week'])
    while week_inc <= current_week - timedelta(weeks=1):
        for business_name in business_names:
            week_str = datetime.combine(week_inc, datetime.min.time())
            if (business_name, week_str) not in feed_grouped_weekly.index:
                feed_grouped_weekly.loc[(business_name, week_str),:] = ('','',0,'')
        week_inc = week_inc + timedelta(weeks = 1)

    # Reformat and save
    feed_grouped_weekly = feed_grouped_weekly.reset_index()
    feed_grouped_weekly = feed_grouped_weekly.sort_values(by=['Week', 'BusinessName'])
    feed_grouped_weekly = feed_grouped_weekly[['Week', 'BusinessName', 'DealCount', 'BrandReview', 'ProductReview', 'RecommendedPercentage']]
    feed_grouped_weekly.to_csv(BUSINESS_WEEKLY_PATH, encoding='utf-8', index=False)

def assign_week_to_deals():
    global feed_df
    feed_df['Week'] = pd.to_datetime(feed_df['DealStarted']) - pd.to_timedelta(7, unit='d')
    feed_df = feed_df.sort_values('DealStarted')

def verify_prerequisites():
    if not os.path.exists("./figures"):
        os.makedirs("./figures")

if __name__ == "__main__":
    # Verify paths exist
    verify_prerequisites()

    # User Figures
    generate_user_categories()
    generate_user_socials()
    generate_users_map()

    # Feed Figures
    assign_week_to_deals()
    generate_feed_by_business_category()
    generate_first_post_summarization()

    # Generate additional reports
    generate_business_weekly_summary()
    find_user_location_by_business()
    find_influencer_type_per_deal_by_business()
