import sqlite3
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

db_path = 'data/processed/box_office.db'
conn = sqlite3.connect(db_path)

df_movie = pd.read_sql_query("SELECT * FROM DimMovie", conn)
df_date = pd.read_sql_query("SELECT * FROM DimDate", conn)
df_revenue = pd.read_sql_query("SELECT * FROM FactRevenue", conn)

df = pd.merge(df_revenue, df_movie, on='movie_id')
df = pd.merge(df, df_date, on='date')

all_genres = set()
for s in df['genre'].str.split(', '):
    all_genres.update(s)
unique_genres = sorted(list(all_genres))
unique_years = sorted(df['year'].unique())

app = dash.Dash(__name__)
app.title = "Movie Revenue Rankings"

app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'backgroundColor': '#f2f2f2', 'padding': '20px'}, children=[
    html.H1(
        "Dashboard: Movie Revenue Rankings",
        style={'textAlign': 'center', 'color': '#2c3e50'}
    ),
    html.P(
        "Use the filters below to dynamically change the data in the rankings.",
        style={'textAlign': 'center', 'color': '#555'}
    ),

    # Filters Section
    html.Div([
        html.Div([
            html.Label("Select Year:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': year, 'value': year} for year in unique_years],
                placeholder="All years",
                style={'width': '100%'}
            )
        ], style={'width': '48%', 'display': 'inline-block', 'padding': '10px'}),

        html.Div([
            html.Label("Select Genre:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='genre-dropdown',
                options=[{'label': genre, 'value': genre} for genre in unique_genres],
                placeholder="All genres",
                style={'width': '100%'}
            )
        ], style={'width': '48%', 'display': 'inline-block', 'float': 'right', 'padding': '10px'})
    ], style={'backgroundColor': 'white', 'borderRadius': '8px', 'padding': '20px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'}),

    # Charts Section
    html.Div(id='charts-container', style={'marginTop': '20px'})
])


@app.callback(
    Output('charts-container', 'children'),
    [Input('year-dropdown', 'value'),
     Input('genre-dropdown', 'value')]
)
def update_charts(selected_year, selected_genre):
    # Copy the dataframe to avoid modifying the original
    filtered_df = df.copy()

    # Filter data based on user selections
    if selected_year:
        filtered_df = filtered_df[filtered_df['year'] == selected_year]
    if selected_genre:
        filtered_df = filtered_df[filtered_df['genre'].str.contains(selected_genre, na=False)]

    if filtered_df.empty:
        return html.Div("No data to display for the selected filters.", style={'textAlign': 'center', 'padding': '50px', 'fontSize': '20px'})
        
    # Prepare data for charts
    top_movies = filtered_df.groupby('title')['revenue_amount'].sum().nlargest(10).sort_values(ascending=True)
    top_directors = filtered_df.groupby('director')['revenue_amount'].sum().nlargest(10).sort_values(ascending=True)
    
    genre_revenue = filtered_df.copy()
    genre_revenue['genre'] = genre_revenue['genre'].str.split(', ')
    genre_revenue = genre_revenue.explode('genre')
    top_genres = genre_revenue.groupby('genre')['revenue_amount'].sum().sort_values(ascending=False)
    
    top_rated_movies = filtered_df.drop_duplicates(subset=['title']).nlargest(10, 'imdb_rating')[['title', 'imdb_rating']].set_index('title').sort_values(by='imdb_rating', ascending=True)

    # Create Plotly figures
    fig_movies = px.bar(top_movies, y=top_movies.index, x='revenue_amount', orientation='h', labels={'revenue_amount': 'Total Revenue', 'y': 'Movie Title'}, title="<b>Top 10: Most Profitable Movies</b>")
    fig_directors = px.bar(top_directors, y=top_directors.index, x='revenue_amount', orientation='h', labels={'revenue_amount': 'Total Revenue from Movies', 'y': 'Director'}, title="<b>Top 10: Most Profitable Directors</b>")
    fig_genres = px.bar(top_genres, x=top_genres.index, y='revenue_amount', labels={'revenue_amount': 'Total Revenue', 'x': 'Genre'}, title="<b>Revenue by Movie Genre</b>")
    fig_ratings = px.bar(top_rated_movies, y=top_rated_movies.index, x='imdb_rating', orientation='h', labels={'imdb_rating': 'IMDb Rating', 'y': 'Movie Title'}, title="<b>Top 10: Highest Rated Movies (IMDb)</b>")
    
    # Aesthetic settings for the charts
    for fig in [fig_movies, fig_directors, fig_genres, fig_ratings]:
        fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', font_color='#2c3e50', title_x=0.5, margin=dict(l=20, r=20, t=50, b=20), yaxis=dict(showgrid=False), xaxis=dict(gridcolor='#e6e6e6'))
    
    fig_movies.update_traces(marker_color='#1f77b4')
    fig_directors.update_traces(marker_color='#2ca02c')
    fig_genres.update_traces(marker_color='#ff7f0e')
    fig_ratings.update_traces(marker_color='#d62728')

    # Return charts to the layout
    charts = html.Div([
        html.Div([dcc.Graph(figure=fig_movies), dcc.Graph(figure=fig_directors)], style={'width': '49%', 'display': 'inline-block', 'padding': '10px 0', 'verticalAlign': 'top'}),
        html.Div([dcc.Graph(figure=fig_genres), dcc.Graph(figure=fig_ratings)], style={'width': '49%', 'display': 'inline-block', 'float': 'right', 'padding': '10px 0', 'verticalAlign': 'top'})
    ], style={'backgroundColor': 'white', 'borderRadius': '8px', 'marginTop': '20px', 'padding': '20px', 'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'})
    
    return charts

if __name__ == '__main__':
    app.run(debug=True)
