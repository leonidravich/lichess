import os
import pandas as pd
import matplotlib.pyplot as plt
import dask.dataframe as dd


base_dir = '/home/ravich/lichess/'

def split_per_event(file):
    df = dd.read_parquet(file)
    df.set_index('Event')
    df['Event'] = df['Event'].apply(lambda x: ' '.join(x.split()[:-1]), meta=('Event', 'string'))
    df = df[~df['Event'].str.contains('\?')]
    df['Date'] = dd.to_datetime(df['Date'])
    df = df[~df['Event'].str.contains('swiss')]
    df['Opening'] = df['Opening'].str.split(':').str[0]
    df['Opening'] = df['Opening'].str.split(',').str[0]
    df = df.drop(['AverageElo'], axis=1)
    unique_events = df['Event'].dropna().unique().compute()

    # filter out minor events
    for event in unique_events:
        filtered_ddf = df[df['Event'] == event]
        filtered_ddf = filtered_ddf.drop(['Event'], axis=1)
        result_ddf = filtered_ddf.compute()
        output_file = os.path.join(base_dir, f'pgn_headers_{"_".join(event.split())}.parquet')
        result_ddf.to_parquet(output_file, index=False)
        del result_ddf
        print(f'event {event} saved')


def agg_openings(df, event):
    """
    Processes the DataFrame in place to categorize openings for a given event,
    filters out unknown openings, and assigns 'Top' or 'Other' categories based on frequency.

    This function modifies the DataFrame in place for efficiency.

    Parameters:
    - df (pd.DataFrame): DataFrame to be modified.
    - event (str): The name of the event.

    WARNING: This function modifies the input DataFrame in place.
    """
    # Simplify opening names and remove rows with 'Unknown' openings directly
    df['Opening'] = df['Opening'].str.split(',').str[0]
    df.drop(df[df['Opening'].str.contains('Unknown')].index, inplace=True)

    print(f'Event {event} - Total number of games: {df.shape[0]}')

    # Group by 'Opening', resample by month end, and compute size
    monthly_openings = df.groupby('Opening').resample('ME', on='Date').size()
    sorted_monthly_openings = monthly_openings.sort_values(ascending=False).groupby(level='Date')

    # Identify top 5 openings per month and collect them in a set
    filter_opening_set = set()
    for month, openings in sorted_monthly_openings:
        top_openings = openings.head(5).index.get_level_values('Opening')
        filter_opening_set.update(top_openings)

    # Apply the filter directly on the DataFrame
    df['Filtered Opening'] = df['Opening'].apply(
        lambda x: x if x in filter_opening_set else 'Other Opening'
    )

    # Optionally, only have top 10 as 'Top', others as 'Other'
    top_10_openings = df['Filtered Opening'].value_counts().nlargest(10).index
    df['Filtered Opening'] = df['Filtered Opening'].where(
        df['Filtered Opening'].isin(top_10_openings), 'Other Opening'
    )

def plot_all(group_df, event):
    start = group_df.head(1)["Date"].dt.year.item()
    end = group_df.tail(1)["Date"].dt.year.item()

    openings = group_df['Filtered Opening'].value_counts()

    print(openings)

    plt.figure(figsize=(10, 7))  # Set the figure size
    plt.pie(openings, labels=openings.index, autopct='%1.1f%%', startangle=140)
    plt.title(f'Top 10 Chess Openings {event} {start}-{end}')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.savefig(os.path.join(base_dir, f"pie_{event}.png"), bbox_inches='tight')
    plt.show()

    total_games_per_month = group_df.resample('ME', on='Date').size()
    # total_games_per_month = total_games_per_month[total_games_per_month > 0]
    monthly_counts = group_df.groupby(['Filtered Opening']).resample('ME', on='Date').size()
    # monthly_counts = monthly_counts[monthly_counts > 0]
    monthly_counts_fixed = monthly_counts.div(total_games_per_month, level='Date', fill_value=0)
    monthly_counts_fixed = monthly_counts_fixed.dropna()
    fig, ax = plt.subplots(figsize=(14, 7))
    monthly_counts_fixed.unstack('Filtered Opening').plot(kind='area', stacked=True, title=f"Opening Trends in {event} {start}-{end}", ax=ax)

    #ax.legend(title='Opening', fontsize='small', title_fontsize='medium', loc='upper left', bbox_to_anchor=(1, 1))
    plt.tight_layout(rect=[0, 0, 0.9, 1])
    plt.savefig(os.path.join(base_dir, f"trend_{event}.png"), bbox_inches='tight')
    plt.show()


def main():
    file_path = os.path.join(base_dir, 'pgn_headers.csv')
    split_per_event(file_path)

    files = os.listdir(base_dir)
    parquet_files = [file for file in files if file.endswith('.parquet')]
    for file in parquet_files:
        event = file[len('pgn_headers_'):file.find('.parquet')].replace('_', ' ')
        print(f'Processing {event}')
        group_df = pd.read_parquet(os.path.join(base_dir, file))
        group_df['Date'] = pd.to_datetime(group_df['Date'])
        agg_openings(group_df, event)
        plot_all(group_df, event)

if __name__ == '__main__':
    main()