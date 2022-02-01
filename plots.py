from pandas import DataFrame, read_csv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
df = read_csv('dataframe_2022_1_17.csv', decimal=',', sep=';')
# Create figure and plot space
fig, ax = plt.subplots(figsize=(12, 12))

# Add x-axis and y-axis
ax.bar( df['Time'],
        df['General'],
        color='purple')

# Set title and labels for axes
ax.set(xlabel="Time",
       ylabel="Parking free slots",
       title="Daily Free Parking Slots")
'''
fig.autofmt_xdate()
# Use a more precise date string for the x axis locations in the toolbar.
ax.fmt_xdata = mdates.DateFormatter('%D-%H:%M')
'''
# Define the date format
time_form = DateFormatter("%H:%M")
ax.xaxis.set_major_formatter(time_form)

# Ensure a major tick for each week using (interval=1)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=10000))

plt.show()

