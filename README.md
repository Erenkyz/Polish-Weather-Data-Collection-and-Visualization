# Polish Weather Data Collection and Visualization

A couple of weeks ago, I got curious about how the weather actually changes day-to-day in Poland during winter. I was wondering: Is it really as cold and cloudy as people say? Does it vary a lot between cities? So I decided to build a small project to find out for myself.

I picked the four biggest cities — Warsaw, Krakow, Wrocław, and Gdańsk — and started collecting real-time weather data using the free OpenWeatherMap API. I ran the script manually three times a day (morning, afternoon, and evening) from December 15, 2025, to December 28, 2025. In the end, I gathered 108 records.

The data is stored in a PostgreSQL database, and I also exported everything to a CSV file so it's easy to play with. Then I used Matplotlib and Seaborn in a Jupyter notebook to create some nice visualizations — time series for temperature trends, heatmaps, box plots, and a breakdown of weather conditions (turns out it was mostly cloudy!).


## How I Did It

Data Collection

I wrote collect_weather.py — it calls the OpenWeatherMap API for each city, cleans the response, and saves new records to PostgreSQL. I just ran:

``` bash
python collect_weather.py
```

three times daily. It even tells me progress and suggests the next run time!

Exporting

Once I was done collecting, I ran export_to_csv.py to dump everything into weather_data.csv.

Visualization

Opened WeatherData_Visualization.ipynb and created a bunch of plots to see trends, compare cities, and check how often it was cloudy, foggy, clear, etc.

## What I Learned

Beginning of winter 2025 was pretty mild in the beginning but got colder toward the end of December.
Gdańsk was often the "warmest" (thanks to the Baltic Sea), while inland cities like Krakow and Wrocław felt colder.
Clouds dominated — over 50% of the time it was cloudy or overcast!

If you're curious about Polish winter weather or just want to see a simple ETL + visualization project in action, feel free to clone this repo and explore.

## Contributing

If you want to add more cities, automate the scheduling, or build a little dashboard — go for it! Pull requests are very welcome 😊

## License

MIT License

Thanks for checking out my little weather project! 🌤️

January 2026

Eren Kaymaz

