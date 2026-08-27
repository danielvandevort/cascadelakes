#  Accessing and filtering Daymet data
# https://developers.google.com/earth-engine/datasets/catalog/NASA_ORNL_DAYMET_V4#bands

import ee

try:
    ee.Initialize()
except Exception as e:
    print(e)
else:
    print('initialization successful...')
# Input data and data qualities
cascade_bounds = ee.FeatureCollection("path/to/AOI")
lakes_all = ee.FeatureCollection("path/to/lake_dataset")
# Define a workload tag for the entire script
ee.data.setDefaultWorkloadTag('daymet_extraction')

# Define the time period
startdate = '2014-01-01'
enddate = '2024-6-30'
# Need to "define" the dataset first.
daymet = ee.ImageCollection("NASA/ORNL/DAYMET_V4") \
    .filterBounds(cascade_bounds) \
    .filterDate(startdate, enddate) \
    .map(lambda img: img.clip(lakes_all)) \
    .filter(ee.Filter.calendarRange(10, 6, 'month')) # Oct thru Jun

num_images = daymet.size().getInfo()
# Print the number of images
print(f"Number of images after filtering: {num_images}")

# Extract daily max and min temperatures specific to each lake polygon
def extract_daily_temp_per_lake(lake):
    # Function to extract temperatures for a single lake
    def extract_temp(image):
        date = image.date().format("YYYY-MM-dd")
        tmax = image.select('tmax').reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=lake.geometry(),
            scale=1000,
            bestEffort=True,
            maxPixels=1e8
        )
        tmin = image.select('tmin').reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=lake.geometry(),
            scale=1000,
            bestEffort=True,
            maxPixels=1e8
        )
        return ee.Feature(None, {
            'lake_id': lake.get('ObjectID_1'),  # get the unique identifier for the lake, carry this thru the rest of the analysis
            'date': date,
            'tmax': tmax.get('tmax'),
            'tmin': tmin.get('tmin')
        })

    # Filter ImageCollection to only include images that overlap the specific lake
    filtered_daymet = daymet.filterBounds(lake.geometry())

    # Map the temperature extraction function over this filtered ImageCollection
    return filtered_daymet.map(extract_temp)

num_lakes = 3880 # change as needed
batch_size = 100  # Number of lakes per batch, change as needed

# Batch process
for i in range(0, num_lakes, batch_size):
    # Create a batch of lakes (slice the FeatureCollection)
    batch = ee.FeatureCollection(lakes_all.toList(num_lakes).slice(i, i + batch_size))
    # Process all lakes in the FeatureCollection
    lake_results = batch.map(extract_daily_temp_per_lake).flatten()
    # Export the results
    export_task = ee.batch.Export.table.toDrive( # goes to a Google Drive folder
        collection=ee.FeatureCollection(lake_results),
        description=f'daily_tempC_per_lake_M_J {i}-{i + batch_size if i + batch_size < num_lakes else num_lakes}',
        folder='Daymet_May_June',
        fileFormat='CSV',
        selectors=['lake_id', 'date', 'tmax', 'tmin']
    )
    export_task.start()

print("Export task started....")
