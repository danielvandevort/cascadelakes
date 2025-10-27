import ee

'''
The combined measurement (Landsat 8 and S2) enables global 
observations of the land every 2 to 3 days at 30m spatial resolution.
USER_INPUT is noted where the user will be required to modify the script for their own use.
'''
try:
    ee.Initialize()
except Exception as e:
    print(e)
else:
    print('initialization successful...')

# Input data and data qualities
cascade_bounds = ee.FeatureCollection("USER_INPUT")
lakes = ee.FeatureCollection("USER_INPUT")

# Define the time period
startDate = 'USER_INPUT' # for example, 2024-01-01
endDate = 'USER_INPUT'
num_lakes = 1000 # CHANGE to the actual number of lakes in data
batch_size = 100  # Number of lakes per batch

# Filtering for both HLSS and HLSL
def filter_collection(collection):
    return ee.ImageCollection(collection) \
        .filterBounds(cascade_bounds) \
        .filterDate(startDate, endDate) \
        .filter(ee.Filter.calendarRange(5 , 6, 'month')) \
        .filter(ee.Filter.lt('CLOUD_COVERAGE', 50)) \
        .map(lambda img: img.clip(lakes))


print('Searching HLSS and HLSL collections...')
hlss = filter_collection('NASA/HLS/HLSS30/v002')
hlsl = filter_collection('NASA/HLS/HLSL30/v002')
hls_combined = hlss.merge(hlsl).sort('system:time_start')
#Optional report the number of images
print(f"Number of Sentinel images after filtering: {hlss.size().getInfo()}")
print(f"Number of Landsat images after filtering: {hlsl.size().getInfo()}")


def calc_data(image, currentLake):
    # Determine if the image is Sentinel-2 or Landsat-8
    properties = image.propertyNames()
    is_sentinel = properties.contains('PRODUCT_URI')
    is_landsat = properties.contains('LANDSAT_PRODUCT_ID')

    # Select SWIR1 band based on the satellite. When either is not present, it doesn't matter...
    swir_band = ee.Image(0)  # Default to 0
    swir_band = ee.Image(ee.Algorithms.If(is_sentinel, image.select('B11'), swir_band))
    swir_band = ee.Image(ee.Algorithms.If(is_landsat, image.select('B6'), swir_band))
    swir_band = swir_band.rename('SWIR')  # Rename the selected SWIR band
    # Calculate NDSI using the selected SWIR band
    ndsi = image.addBands(swir_band).normalizedDifference(['B3', 'SWIR']).rename('NDSI')
    # NIR selection
    nir_s = ee.Image(0)  # Default NIR_S to 0
    nir_l = ee.Image(0)  # Default NIR_L to 0
    nir_s = ee.Image(ee.Algorithms.If(is_sentinel, image.select('B8'), nir_s)).rename('NIR_S')  # 'NIR_Broad'
    nir_l = ee.Image(ee.Algorithms.If(is_landsat, image.select('B5'), nir_l)).rename('NIR_L')
    # Combine the bands
    combined = image.addBands([ndsi, nir_s, nir_l])
    #  get pixel-based values
    values = combined.reduceRegion(  # this returns an ee.Dictionary
        reducer=ee.Reducer.toList(),
        geometry=currentLake.geometry(),
        scale=30  # HLS imagery is 30m resolution.
    )
    # each is an ee.List object
    ndsi_values = values.get('NDSI')

    def create_feature(i):
        return ee.Feature(None, {
            'date': image.date().format('YYYY-MM-dd'),
            'lake_id': currentLake.get("USER_INPUT"), # for example, "ObjectID_1"
            'NDSI': ee.List(values.get("NDSI")).get(i),
            'NIR_S': ee.List(values.get("NIR_S")).get(i), # Sentinel-2
            'NIR_L': ee.List(values.get("NIR_L")).get(i)  # Landsat
        })

    # Create a sequence of indices for mapping
    index_list = ee.List.sequence(0, ee.List(ndsi_values).size().subtract(1))
    # Map the list of values to a FeatureCollection
    return ee.FeatureCollection(index_list.map(create_feature))


def process_lakes(thislake):
    def get_data(image):
        all_data = calc_data(image, thislake)
        return all_data

    # Combine the two collections
    return hls_combined.map(get_data).flatten()


# OPTIONAL Batch process
# for i in range(0, num_lakes, batch_size):
#     # Create a batch of lakes (slice the FeatureCollection)
#     batchlakes = ee.FeatureCollection(lakes.toList(num_lakes).slice(i, i + batch_size))
#     results = batchlakes.map(process_lakes).flatten()
#     # Define export task for the current batch
#     export_task = ee.batch.Export.table.toDrive(
#         collection=results,
#         description=f'USER_INPUT_batch_{i}',
#         folder='USER_INPUT',
#         fileFormat='CSV',
#         selectors=['date', 'lake_id', 'NDSI', 'NIR_S', 'NIR_L']
#     )
#     export_task.start()


laketoexport = lakes.map(process_lakes).flatten()
export = ee.batch.Export.table.toDrive(
    collection=laketoexport,
    description=f'USER_INPUT',
    folder='USER_INPUT',
    fileFormat='CSV',
    selectors=['date', 'lake_id', "NDSI", "NIR_L", 'NIR_S']
)
export.start()

print('submitted export task for combined NDSI and NIR values...')
