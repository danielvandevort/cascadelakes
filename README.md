# cascadelakes
Information for Cascade Mountain Range lake ice phenology study.

Links to the publically available data used in this study are as follows: 
* National Hydrography dataset: https://apps.nationalmap.gov/downloader/
* National Elevation Dataset: https://apps.nationalmap.gov/downloader/
* HLS-2: https://www.earthdata.nasa.gov/data/projects/hls
* DayMet V4 1 km air Temperature: https://doi.org/10.3334/ORNLDAAC/1840

The codework for this study was performed in the DataSpell 2026.1.2 IDE. The code language was Python 3.12. Jupyter notebooks
contain the analyses and individual Python scripts contain data acquisition code. We have done our best to make the code reproducible and easy to follow. However, please take time
to review the code and check for bugs. Package versions update frequently and may not always be compatible. 
Also check the string names for input data. Ensure your data is either named to match, or just change the names in the code.
The acquisition scripts that use the Google Earth Engine API require a Google account; Google updates its user policies frequently, so please check the Google Earth Engine documentation for any changes to the API.
It may be simpler to convert those Python scripts to Javascript files and run them directly in the Google Earth Engine code editor.
