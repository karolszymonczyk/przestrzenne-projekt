
# Code structure of the project

All the source code used for the stance detection is located in the `stance-detetion/` directory. Inside there are a bunch of subdirectories containing the code for different stages of the project:

- `stance-detetion/scraper/` - the web scraper written using the [stweet](https://github.com/markowanga/stweet) library
- `stance-detetion/data-processing/` - downloading the dataset from the database and sampling the data
- `stance-detetion/annotate_test/` - calculating the annotators' agreement
- `stance-detetion/classify/` - implementation of different classifiers
- `stance-detetion/data-analysis` - analysis of the results

Spatial analysis is located in the `spatial-analysis/` directory.