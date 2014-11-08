'''
Author: Leah Xu
Lab2: Part III
'''

import pandas as pd
import numpy as np
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

countries = []

reader = DataFileReader(open("countries.avro", "r"), DatumReader())

for country in reader:
	countries.append(country)
reader.close()

countries = pd.DataFrame(countries)

pop_ten_million = countries[countries.population > 10000000].name.count()

print "Number of countries with population over ten million:", pop_ten_million

# Output
# Number of countries with population over ten million: 36
