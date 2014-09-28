import pandas as pd
import numpy as np
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

countries = pd.DataFrame()
temp = []

reader = DataFileReader(open("countries.avro", "r"), DatumReader())

for country in reader:
	temp.append(countries.append(country, ignore_index=True))
reader.close()

countries = pd.concat(temp)

pop_ten_million = countries[countries.population > 10000000].name.count()

print "Number of countries with population over ten million:", pop_ten_million