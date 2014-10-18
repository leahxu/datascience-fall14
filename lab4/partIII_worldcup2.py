import pandas as pd
import numpy as np

worldcup = pd.read_csv('partIII_worldcup1.csv')
print worldcup.pivot(index='Team', columns='Year', values='Place')