import pandas as pd
import numpy as np

# Assumes we are dealing with data at 1Hz (1 second between timestamps)

# Build First 10 Second Dataset with some NaN Values
range1 = pd.date_range('2022-01-01T00:00:00.000Z', '2022-01-01T00:00:10.000Z', freq='1s')
df1 = pd.DataFrame(index = range1)
df1['speed'] = [np.NaN,1,2,3,4,5,np.NaN,7,8,9,np.NaN]
df1['distance'] = [0,10,20,30,np.NaN,50,60,70,80,90,100]
# introduce a missing second
df1.drop(df1.index[5], inplace=True) 

# introduce a short section of data (3 second blip)
range2 = pd.date_range('2022-01-01T00:03:00.000Z', '2022-01-01T00:03:02.000Z', freq='1s')
df2 = pd.DataFrame(index = range2)
df2['speed'] = [99,99,99]
df2['distance'] = [999,999,999]

# Build Second 10 Second Dataset with some NaN Values
range3 = pd.date_range('2022-01-01T00:05:00.000Z', '2022-01-01T00:05:10.000Z', freq='1s')
df3 = pd.DataFrame(index = range3)
df3['speed'] = [np.NaN,10,20,30,40,50,np.NaN,70,80,90,100]
df3['distance'] = [np.NaN,100,200,300,400,500,600,700,800,900,np.NaN]

# Combine sample datasets into one DataFrame
df=pd.concat([df1,df2,df3])
print (df)

# Find blocks of consecutive timestamp data and fillna individually
# https://stackoverflow.com/questions/14358567/finding-consecutive-segments-in-a-pandas-data-frame

# Find Small Gaps
df['unixtime']=df.index.astype(np.int64) // 10**9           # convert timestamps to integers for compare purposes
df['deltaT'] = df['unixtime'].diff().div(1, fill_value=1)   # calc the time gap between timestamps - assumes first timestamp is start of consecutive series (fill_value=1)
df.loc[df['deltaT'] <=2, 'deltaT'] = 1                       # if the gap is less than 2 second threat it as part of same consecutive timestamps group
print (df)

# Find Big Gaps
df['block'] = ((df['deltaT'].shift(1) != df['deltaT']) & (df['unixtime'].shift(1) != df['unixtime']-1)).astype(int).cumsum()  # identify blocks markers for groups of consective timestamps 
blockDF=df.reset_index().groupby(['block'])['index'].apply(np.array) # group block markers 
print (df)
print (blockDF)
df.drop(['unixtime','block','deltaT'], axis=1, inplace=True) # cleanup added compute columns


# Remove blocks that are less than 3 seconds (Blips)
for block_id in blockDF.index:
    startTS=blockDF.loc[block_id][0].to_pydatetime()    # Get the first timestamp of the block
    endTS=blockDF.loc[block_id][-1].to_pydatetime()     # Get the last timestamp of the block
    blockDuration=(endTS-startTS).seconds               # calculate block duration
    if blockDuration <=3:
        print (f" - Found block <= 3 seconds - Removing block {block_id} from dataframe")
        df.drop(index=blockDF.loc[block_id], axis=0, inplace=True)
        blockDF.drop(index=block_id, axis=0, inplace=True)
print (blockDF)

# iterate over all the identified blocks remove small blips and fillNaN
for block_id in blockDF.index:
    startTS=blockDF.loc[block_id][0].to_pydatetime()    # Get the first timestamp of the block
    endTS=blockDF.loc[block_id][-1].to_pydatetime()     # Get the last timestamp of the block
    blockDuration=(endTS-startTS).seconds               # calculate block duration
    print("Processing Block", block_id, startTS, endTS, blockDuration )

    df[startTS: endTS]=df[startTS: endTS].interpolate(method="linear", axis=0).ffill().bfill()
    #df[startTS: endTS]=df[startTS: endTS].interpolate(method="linear", axis=0).bfill().ffill()
    #df[startTS: endTS]=df[startTS: endTS].interpolate(method="bfill", axis=0).ffill()

# Print Final DataFrame       
print (df)