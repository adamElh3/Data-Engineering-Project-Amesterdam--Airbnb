#!/usr/bin/env python
# coding: utf-8

# In[583]:


from IPython.display import display,HTML
display(HTML("<style>pre { white-space: pre !important; }</style>"))


# # Extraction

# In[584]:


import os
print(os.listdir("/opt/airflow/spark/app/amesterdam_files"))


# In[585]:


import pandas as pd
listings = pd.read_csv('/opt/airflow/spark/app/amesterdam_files/listings.csv',low_memory=False)
listings_details=pd.read_csv('/opt/airflow/spark/app/amesterdam_files/listings_details.csv',low_memory=False)


# In[586]:


#listings.columns


# In[587]:


listings_details.columns


# # Pandas Transformations

# In[588]:


target_columns = [
     'id','street',
    'host_response_time', 'host_response_rate', 'host_acceptance_rate',
    'host_is_superhost', 'host_listings_count', 'host_total_listings_count',
    'neighbourhood_cleansed', 'property_type',
     'accommodates', 'bathrooms', 'bedrooms', 'beds', 'amenities',
    'square_feet',  'weekly_price', 'monthly_price', 'security_deposit',
    'cleaning_fee', 
    'maximum_nights', 'availability_30', 'availability_60', 'availability_90',
    'review_scores_rating',
    'review_scores_cleanliness',
     'review_scores_value', 
    'instant_bookable',  'cancellation_policy',
]






# In[589]:


listings.columns


# In[590]:


listings = pd.merge(listings, listings_details[target_columns], on='id', how='left')
#listings.info()


# In[591]:


listings.columns


# # Pandas Cleaning

# In[592]:


#placing NaN values with 0
listings.review_scores_rating.fillna(0, inplace=True)
listings.reviews_per_month.fillna(0, inplace=True)
listings.host_listings_count.fillna(0, inplace=True)
listings.review_scores_cleanliness.fillna(0, inplace=True)
listings.review_scores_value.fillna(0, inplace=True)
#listings.last_review.fillna('unknown_date', inplace=True)
listings.host_response_time.fillna('within a few hours',inplace=True)


# # Spark Session

# In[593]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *


# In[594]:


#spark=SparkSession.builder.\
#master('spark://3b7cb4581ea0:7077').\
#appName('amsterdam').\
#getOrCreate()
spark = (SparkSession
    .builder
    .getOrCreate()
)


# In[595]:


df_listing=spark.createDataFrame(listings) #without schema
#df_neighb=spark.createDataFrame(neighbourhoods)


# In[596]:


df_listing.printSchema()


# In[597]:


df_listing.schema


# In[598]:


schema_listings=StructType([StructField('id', IntegerType(), True), 
                            StructField('name', StringType(), True), 
                            StructField('host_id', IntegerType(), True), 
                            StructField('host_name', StringType(), True), 
                            StructField('neighbourhood_group', FloatType(), True), 
                            StructField('neighbourhood', StringType(), True), 
                            StructField('latitude', FloatType(), True), 
                            StructField('longitude', FloatType(), True), 
                            StructField('room_type', StringType(), True), 
                            StructField('price', IntegerType(), True), 
                            StructField('minimum_nights', IntegerType(), True), 
                            StructField('number_of_reviews', IntegerType(), True), 
                            StructField('last_review', StringType(), True), 
                            StructField('reviews_per_month', FloatType(), True), 
                            StructField('calculated_host_listings_count', IntegerType(), True), 
                            StructField('availability_365', IntegerType(), True), 
                            StructField('street', StringType(), True),   
                            StructField('host_response_time', StringType(), True), 
                            StructField('host_response_rate', StringType(), True), 
                            StructField('host_acceptance_rate', StringType(), True), 
                            StructField('host_is_superhost', StringType(), True), 
                            StructField('host_listings_count', FloatType(), True), 
                            StructField('host_total_listings_count', FloatType(), True), 
                            StructField('neighbourhood_cleansed', StringType(), True), 
                            StructField('property_type', StringType(), True), 
                            StructField('accommodates', IntegerType(), True), 
                            StructField('bathrooms', FloatType(), True), 
                            StructField('bedrooms', FloatType(), True), 
                            StructField('beds', FloatType(), True), 
                            StructField('amenities', StringType(), True), 
                            StructField('square_feet', FloatType(), True), 
                            StructField('weekly_price', StringType(), True), 
                            StructField('monthly_price', StringType(), True), 
                            StructField('security_deposit', StringType(), True), 
                            StructField('cleaning_fee', StringType(), True),  
                            StructField('maximum_nights', IntegerType(), True), 
                            StructField('availability_30', IntegerType(), True), 
                            StructField('availability_60', IntegerType(), True), 
                            StructField('availability_90', IntegerType(), True), 
                            StructField('review_scores_rating', FloatType(), True), 
                            StructField('review_scores_cleanliness', FloatType(), True),  
                            StructField('review_scores_value', FloatType(), True), 
                            StructField('instant_bookable', StringType(), True), 
                            StructField('cancellation_policy', StringType(), True)])


# # Spark Cleaning

# In[599]:


df_listing=spark.createDataFrame(listings,schema=schema_listings)
df_listing.show()


# In[600]:


listings.info()


# # Dropping some columns

# In[601]:


#we have all neighbourhood_group as nan so we are going to dop this column
df_non_nan_neighb=df_listing.filter(~f.isnan(f.col('neighbourhood_group'))).count()
df_non_nan_neighb


# In[602]:


df_non_nan_host_acceptance_rate=df_listing.filter(~f.isnan(f.col('host_acceptance_rate'))).count()
df_non_nan_host_acceptance_rate


# In[603]:


drop_columns=['neighbourhood_group','host_acceptance_rate']
df_listing=df_listing.drop(*drop_columns)
df_listing.show(1)


# In[604]:


df_listing.printSchema()


# # Replacing some column-values 

# In[605]:


df_listing = df_listing.withColumn(
    'host_is_superhost',
    f.when(f.col('host_is_superhost') == 't', 'True')
    .otherwise('False')
)


# In[606]:


df_listing = df_listing.withColumn(
    'instant_bookable',
    f.when(f.col('instant_bookable') == 't', 'True')
    .otherwise('False')
)


# In[607]:


df_listing.cache()


# In[608]:


df_listing.show(truncate=False)


# # Dropping dollar symbol from some columns

# In[609]:


df_listing=df_listing.withColumn('weekly_price',f.regexp_replace(f.col('weekly_price'),'\$',''))
df_listing=df_listing.withColumn('monthly_price',f.regexp_replace(f.col('monthly_price'),'\$',''))
df_listing=df_listing.withColumn('security_deposit',f.regexp_replace(f.col('security_deposit'),'\$',''))
df_listing=df_listing.withColumn('cleaning_fee',f.regexp_replace(f.col('cleaning_fee'),'\$',''))
df_listing=df_listing.withColumn('host_response_rate', f.regexp_extract(f.col('host_response_rate'), r'(\d+)', 1))



# # Dropping nan values in a specific column

# In[610]:


df_listing = df_listing.dropna(subset=['host_name'])


# In[611]:


nan_rows = df_listing.filter(f.isnan(f.col('host_name')))
nan_rows.show()


# In[612]:


nan_columns=[6345018,10227700,13585881,18661034,21892378]

df_listing = df_listing.filter(~f.col('id').isin(nan_columns))
df_listing.filter(f.isnan(f.col('host_name'))).show()


# # Casting

# In[613]:


df_listing=df_listing.withColumn('beds',f.col('beds').cast('integer'))
df_listing=df_listing.withColumn('bathrooms',f.col('bathrooms').cast('integer'))
df_listing=df_listing.withColumn('bedrooms',f.col('bedrooms').cast('integer'))
df_listing=df_listing.withColumn('weekly_price',f.col('weekly_price').cast('integer'))
df_listing=df_listing.withColumn('monthly_price',f.col('monthly_price').cast('integer'))
df_listing=df_listing.withColumn('security_deposit',f.col('security_deposit').cast('integer'))
df_listing=df_listing.withColumn('cleaning_fee',f.col('cleaning_fee').cast('integer'))
df_listing = df_listing.withColumn("last_review",f.from_utc_timestamp("last_review", "UTC"))
df_listing=df_listing.withColumn('host_response_rate',f.col('host_response_rate').cast('float'))
df_listing=df_listing.withColumn('host_is_superhost',f.col('host_is_superhost').cast('boolean'))
df_listing=df_listing.withColumn('Instant_bookable',f.col('Instant_bookable').cast('boolean'))
df_listing.printSchema()


# # Replacing nan values with the average values 

# In[614]:


#replacing nan values by the average of beds
df_non_nan=df_listing.na.drop(subset=['beds'])
df_avg_beds=df_non_nan.agg(f.avg('beds').alias('average_of_beds'))
df_avg_beds.show()


#replacing nan values by the average of bedrooms 
df_non_nan=df_listing.na.drop(subset=['bedrooms'])
df_avg_bedrooms=df_non_nan.agg(f.avg('bedrooms').alias('average_of_bedrooms'))
df_avg_bedrooms.show()



#replacing nan values by the average of bathrooms
df_non_nan=df_listing.na.drop(subset=['bathrooms'])
df_avg_bathrooms=df_non_nan.agg(f.avg('bathrooms').alias('average_of_bathrooms'))
df_avg_bathrooms.show()





#replacing nan values by the average of weekly_price
df_non_nan=df_listing.na.drop(subset=['weekly_price'])
df_avg_weekly_price=df_non_nan.agg(f.avg('weekly_price').alias('average_of_weekly_price'))
df_avg_weekly_price.show()





#replacing nan values by the average of monthly_price
df_non_nan=df_listing.na.drop(subset=['monthly_price'])
df_avg_monthly_price=df_non_nan.agg(f.avg('monthly_price').alias('average_of_monthly_price'))
df_avg_monthly_price.show()



#replacing nan values by the average of security_deposit
df_non_nan=df_listing.na.drop(subset=['security_deposit'])
df_avg_security_deposit=df_non_nan.agg(f.avg('security_deposit').alias('average_of_security_deposit'))
df_avg_security_deposit.show()



#replacing nan values by the average of cleaning_fee
df_non_nan=df_listing.na.drop(subset=['cleaning_fee'])
df_avg_cleaning_fee=df_non_nan.agg(f.avg('cleaning_fee').alias('average_of_cleaning_fee'))
df_avg_cleaning_fee.show()


#replacing nan values by the average of square_feet 
df_non_nan=df_listing.na.drop(subset=['square_feet'])
df_avg_square_feet=df_non_nan.agg(f.avg('square_feet').alias('average_of_square_feet'))
df_avg_square_feet.show()


#replacing nan values by the average of square_feet 
df_non_nan=df_listing.na.drop(subset=['host_response_rate'])
df_avg_host_response_rate=df_non_nan.agg(f.avg('host_response_rate').alias('average_of_host_response_rate'))
df_avg_host_response_rate.show()



#replacing nan values by the average of square_feet 
df_non_nan=df_listing.na.drop(subset=['host_total_listings_count'])
df_average_of_host_total_listings_count=df_non_nan.agg(f.avg('host_total_listings_count').alias('average_of_host_total_listings_count'))
df_average_of_host_total_listings_count.show()

average_of_beds = df_avg_beds.first()['average_of_beds']
average_of_bedrooms=df_avg_bedrooms.first()['average_of_bedrooms']
average_of_bathrooms=df_avg_bathrooms.first()['average_of_bathrooms']
average_of_weekly_price=df_avg_weekly_price.first()['average_of_weekly_price']
average_of_monthly_price=df_avg_monthly_price.first()['average_of_monthly_price']
average_of_security_deposit=df_avg_security_deposit.first()['average_of_security_deposit']
average_of_cleaning_fee=df_avg_cleaning_fee.first()['average_of_cleaning_fee']
average_of_square_feet=df_avg_square_feet.first()['average_of_square_feet']
average_of_host_response_rate=df_avg_host_response_rate.first()['average_of_host_response_rate']
average_of_host_total_listings_count=df_average_of_host_total_listings_count.first()['average_of_host_total_listings_count']


# In[615]:


df_listing = df_listing.withColumn(
    'beds',
    f.when((f.isnan('beds') | f.isnull('beds')),int(average_of_beds)).otherwise(f.col('beds')))


df_listing = df_listing.withColumn(
    'bedrooms',
    f.when((f.isnan('bedrooms') | f.isnull('bedrooms')), int(average_of_bedrooms)).otherwise(f.col('bedrooms')))


df_listing = df_listing.withColumn(
    'bathrooms',
    f.when((f.isnan('bathrooms') | f.isnull('bathrooms')), int(average_of_bathrooms)).otherwise(f.col('bathrooms')))


df_listing = df_listing.withColumn(
    'weekly_price',
    f.when((f.isnan('weekly_price') | f.isnull('weekly_price')), int(average_of_weekly_price)).otherwise(f.col('weekly_price')))

df_listing = df_listing.withColumn(
    'monthly_price',
    f.when((f.isnan('monthly_price') | f.isnull('monthly_price')), int(average_of_monthly_price)).otherwise(f.col('monthly_price')))


df_listing = df_listing.withColumn(
    'security_deposit',
    f.when((f.isnan('security_deposit') | f.isnull('security_deposit')), int(average_of_security_deposit)).otherwise(f.col('security_deposit')))


df_listing = df_listing.withColumn(
    'cleaning_fee',
    f.when((f.isnan('cleaning_fee') | f.isnull('cleaning_fee')), int(average_of_cleaning_fee)).otherwise(f.col('cleaning_fee')))


df_listing = df_listing.withColumn(
    'square_feet',
    f.when((f.isnan('square_feet') | f.isnull('square_feet')), int(average_of_square_feet)).otherwise(f.col('square_feet')))


df_listing = df_listing.withColumn(
    'host_response_rate',
    f.when((f.isnan('host_response_rate') | f.isnull('host_response_rate')), int(average_of_host_response_rate)).otherwise(f.col('host_response_rate')))

df_listing = df_listing.withColumn(
    'host_total_listings_count',
    f.when((f.isnan('host_total_listings_count') | f.isnull('host_total_listings_count')), int(average_of_host_response_rate)).otherwise(f.col('host_response_rate')))

df_listing.cache()


# In[616]:


df_listing = df_listing.na.fill(average_of_host_response_rate, subset=['Host_response_rate'])


# In[617]:


df_listing.show(1,truncate=False)


# # Rename column-names

# In[618]:


current_col = df_listing.columns
new_col = [
    'Listing_id', 'Listing_name', 'Host_id', 'Host_name', 'Neighborhood', 'Latitude', 'Longitude',
    'Room_type', 'Price', 'Min_nights', 'Num_reviews', 'Last_review', 'Reviews_per_month','calculated_host_listings_count'
, 'Availability_365', 'Street_address',  'Host_response_time', 'Host_response_rate', 'Superhost_status','Host_listings_count',
     'Host_total_listings_count', 'Cleansed_neighborhood', 'Property_type',
    'Accommodates', 'Bathrooms', 'Bedrooms', 'Beds', 'Amenities', 'Square_feet', 'Weekly_price($)',
    'Monthly_price($)', 'Security_deposit($)', 'Cleaning_fee($)','Max_nights', 'Availability_30', 'Availability_60', 'Availability_90',
    'Review_scores_rating',  'Review_scores_cleanliness',
    'Review_scores_value', 'Instant_bookable','Cancellation_policy'
]


for old,new in zip(current_col,new_col):
    df_listing=df_listing.withColumnRenamed(old,new)
df_listing.cache()
df_listing.show(truncate=False)


# In[619]:


df_listing.printSchema()


# # Pricing Analysis:

# In[620]:


import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# In[621]:


#Average pricing per neighborhood.
Average_pricing_per_neighborhood=df_listing.groupBy('neighborhood').agg(f.avg(f.col('price')).alias('average_price'))
Average_pricing_per_neighborhood_pd=Average_pricing_per_neighborhood.toPandas()

Average_pricing_per_neighborhood.show()

# Explore pricing variations based on property type
plt.figure(figsize=(12, 6))
sns.boxplot(x='neighborhood', y='average_price', data=Average_pricing_per_neighborhood_pd)
plt.title('Average_pricing_per_neighborhood')
plt.xlabel('Neighborhood')
plt.ylabel('average_price')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.show()


# In[622]:


#Distribution of prices for different property types (entire home, private room, shared room).

avg_price_per_property=df_listing.groupBy('property_type').agg(f.avg(f.col('price')).alias('average_pricing'))
avg_price_per_property_pd=avg_price_per_property.toPandas()




avg_price_per_property.show()


# Average pricing variations based on property type
plt.figure(figsize=(12, 6))
sns.boxplot(x='property_type', y='average_pricing', data=avg_price_per_property_pd)
plt.title('Average Pricing Variation Based on Property Type')
plt.xlabel('Property Type')
plt.ylabel('Nightly Price')
plt.xticks(rotation=45, ha='right') 
plt.show()









# In[623]:


#Identify the most expensive and cheapest listings.

df_listing.orderBy(f.col('price').desc()).select(f.col('listing_name').alias('The most expensive listing')).limit(1).show(truncate=False)

df_listing.orderBy(f.col('price').asc()).select(f.col('listing_name').alias('The most cheapest listing')).limit(1).show(truncate=False)



# In[ ]:





# # Distribution of property types:

# In[624]:


Distribution_of_properites=df_listing.groupBy('property_type').count().orderBy('count',ascending=False)
Distribution_of_properites_pd=Distribution_of_properites.toPandas()

Distribution_of_properites.show()

sns.set(style="whitegrid")

# Plot the distribution of properties
plt.figure(figsize=(12, 6))
sns.barplot(x='count', y='property_type', data=Distribution_of_properites_pd, palette='viridis')
plt.title('Distribution of Properties Based on Property Type')
plt.xlabel('Number of Properties')
plt.ylabel('Property Type')
plt.show()


# In[625]:


correlation_result = df_listing.select(f.corr("square_feet", "price")).first()[0]
print(f"Correlation between square_feet and price: {correlation_result}")


# In[ ]:





# # Availability Analysis:

# In[626]:


#Average availability per neighborhood throughout the year


Average_availability_per_neighborhood =df_listing.groupBy('neighborhood')\
        .agg(f.avg(f.col('availability_365')).alias('avg_availability_year'))\
        .orderBy('avg_availability_year', ascending=False)


Average_availability_per_neighborhood_pd=Average_availability_per_neighborhood.toPandas()

Average_availability_per_neighborhood.show()

plt.figure(figsize=(12, 6))
plt.bar(Average_availability_per_neighborhood_pd['neighborhood'], Average_availability_per_neighborhood_pd['avg_availability_year'])
plt.title('Average Availability per Year by Neighborhood')
plt.xlabel('Neighborhood')
plt.ylabel('Average Availability per Year')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()





# In[627]:


#the relationship between minimum nights and availability.

relation=df_listing.select('min_nights','availability_365')
relation.summary().show()


# In[628]:


#Identify properties with high availability throughout the year.


# In[629]:


property_with_h_availability=df_listing.groupBy('property_type')\
        .agg(f.max('availability_365').alias('max_availability'))\
        .orderBy('max_availability',ascending=False)\


property_with_h_availability_pd=property_with_h_availability.toPandas()

property_with_h_availability.show()


# Plotting the data
plt.figure(figsize=(12, 6))
plt.bar(property_with_h_availability_pd['property_type'], property_with_h_availability_pd['max_availability'])
plt.title('Maximum Availability by Property Type')
plt.xlabel('Property Type')
plt.ylabel('Maximum Availability')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()




# # Host Analysis:

# In[630]:


#Distribution of the number of listings per host (top 10).

listings_per_host=df_listing.groupBy('host_name')\
    .agg(f.count('Listing_name').alias('listing_count'))\
    .orderBy('listing_count',ascending=False)



listings_per_host_pd=listings_per_host.toPandas()
listings_per_host_pd=listings_per_host_pd.head(10)
listings_per_host.show()

plt.figure(figsize=(12, 6))
plt.bar(listings_per_host_pd['host_name'], listings_per_host_pd['listing_count'])
plt.title('Number of Listings per Host (Top 10)')
plt.xlabel('Host Name')
plt.ylabel('Number of Listings')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()


# In[631]:


## Drop rows with NaN values in any column
#df_listing = df_listing.dropna()


# In[632]:


#Analyze the response time and rate of hosts.

df_rate_response=df_listing.groupBy('host_response_time') \
    .agg(
        f.avg(f.col('host_response_rate')).alias('avg_response_rate'),
    ) 


df_rate_response = df_rate_response.filter(~f.isnan(f.col('host_response_time'))).show()



# In[633]:


#Identify superhosts and their impact on pricing and reviews.

#average price for superhosts
superhosts = df_listing.filter(f.col('Superhost_status') == 'True')

avg_price_superhost=superhosts.groupBy('host_name')\
    .agg(f.avg('price')
    .alias('average_price'))\
    .orderBy('average_price',ascending=False)\
    .withColumnRenamed("host_name", "Super hosts")\

avg_price_superhost_pd=avg_price_superhost.toPandas()
avg_price_superhost_pd=avg_price_superhost_pd.head(10)
avg_price_superhost.show()



plt.figure(figsize=(12, 6))
plt.bar(avg_price_superhost_pd['Super hosts'], avg_price_superhost_pd['average_price'])
plt.title('Average Price for Superhosts (Top 10)')
plt.xlabel('Superhosts')
plt.ylabel('Average Price')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()





#average price for all hosts
average_price_for_all_hosts=df_listing.groupBy('host_name')\
    .agg(f.avg('price')\
    .alias('average_price'))\
    .orderBy('average_price',ascending=False)\
    .withColumnRenamed("host_name", "All_hosts")\
    

average_price_for_all_hosts_pd=average_price_for_all_hosts.toPandas()
average_price_for_all_hosts_pd=average_price_for_all_hosts_pd.head(10)
average_price_for_all_hosts.show()



# Plotting the data
plt.figure(figsize=(12, 6))
plt.bar(average_price_for_all_hosts_pd['All_hosts'], average_price_for_all_hosts_pd['average_price'])
plt.title('Average Price for All Hosts (Top 10)')
plt.xlabel('Hosts')
plt.ylabel('Average Price')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()






# In[634]:


#average review for superhosts
average_review_for_superhosts=superhosts.groupBy('host_name')\
    .agg(f.avg('Num_reviews')\
    .alias('average_reviews'))\
    .orderBy('average_reviews',ascending=False)\
    .withColumnRenamed("host_name", "Super hosts")\




average_review_for_superhosts_pd=average_review_for_superhosts.toPandas()
average_review_for_superhosts_pd=average_review_for_superhosts_pd.head(10)
average_review_for_superhosts.show()



plt.figure(figsize=(12, 6))
plt.bar(average_review_for_superhosts_pd['Super hosts'], average_review_for_superhosts_pd['average_reviews'])
plt.title('Average Number of Reviews for Superhosts (Top 10)')
plt.xlabel('Superhosts')
plt.ylabel('Average Number of Reviews')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.show()







#average review for all hosts
average_review_for_all_hosts=df_listing.groupBy('host_name')\
    .agg(f.avg('Num_reviews')\
    .alias('average_reviews'))\
    .orderBy('average_reviews',ascending=False)\
    .withColumnRenamed("host_name", "All_hosts")\


average_review_for_all_hosts_pd=average_review_for_all_hosts.toPandas()
average_review_for_all_hosts_pd=average_review_for_all_hosts_pd.head(10)
average_review_for_all_hosts.show()

plt.figure(figsize=(12, 6))
sns.barplot(x='average_reviews', y='All_hosts', data=average_review_for_all_hosts_pd, palette='viridis')
plt.title('Average Number of Reviews for All Hosts (Top 10)')
plt.xlabel('Average Number of Reviews')
plt.ylabel('Hosts')
plt.show()


# # Review Analysis:

# # Time Trends

# In[635]:


df_listing.cache()


# In[636]:


#Analyze trends over time for pricing, availability, and reviews.

last_review=df_listing.withColumn('review_year',f.year('last_review'))
last_review=last_review.withColumn('review_month',f.month('last_review'))


# In[637]:


trend_analysis = last_review.groupBy('review_year', 'review_month')\
    .agg({'price': 'avg', 'availability_365': 'avg', 'Num_reviews': 'count'})\
    .orderBy('review_year', 'review_month',ascending=False)

trend_analysis = trend_analysis.filter(f.col('review_year').isNotNull())
trend_analysis_pd=trend_analysis.toPandas()
trend_analysis.show()






plt.figure(figsize=(12, 6))

# Plot average price
plt.subplot(2, 1, 1)
sns.lineplot(x='review_year', y='avg(price)', data=trend_analysis_pd, marker='o', label='Average Price')
plt.title('Trend Analysis: Average Price Over Time')
plt.xlabel('Year')
plt.ylabel('Average Price')
plt.legend()

# Plot number of reviews
plt.subplot(2, 1, 2)
sns.lineplot(x='review_year', y='count(Num_reviews)', data=trend_analysis_pd, marker='o', label='Number of Reviews', color='orange')
plt.title('Trend Analysis: Number of Reviews Over Time')
plt.xlabel('Year')
plt.ylabel('Number of Reviews')
plt.legend()

plt.tight_layout()
plt.show()










# In[638]:


#Identify seasonal patterns in pricing


seasonal_analysis = df_listing.withColumn('review_months', f.month('last_review'))

seasonal_analysis = seasonal_analysis.groupBy('review_months')\
    .agg({'price': 'avg', 'availability_365': 'avg', 'Num_reviews': 'count'})\
    .orderBy( 'review_months',ascending=False)

seasonal_analysis = seasonal_analysis.filter(f.col('review_months').isNotNull())
seasonal_analysis_pd=seasonal_analysis.toPandas()
seasonal_analysis.show()


plt.figure(figsize=(12, 6))

# Plot average price
plt.subplot(2, 1, 1)
sns.lineplot(x='review_months', y='avg(price)', data=seasonal_analysis_pd, marker='o', label='Average Price')
plt.title('Trend Analysis: Average Price Over Time')
plt.xlabel('Month')
plt.ylabel('Average Price')
plt.legend()

# Plot number of reviews
plt.subplot(2, 1, 2)
sns.lineplot(x='review_months', y='count(Num_reviews)', data=seasonal_analysis_pd, marker='o', label='Number of Reviews', color='orange')
plt.title('Trend Analysis: Number of Reviews Over Time')
plt.xlabel('Month')
plt.ylabel('Number of Reviews')
plt.legend()

plt.tight_layout()
plt.show()





# In[639]:


df_listing.select('last_review').show()


# In[ ]:





# # Cancellation Policy Analysis:

# In[640]:


#Distribution of cancellation policies.


# In[641]:


cancellation_policy_distribution = df_listing.groupBy('cancellation_policy')\
.count()\
.orderBy('count', ascending=False)
cancellation_policy_distribution

cancellation_policy_distribution_pd=cancellation_policy_distribution.toPandas()

cancellation_policy_distribution.show(truncate=False)

# Plot distribution of cancellation policies.
plt.figure(figsize=(12, 6))
sns.barplot(x='count', y='cancellation_policy', data=cancellation_policy_distribution_pd, palette='viridis')
plt.title('Distribution of cancellation policies.')
plt.xlabel('Number of cancellation policies')
plt.ylabel('Cancellation policy')
plt.show()


# In[ ]:





# In[642]:


#the impact of cancellation policies on pricing and availability.

pricing_analysis = df_listing.groupBy("cancellation_policy").agg(
    f.avg("price").alias("average_price"),
    f.median("price").alias("median_price")
)


pricing_analysis_pd = pricing_analysis.toPandas()

# Plotting
plt.figure(figsize=(12, 6))

# Bar plot for average price
sns.barplot(x='cancellation_policy', y='average_price', data=pricing_analysis_pd, color='blue', label='Average Price')

# Bar plot for median price
sns.barplot(x='cancellation_policy', y='median_price', data=pricing_analysis_pd, color='orange', label='Median Price')

plt.title('Pricing Analysis by Cancellation Policy')
plt.xlabel('Cancellation Policy')
plt.ylabel('Price')
plt.legend()
plt.show()



# group by cancellation_policy and analyze availability
availability_analysis = df_listing.groupBy("cancellation_policy").agg(
    f.avg("availability_365").alias("average_availability"),
    f.median("availability_365").alias("median_availability")
)




availability_analysis_pd = availability_analysis.toPandas()

# Plotting
plt.figure(figsize=(12, 6))

# Bar plot for average price
sns.barplot(x='cancellation_policy', y='average_availability', data=availability_analysis_pd, color='blue', label='Average availability')

# Bar plot for median price
sns.barplot(x='cancellation_policy', y='median_availability', data=availability_analysis_pd, color='orange', label='Median availability')

plt.title('Availability Analysis by Cancellation Policy')
plt.xlabel('Cancellation Policy')
plt.ylabel('Availabilty')
plt.legend()
plt.show()





# show the results
pricing_analysis.show()
availability_analysis.show()


# # Spark MLib:Linear regression model to predict listing prices based on features like: bedrooms, bathrooms, amenities

# In[643]:


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder,CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator


# In[562]:


df=df_listing.withColumn('amenities_count',f.size(f.split(f.col('amenities'),',')))


# In[563]:


df.select('amenities_count').show()


# In[564]:


selected_columns=['bedrooms','bathrooms','amenities_count','price']


# In[565]:


df_selected=df.select(selected_columns)


# In[566]:


df_selected=df_selected.dropna()


# In[567]:


feature_columns=['bedrooms','bathrooms','amenities_count']


# In[568]:


assembler=VectorAssembler(inputCols=feature_columns,outputCol='features')
df_vectorized=assembler.transform(df_selected)


# In[569]:


df_vectorized.show()


# In[570]:


(training_data,testing_data)=df_vectorized.randomSplit([0.8,0.2],seed=42)


# In[571]:


lr=LinearRegression(featuresCol='features',labelCol='price')


# In[572]:


model=lr.fit(training_data)


# In[573]:


predictions = model.transform(testing_data)


# In[574]:


evaluator=RegressionEvaluator(labelCol='price',metricName='rmse')
rmse=evaluator.evaluate(predictions)
print(f'Root mean squared error :{rmse}')


# In[575]:


target_variable_range = df_listing.agg({"price": "max"}).collect()[0][0] - df_listing.agg({"price": "min"}).collect()[0][0]
HIGH=df_listing.agg({"price": "max"}).collect()[0][0]
LOW=df_listing.agg({"price": "min"}).collect()[0][0]

# Normalized RMSE
nrmse = rmse / target_variable_range
print(f"Normalized RMSE: {nrmse}")


# In[576]:


target_variable_range,HIGH,LOW


# In[577]:


param_grid=ParamGridBuilder().addGrid(lr.regParam,[0.1,0.01]).build()
crossval=CrossValidator(estimator=lr, estimatorParamMaps=param_grid,evaluator=evaluator,numFolds=3)
cv_model=crossval.fit(training_data)
best_model=cv_model.bestModel
best_model.save('best_model_regresion',overwrite=True)


# In[578]:


predictions_cv=best_model.transform(testing_data)


# In[579]:


rmse_cv=evaluator.evaluate(predictions_cv)
print(f"root mean squared error for cross validation: {rmse_cv}")





# In[646]:


df_listing=df_listing.drop('amenities')


# In[647]:


df_listing=df_listing.withColumn('last_review',f.col('last_review').cast('string'))


# In[648]:


column_names=df_listing.columns
for column_name in column_names:
    df_listing = df_listing.withColumn(column_name, f.regexp_replace(f.col(column_name), '"', ''))


# In[ ]:


def write_df_to_csv(filename):
    num_output_files = 1  # Change this to the desired number
    df_listing = df_listing.repartition(num_output_files)

    # Write the DataFrame to a CSV file
    df_listing.write.csv(filename, mode="overwrite")


# In[ ]:


write_df_to_csv('listings_to_aws')


# In[ ]:


import os
def delete_success_file(success_file_path):
    # Check if the success file exists before attempting to delete it
    if os.path.exists(success_file_path):
        os.remove(success_file_path)
        print(f'Success file {success_file_path} deleted.')
    else:
        print(f'Success file {success_file_path} not found.')


# In[ ]:


delete_success_file('listings_to_aws/_SUCCESS')


# In[ ]:


def csv_file(directory_path):
    # List all files in the directory
    files = os.listdir(directory_path)

    # Filter for CSV files
    csv_files = [file for file in files if file.endswith('.csv')]

    # Check if there is exactly one CSV file
    if len(csv_files) == 1:
        csv_file_path = os.path.join(directory_path, csv_files[0])
        print(f'Path to CSV file: {csv_file_path}')
    else:
        print('Error: There should be exactly one CSV file in the directory.')
        
    return str(csv_file_path)    


# In[653]:


import boto3
import psycopg2
from botocore.exceptions import NoCredentialsError

# Replace these values with your own
aws_access_key_id = 'YOUR aws_access_key_id'
aws_secret_access_key = 'YOUR aws_secret_access_key'
redshift_user = 'YOUR redshift_user'
redshift_password = 'YOUR redshift_password'
redshift_host = 'YOUR redshift_host '
redshift_port = '5439'
redshift_db = 'YOUR redshift_data_base '
s3_bucket = 'YOUR s3_bucket'
local_csv_file = csv_file('listings_to_aws')
s3_key = 'amesterdam_airbnb/listings.csv'

# Upload local CSV file to S3
def upload_to_s3(local_file, bucket, s3_key, access_key, secret_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    try:
        s3.upload_file(local_file, bucket, s3_key)
        print(f"Upload successful: {local_file} to s3://{bucket}/{s3_key}")
        return True
    except FileNotFoundError:
        print(f"The file {local_file} was not found.")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

# Upload the local CSV file to S3
upload_to_s3(local_csv_file, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key)



