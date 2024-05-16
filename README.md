# WeatherData
Created this a data source pipeline using Open Weather Map API by retrieving temperataure data for 50 cities all over the world. 

The python script "lambda_function.py" is run periodically every hour using the AWS CloudWatch function in AWS Lambda which in turn updates the CSV File in AWS S3. This CSV file is then called up in Tableau as a data source and visualization is made out of the data.

This visualization is hosted at https://public.tableau.com/app/profile/kevin.merchant/viz/WeatherData_17123410892740/WeatherData

Visualization can only be seen for a particular date  in line graph so as to reduce the cluttering, suggestions welcomed for improving the viz.

Working on updating the visualization automatically at the moment, stay tuned!
