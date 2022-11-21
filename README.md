Extract base data to train the model for propensity of Swipe, speaker and loan


Step 1 - No need to run this file but any SQL changes for model training data will have to be done in this file
query_V0.py - SQL to fetch data for swipe, speaker and loan

Step 2 - Run the below file
data_extract_V0.ipynb - run the file and extract the base data for each product

Step 3 - Run the below 3 files

model_V0_loan.ipynb - train the loan model

model_V0_swipe.ipynb - train the swipe model

model_V0_speaker.ipynb - train the speaker model


Extract base data for the current active base of merchants and run it through the tained models


Step 4 - Runnning the below file is not needed
query_inf.py - SQL to fetch data on the current active merchants

Step 5 - Run the below file
data_extract_inf_V0.ipynb - run the file and extract the base data for current active merchants

Step 6 - Run the below file
app_profiler.ipynb - running the file will generate the 'digital_merchants.csv' that will be used in the inference in Step 7

Step 7 - Run the below file
model_V0_inf.ipynb - running the model on the current active merchants and push the final result to BQ.
