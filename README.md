Extract base data to train the model for propensity of Swipe, speaker and loan



query_V0.py - SQL to fetch data for swipe, speaker and loan

data_extract_V0.ipynb - running the above file and extracting the base data for each product


model_V0_loan.ipynb - train the loan model

model_V0_swipe.ipynb - train the swipe model

model_V0_speaker.ipynb - train the speaker model


Extract base data for the current active base of merchants and run it through the tained models



query_inf.py - SQL to fetch data on the current active merchants

data_extract_inf_V0.ipynb - running the above file and extracting the base data for current active merchants

model_V0_inf.ipynb - running the model on the current active merchants and push the final result to BQ.
