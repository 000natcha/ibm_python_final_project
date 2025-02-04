# Final Project
These are the files for the final project of the course ["Python Project for Data Engineering"](https://www.coursera.org/learn/python-project-for-data-engineering) offered by IBM on Coursera.

## Overview
### Project Scenario
> A multi-national firm has hired you as a data engineer. Your job is to access and process data as per requirements.<br>
> Your boss asked you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate information made available to you as a CSV file. You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

### Directions
> 1. Write a function to extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a data frame.
> 2. Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
> 3. Write a function to load the transformed data frame to an output CSV file.
> 4. Write a function to load the transformed data frame to an SQL database server as a table.
> 5. Write a function to run queries on the database table.
> 6. Run the following queries on the database table:<br>
>   a. Extract the information for the London office, that is Name and MC_GBP_Billion<br>
>   b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion<br>
>   c. Extract the information for New Delhi office, that is Name and MC_INR_Billion<br>
> 7. Write a function to log the progress of the code.
> 8. While executing the data initialization commands and function calls, maintain appropriate log entries.

## Files
### Banks.db
The resultant database file containing the transformed data frame.

### banks_project.py
The Python file containing all the codes used to achieve each task as given in the directions.

### code_log.txt
A log of actions taken during the progress of extracting, transforming, and loading the data, in the format of 'timestamp':'message'.

### exchange_rate.csv
The given .csv file containing the exchange rates used within the project.

### Largest_banks_data.csv
The resultant .csv file containing the transformed data frame. 
