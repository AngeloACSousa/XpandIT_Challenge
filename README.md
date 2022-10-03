# xpand-it Challenge
![logo xpand-it](https://www.xpand-it.com/wp-content/uploads/2016/10/LogoXpandIT-2016.png)

This repo contains a solution for [xpand-it Spark2 Recruitment Challenge](https://github.com/bdu-xpand-it/BDU-Recruitment-Challenges/wiki/Spark-2-Recruitment-Challenge) where a Spark2 application (in Scala) was developed to processes 2 CSV files.



## How to use:

- 2 Variables in Main.Scala are used for the csv paths change these to their location(gps_user_reviews_path and gps_path);
  - Defaults values to googleplaystore/googleplaystore_user_reviews.csv and googleplaystore/googleplaystore.csv correspondingly.
- Run using the Main configuration;
  - This produces 3 output files (best_apps.csv, googleplaystore_cleaned, googleplaystore_metrics)
  
## General Notes

- Rows with NaN Values are dropped
- Part 1:
  - Non Numeric Row Values in Sentiment Polarity Column are filtered out
- Part 2:
  - Filter out the ratings which dont meet the criteria first then order them resulting in less computation
- Part 3:
  - If column "Size" contains a K or k is the value is read as kilobyte, thus the value is divided by 1024.
  - Otherwise just removes the char and cast the value to float.
  - When grouping by "App":
    - collect_set() function is used to remove duplicate categories
    - max() function is used for date and numeric types and first() function is used for strings
- Part 4:
  - Join the DataFrames from Part 1 and Part 3 using "App" Column
 
- Part 5:
  - Join the DataFrames from Part 1 and Part 3 using "App" Column
  - explode() function to make new rows with elements inside the list in column "Genres".
  
- Testing was not implemented.
  
