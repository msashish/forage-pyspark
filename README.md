# forage

## Getting local environment versions correct (jars etc)
    spark version = 3.0.1 (/usr/local/Cellar/apache-spark/3.0.1/)
    scala -version (= 2.12)
    python --version (= 3.7.3)

    brew install scala@2.12
    If you have other scala versions then,
        brew link scala@2.12 --force   
    
    
## Run a simple test code
    jdk 1.8.0_212  (switching jdk)
    Version of spark-excel (maven) = com.crealytics:spark-excel_2.12:0.13.4
    To simply run,
        spark-submit --packages com.crealytics:spark-excel_2.12:0.13.4 just_a_test.py


## To run some tasks (spark-submit includes pyspark python module at runtime)
    spark-submit --packages com.crealytics:spark-excel_2.12:0.13.4 big_data_tasks.py -f "/Users/sheelava/msashishgit/forage/input/ANZ_synthesised_transaction_dataset.xlsx" > output.log
