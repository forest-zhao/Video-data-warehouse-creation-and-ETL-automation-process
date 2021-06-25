#import
mongoimport -d video -c videos --type csv --file video_data.csv --headerline
# ETL process
mongo ~/Documents/ETL.js