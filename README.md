# Prerequisites
+ Java 8
+ MongoDB

# Quickstart

> git clone https://github.com/news-document-pipeline-htw-berlin/Authors \
> cd Authors\
> Edit inputDBSettings and outputDBSettings in src/main/resources\
> Make sure input MongoDB is structured according to cheat sheet \
> sbt run

# MongoDB Cheat sheet

| id      | 0             | 1    | 2             | 3      | 4             | 5      | 6             | 7             | 8            | 9      | 10            | 11     | 12   | 13      | 14     | 15     |
| ------- | ------------- | ---- | ------------- | ------ | ------------- | ------ | ------------- | ------------- | ------------ | ------ | ------------- | ------ | ---- | ------- | ------ | ------ |
| element | Array[String] | Date | Array[String] | String | Array[String] | String | Array[String] | Array[String] | List[String] | String | Array[String] | String | Date | Integer | String | String |
