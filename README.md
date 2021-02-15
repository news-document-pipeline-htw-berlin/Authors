# Prerequisite
+ Java 8
+ MongoDB
+ Scala 2.12.7

# Quickstart

> git clone https://github.com/news-document-pipeline-htw-berlin/Authors \
> cd Authors \
> Edit inputUri and outputUri in src/main/scala/App.scala so it refers to the collection you want to read from and write to  \
> Make sure input MongoDB is structured according to cheat sheet \
> sbt run

# Input MongooDB Structure

| 0    |    _id  |                    
| ---- | ---- |
| 1    |  authors    |
| 2    |  crawl_time    |
| 3    |  description    |
| 4    |  entities    |
| 5    |  image_links    |
| 6    |  intro    |
| 7    |  keywords    |
| 8    |  keywords_extracted    |
| 9    |  lemmatizer    |
| 10    |  links    |
| 11    |  long_url    |
| 12    |  news_site    |
| 13    |  published_time   |
| 14    |  read_time    |
| 15    |  sentiments    |
| 16    |  short_url    |
| 17    |  text    |
| 18    |  testsum    |
| 19    |  title    |

# Output MongoDB Structure
| 0    |    _id  | type |
| ---- | ---- | ---- |
| 1    |  amountOfArticles   | integer|
| 2    |  amountOfArticlesPerDay    | list |
| 3    |  amountOfArticlesPerDepartment   | list |
| 4    |  amountOfArticlesPerWebsite    | map |
| 5    |  averageAmountOfSources    | double |
| 6    |  averageWords    | double | 
| 7    |  score    | double | 
| 8    |  sentimentPerDay    | list |
| 9    |  sentimentPerDepartment    | list |
| 10   |  wordCountOfLastTexts    | list |
