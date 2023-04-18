# verve-group
This repo is related to verve group tasks. 
# About project
I separated tasks into two parts so two use-cases: part1 and part2

In part1, I did:

* 1. Read json files of impressions and clicks
* 2. Left join impressions with clicks
* 3. groupBy and reduces to calculate metrics per dimensions
* 4. Write to json file

In part2, the general idea to recommend advertiser is to calculate performance of advertiser per app_id and per country_code
separately and then merge the sorted performance together and recommend the top five of the merged advertiser list
per app_id and country_code. 
* For calculation of performance/score I used _**window function**_ to facilitate process.
* Also note that for recommendation, I first chose the top 5 of ( performance of advertiser per app_id) and then the top 5 of
(performance of advertiser per app_id) then merge these two list and again select the top 5 of them.
* So I assume these top five items are good enough to recommend.
* The reason why to choose top five in two steps one in each list and then in the merged list 
is to ensure if each of the lists are empty the other lists have elements. This is true because atleast 
the list of advertisers per country_code are more than five.

## Further improvement 

1. To increase recommendation precision, it is better to merge the two performance list then sort them descending and then select top five

2. Also, the performance of each advertiser per app_id and country_code should be calculated and then be merged with other two lists.

3. All calculations here are offline but in real case it should be near online such that when a new impression comes or a new click occurred,
the performance metrics should be recalculated for that specific advertiser, and these calculations should be stored and renewed for recent events
forexample last 1 week because the recent behavior has more impact. It is another idea to give weight to performance calculation such that
recent calc has more weigh than the last month 
## Performance formula
Performance formula is the average revenue obtained from an advertiser per app_id or country_code:
The average revenue per advertiser is : 
```
(advertiser_total_revenue per app_id or country_code) / (advertiser_total_impression per app_id or code)
```


# How to run: 
In order to run:
0. Use jdk-11
1. mv src/main/resources/application.template.conf src/main/resources/application.conf
2. For the first part: 
```shell
sbt run
```
3. For the second part:
```shell
sbt run
```

In order to run with spark docker, first run spark:
```shell
./spark_runner.sh

```
then create jar file of the project and copy it manually ro spark-master:
```shell
sbt package
docker cp target/scala-2.13/verve-group_2.13-0.0.1.jar spark-master:/
```
then run 
```shell
./submit_task.sh
```

**Note that json4s library used in this project is compatible with jdk11.**
