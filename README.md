# WeblogAnalysis

Use Pig and PigUnit to do Weblog Analysis as a Maven project

## Tools Used:
- Exploratory Data Analysis: Hortonworks Sandbox, Hive, HDFS, vim, sublime 
- Processing & Analysis: Pig, Piggybank, Datafu
- Testing & Validation: PigUnit, Junit, Java, Maven
- Version Control: Git/Github, https://github.com/johnnycaol/pig-maven.git

## Setup:
- 1. Import the src folder and pom.xml file as a Maven project in Eclipse
- 2. Install all the dependencies specified in the pom.xml file
- 3. unzip `2015_07_22_mktplace_shop_web_log_sample.log.gz`

## Project Goal:
- 1. Sessionize the web log by web log IP
- 2. Determine the average session time
- 3. Determine unique URL visits per session (i.e. count a hit to a unique URL only once per session)
- 4. Find the most engaged users/IPs (i.e. the IPs with the longest session times)

## Assumptions:
- 1. IP addresses can be used to uniquely identify users
- 2. Sessionize means aggregate all page hits by IP by a fixed time window rather than navigation
- 3. Unique URL visits means count a hit to a unique URL only once per session
- 4. Most engaged users means the IPs with session length greater than or equal to 95th percentile
- 5. If required data is missing or error, that log will be ignored

## Tests:
- 1. testSessionize()
- 2. testAvgSessionLength()
- 3. testAvgUrlCountPerSession()
- 4. testMostEngagedUserIps()

## Run:
- 1. Run PigUnitTest.java as a jUnit test. This will use the test data in test_data.log
- 2. To process the data provided in the WeblogChallenge repository, first, REGISTER piggybank and datafu jars in the pig scripts
- 3. Second, use pig command line by specifying the path to the log data,
e.g. `pig -x local -f sessionize.pig -param input='../data/2015_07_22_mktplace_shop_web_log_sample.log'`

## Results for the `2015_07_22_mktplace_shop_web_log_sample.log` dataset:
- average session time: 100.28 seconds
- unique URL visits per session: 8.31
- most engaged users: Top IPs based on quantile (generated in the output)

## Limitations and Future Work:
- IP addresses do not guarantee distinct users, user id, sessions will help
- Sessionize code repeated couple of times
- Explore the results with different settings, e.g. sessionize by navigation or different time intervals, and then compare the results
- Make use of other fields, e.g. user_agent to understand what device users are coming from
- Use visualization tools like Tableau or ELK to visualize the data to find potential patterns for better understanding of the data, e.g. time series patterns, sales event patterns, networks issues, difference between engaged users and not engaged users, device usage comparison, most popular url/section of site, etc.

