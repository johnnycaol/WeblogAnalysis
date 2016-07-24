# WeblogAnalysis

Use Pig and PigUnit to do Weblog Analysis as a Maven project

## Setup:
- 1. Import the src folder and pom.xml file as a Maven project in Eclipse
- 2. Install all the dependencies specified in the pom.xml file
- 3. unzip `2015_07_22_mktplace_shop_web_log_sample.log.gz`

## Project Goal:
- 1. Sessionize the web log by web log IP
- 2. Determine the average session time
- 3. Determine unique URL visits per session (i.e. count a hit to a unique URL only once per session)
- 4. Find the most engaged users/IPs (i.e. the IPs with the longest session times)

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

## Results:
- average session time: 100.28 seconds
- unique URL visits per session: 8.31
- most engaged users: Top IPs based on quantile (generated in the output)
