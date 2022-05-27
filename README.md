# honey-faucet-rs

## DESIGN
<p>Event-Driven Data Warehouse Creation for NLP and on-chain analytics, with a focus on NFT data. </br></br>
A 'flow' is an ETL pipeline sequence that generates an output, in the form of either transformed data or analysis. </br>
A 'flow_step' signifies an operation on set of data. (copy, move, transform, ...) </br>
The modular design allows for scalable, custom ETL pipeline creation by chaining together relevant processes. </br>
</p>

<p align="center" width="15%" size="50%">
   <img src="work/db_design_flows.png">  
</p>

## CLI
<p>The CLI tool is designed to query specific subjects of interest prior to setting up a pipeline. </br>
The current set of supported actions hit the twitter v2 api endpoint. </br></br>

Suggested Use (Topic Search): </br>
- The [Counts] command should be used to gauge how much data there is for a particular topic. </br>
- The [Recent] command should be used to view the first 100 results of raw tweet data for a particular topic. </br>
- At this point, if enough data is available for a particular topic, a flow can be set up for it. </br>
- Otherwise, one can do a deep dive using the [Tweet Lookup], [User Timeline], [Mentions Timeline], or [Users Lookup] commands. </br></br>

Suggested Use (Account R&D - Persons/Projects of Interest): </br>
- If the unique user name is known, then the user_id can be found using the [Users Lookup] command. </br>
- Next, the [User Timeline] or [Mentions Timeline] commands can be used to view a portion of the timeline data for a particular user. </br>
</p>


### Recent Command
<p align="center" width="15%" size="50%">
   <img src="work/cli_recent_test_run.png">  
</p>

### Counts Command
<p align="center" width="15%" size="50%">
   <img src="work/cli_counts_test_run.png">  
</p>

### Tweet Lookup Command
<p align="center" width="15%" size="50%">
   <img src="work/cli_tweet_lookup_test_run.png">  
</p>

### User Timeline Command
<p align="center" width="15%" size="50%">
   <img src="work/cli_user_timeline_test_run.png">  
</p>

### Users Lookup Command
<p align="center" width="15%" size="50%">
   <img src="work/cli_users_lookup_test_run.png">  
</p>

## SUPPORTED FEATURES
<p>nlp-recent-topic-land - this flow step will pull and land recents data for a topic.</br>
nlp-user-timeline-land - this flow step will pull and land standard timeline data for a particular user. (WIP) </br>
nlp-topic-land - this flow step will pull and land data specified by date for a topic. (BLOCKED - R&D for v1.1 endpoint for archive search) </br>
</p>

## NLP Strategy
<p>Current NFT Sentiment Analysis Design: </br>
- Collect data for an initial Training and Test Set. </br>
- Perform standard sentiment analysis on tweet text for a particular topic. </br>
- R&D to include tweet impressions in analysis (likes + retweets + comments) </br>
- Aggregate Persons of Interest as separate topics and include them for the analysis. (EX. Do Kwon for topic = "LUNA") </br>
---- Avoid shitposters, anime pfps, and trolls </br>
- Aggregate Projects of Interest as separate topics and include them for the analysis. (EX: Cyberkongz,Nansen.ai,LooksRare for topic = "NFT") </br>
- R&D on analysis tuning
</p>

### NLP PIPELINE DESIGN
<p>The current design will consist of a 4-step Flow. </br>
- The first step will be called nlp-topic-land. </br>
---- This step will pull data down from Twitter and save it in a parquet file format. </br>
- The second step will handle the transformation of the data to remove duplicate tweets. (Not sure if this step is needed)</br>
- The third step will pre-process the text data. </br>
- The fourth step will make classifications on a data set based on various algorithms. (Naive Bayes, SVM, Logistic Regression, LSTM, etc...) </br>
- The idea is to be able to provide sentiment classification (positive / negative) for a particular topic for a particular interval (Past 24HR, Past 7D, Past 30D). </br>
</br>
</p>
