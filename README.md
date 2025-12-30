# New Relic load generator

timeout (seconds)
>> "Send all batches every x seconds"
This controls how often the engine connects to New Relic to deliver data via the ingest APIs.
This should be configured to synhtetisize the frequency your data *reports* to New Relic


duration (seconds)
>> "Batch duration"
Metrics are generated in batches over the dfeined duration. The number of batches generated will dependo n the duration. 


frequency 
>> "How many metric points per batch duration"



run_x_amount_times
>> "How many batches to send, 0 for unlimited"


## Running docker

1. Ensure you've created the `env_file` (beware, it shouldnt have double quotes in it at all, bug to fix there!)
2. Build the image: `docker build -t nr-signal-generator .`
3. Run the image specifying the env_file: `docker run --env-file env_file nr-signal-generator` 
