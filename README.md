# New Relic Signal Generator

This signal generator reads configuration and signal models configured in the [Signal Generator Manager](https://github.com/jsbnr/nr-signal-generator-manager). Refer to the manager application docs for signal setup instructions.

The python script (generator.py) reads the configuration set by the manager from NerdStorage using a user API key you must provide. You also specify the document ID and package UUID in order to load the correct data. The generator runs indefinitely, looking for config updates at the end of each signal playback.

![Screenshot](https://github.com/jsbnr/nr-signal-generator-manager/raw/main/screenshot1.png)
## Running in Docker

1. Ensure you've created the `env_file` (beware, it shouldnt have double quotes in it at all, bug to fix there!)
2. Build the image: `docker build -t nr-signal-generator .`
3. Run the image specifying the env_file: `docker run --env-file env_file nr-signal-generator` 


## Running in EC2 Ubuntu

Checkout this rough ec2 (ubuntu) launch configuration if you want to run the generator on an ec2 instance: [ec2-user-data-example.txt](ec2-user-data-example.txt)