#!/bin/bash

# Start an infinite loop
while true; do
    # Run the specified Python script
    python3 /home/ubuntu/Americold.i3pl.DataLake.Integrations/ec2/xmlparser/code/orion_xml_json_converter.py

    # Sleep for 60 seconds before the next iteration
    sleep 60
done
