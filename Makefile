cnf ?= config.env
include $(cnf)
export $(shell sed 's/=.*//' $(cnf))


.PHONY: help

help: ## List Citizen TLKT make tasks
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help



get_flights: ## Scraped whats visible on the map and update the manifest
	python3 run.py get_flights --data_path "data" --start_time "1/1/2021" --end_time "9/1/2023"

scrape_flight_details: ## Scraped whats visible on the map and update the manifest
	python3 run.py scrape_flight_details --data_path "data" --kml_storage_path "/Volumes/easystore/Drones" --kind "kml"

scrape_flight_details_timestamped: ## Scraped whats visible on the map and update the manifest
	python3 run.py scrape_flight_details --data_path "data" --kml_storage_path "/Volumes/easystore/Drones" --kind "gpx"

scrape_flight_messages: ## Scraped whats visible on the map and update the manifest
	python3 run.py scrape_flight_details --data_path "data" --kml_storage_path "/Volumes/easystore/Drones" --kind "kml-messages"

compile_kml: 	
	python3 run.py compile_flights --data_path "data" --kml_storage_path "/Volumes/easystore/Drones" --out_path "/Volumes/easystore/Drones" --kind "kml"

compile_gpx: 	
	python3 run.py compile_flights --data_path "data" --kml_storage_path "/Volumes/easystore/Drones" --out_path "/Volumes/easystore/Drones" --kind "gpx"

geocode_kml: 	
	python3 run.py geocode --in_path '/Volumes/easystore/Drones/compiled-flight-data-kml.csv' --out_path '/Volumes/easystore/Drones/geocoded.csv'

geocode_gpx: 	
	python3 run.py geocode --in_path '/Volumes/easystore/Drones/compiled-flight-data-gpx.csv' --out_path '/Volumes/easystore/Drones/geocoded.csv'

add_census_and_reduce_resolution_of_gpx: ## spits out gpx with census data to easystore
	python3 run.py join --in_path '/Volumes/easystore/Drones' --kind "gpx" --limit "S"

add_census_and_reduce_resolution_of_kml:
	python3 run.py join --in_path '/Volumes/easystore/Drones' --kind "kml" --limit "none"

prepare_calls-for-service:
	python3 run.py xls2csv --in_path '/Volumes/easystore/Drones' --kind "calls-for-service" --geocode_folder "geocodio"

add_census_to_calls-for-sevice:
	#relies on data from 3p geocoder
	python3 run.py join3p --in_path '/Volumes/easystore/Drones/calls-for-service/calls-for-service.csv' --column "Address" --geocode_file "/Volumes/easystore/Drones/geocodio/calls-for-service-addresses_geocodio-9-1.csv"

# scrape_24h_logs: ## Get Incident Updates from the last day
# 	python3 run.py scrape_map --data_path "data"
# 	python3 run.py scrape_incidents --data_path "data" --timeframe "24h" 

# scrape_48h_logs: ## Get Incident Updates from the last day
# 	python3 run.py scrape_incidents --data_path "data" --timeframe "48h" 

# scrape_logs: ## Get Incident Updates
# 	python3 run.py scrape_incidents --data_path "data" --timeframe "all" 

# compile_logs: 
# 	python3 run.py compile_logs --data_path "data/logs" --out_path "data/outputs"

# pull_remote_logs: ## this only works if you have given dhruv your id_rsa.pub. Saves to data/output/
# 	scp -r root@139.59.147.91:~/tlkt/CTZN-TLKT/data/outputs/citizen_incidents.csv ./data/outputs/citizen_incidents.csv

# check_audio_progress: #Checks the Status of Media downloads by comparing the citizen incidents you've scraped with the audio files you've downloaded. It spits out a manifest that can be used later to pull new audio.
# 	python3 run.py check_audio_progress --data_path "data/outputs" --audio_path "/Volumes/easystore/Citizen/data/audio" 

# pull_dispatch_audio:
# 	make check_audio_progress
# 	python3 run.py pull_dispatch --data_path "data/outputs"

# pull_dispatch_text:
# 		python3 run.py transcribe_dispatch --data_path "data/outputs"  --open_ai_key ${OPEN_AI_KEY}
