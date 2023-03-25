#!/bin/bash

/home/ubuntu/.local/bin/ray start --head --port=6380 --object-store-memory 5000000000 --system-config='{"automatic_object_spilling_enabled":true,"max_io_workers":4,"object_spilling_config":"{\"type\":\"filesystem\",\"params\":{\"directory_path\":\"/data\"}}"}'
