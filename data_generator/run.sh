#!/bin/bash

python generate_countries.py &
python generate_products.py &
python generate_logs.py &

wait
