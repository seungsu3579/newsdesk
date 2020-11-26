#!/bin/bash

sudo docker run -idt -p 8888:8888 -v /home/ubuntu/newsdesk/model/:/workspace/ --rm --name=model konlpy:1.0 bash
sudo docker attach model
# sudo docker exec -it modcel bash