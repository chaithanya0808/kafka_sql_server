from kafka import  KafkaProducer
import sys, os, json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.functions import to_json, struct
import kafka