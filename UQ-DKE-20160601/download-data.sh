#!/bin/bash

curl=`which curl`

# Identify some DBpedia resources:
declare -a Universities=(
University_of_Queensland \
University_of_Western_Australia \
University_of_Melbourne \
Monash_University \
University_of_New_South_Wales \
Australian_National_University \
University_of_Adelaide \
University_of_Sydney \
)

# Ensure data directories exist:
mkdir -p data/turtle
mkdir -p data/n-triples

# Download data for each resource in Turtle/N3 format:
for i in ${Universities[@]}; do
echo $curl http://dbpedia.org/data/$i.n3 \> data/turtle/$i.n3
$curl http://dbpedia.org/data/$i.n3 > data/turtle/$i.n3
done

# Download data form each resource in n-triples format:
for i in ${Universities[@]}; do
echo $curl http://dbpedia.org/data/$i.ntriples \> data/n-triples/$i.ntriples
$curl http://dbpedia.org/data/$i.ntriples > data/n-triples/$i.ntriples
done
