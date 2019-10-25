#!/bin/bash


echo '#################################';
echo '# CONFIGURING CASSANDRA #';
echo '#################################';

cqlsh -f cassandra-bootstrap.cql -u cassandra -p cassandra
