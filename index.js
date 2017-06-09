#!/usr/bin/env node

'use strict';

const Path = require('path');
const _ = require('lodash');
const async = require('async');

var neo4j = require('neo4j-driver').v1;

var driver = neo4j.driver(process.env.NEO4J_HOST, neo4j.auth.basic(process.env.NEO4J_UID, process.env.NEO4J_PWD));

driver.onCompleted = function() {
    console.log('Successfully connected to Neo4J');
};

driver.onError = function(error) {
    console.log('Neo4J Driver instantiation failed', error);
};

var session = driver.session();



var listener = require('seneca')()
    .use('seneca-amqp-transport')
    .add('cmd:getNode,cuid:*,depth:*', function(message, done) {

        var fragmentTree = {};
        var fragmentContent = [];
        var fragmentModifier = [];

        // publication head (only used when results include a Publication node)
        var pubHead = {};
        var isPublication = false;

        // get node plus all (bi-directional) related nodes, except 'parent' nodes
        var queryString = "MATCH (startNode {cuid:'" + message.cuid + "'}) -[relation]- (childNode) WHERE NOT (startNode)<-[:USES]-(childNode) AND NOT (startNode)-[:BLUEPRINT_INSTANCE]->(childNode) RETURN startNode, childNode, relation ORDER BY relation.nodeOrder";

        session
            .run(queryString)

            .then(function(result) {

                async.eachSeries(result.records, function(record, callback) {

                    // Set start node data
                    if (!_.has(fragmentTree, 'cuid')) {

                        _.set(fragmentTree, 'type', record.get('startNode').labels[0].toLowerCase());
                        _.set(fragmentTree, 'cuid', record.get('startNode').properties.cuid);
                        _.set(fragmentTree, 'title', record.get('startNode').properties.title);
                    }

                    // Set start node modifier
                    if (record.get('childNode').labels.includes('Modifier')) {
                        if (record.get('childNode').properties.title != null) {
                            fragmentModifier.push(record.get('childNode').properties.title);
                        } else {
                            fragmentModifier.push(record.get('childNode').properties.name);
                        }
                        return callback();
                    }

                    // if this node is of type Publication, set publication details
                    if (record.get('startNode').labels.includes('Publication')) {

                        isPublication = true;

                        if (record.get('childNode').labels.includes('Kontext')) {
                            _.set(fragmentTree, 'kontext', record.get('childNode').properties.cuid);
                            return callback();
                        }
                        if (_.get(record.get('relation').properties, 'as', '') === 'description') {
                            _.set(pubHead, 'desc', record.get('childNode').properties.cuid);
                            _.set(fragmentTree, 'head', pubHead);
                            return callback();
                        }
                        if (_.get(record.get('relation').properties, 'as', '') === 'image') {
                            _.set(pubHead, 'image', record.get('childNode').properties.cuid);
                            _.set(fragmentTree, 'head', pubHead);
                            return callback();
                        }
                        if (_.get(record.get('relation').properties, 'as', '') === 'title') {
                            _.set(pubHead, 'title', record.get('childNode').properties.cuid);
                            _.set(fragmentTree, 'head', pubHead);
                            return callback();
                        }
                    }


                    if (message.depth > 0) {

                        // Fragment: call service again with childNode.cuid and depth-1
                        if (record.get('childNode').labels.includes('Fragment')) {

                            var msg = "cmd:getNode,cuid:" + record.get('childNode').properties.cuid + ",depth:" + (message.depth - 1);
                            listener.act(msg, (err, result) => {
                                if (err) {
                                    throw err;
                                }
                                if (_.size(result) > 0) {
                                    fragmentContent.push(result);
                                }
                                return callback();
                            });
                        }

                        // Content node: push to fragmentContent directly
                        else {

                            var childNode = {};
                            _.set(childNode, 'type', record.get('childNode').labels[0].toLowerCase());
                            _.set(childNode, 'cuid', record.get('childNode').properties.cuid);
                            fragmentContent.push(childNode);

                            // Content node: check for modifiers by calling service again with childNode.cuid and depth=0
                            var msg = "cmd:getNode,cuid:" + record.get('childNode').properties.cuid + ",depth:0";
                            listener.act(msg, (err, result) => {
                                if (err) {
                                    throw err;
                                }
                                if (_.has(result, 'mod')) {
                                    _.set(childNode, 'mod', _.get(result, 'mod'));
                                }
                                return callback();
                            });
                        }
                    } else {
                        return callback();
                    }
                }, function(err) {

                    session.close();

                    if (err) {
                        console.error(err);
                        return;
                    }

                    if (fragmentContent.length > 0) {
                        if (isPublication) {
                            _.set(fragmentTree, 'body', fragmentContent);
                        } else {
                            _.set(fragmentTree, 'cont', fragmentContent);
                        }
                    }
                    if (fragmentModifier.length > 0) {
                        _.set(fragmentTree, 'mod', fragmentModifier);
                    }

                    return done(null, fragmentTree);
                });
            })

            .catch(function(error) {
                console.log(error);
            });
    })

    .listen({
        type: 'amqp',
        pin: 'cmd:getNode,cuid:*,depth:*',
        url: process.env.AMQP_URL
    });

// Will never be called when quitting service...
driver.close();
