/**
 * Topic Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('TopicSvc', function ($http, $log, $q, $timeout) {
            var service = {
                topics: []
            };

            service.getFilteredTopics = function(hideEmptyTopics) {
                return service.topics.filter(function(topic) {
                    return !hideEmptyTopics || topic.totalMessages > 0;
                });
            };

            /**
             * Filters out topics without messages; returning only the topics containing messages
             * @param topics the given array of topic summaries
             * @returns Array of topics containing messages
             */
            service.filterEmptyTopics = function (topics) {
                var filteredTopics = [];
                for (var n = 0; n < topics.length; n++) {
                    var ts = topics[n];
                    if (ts.totalMessages > 0) {
                        filteredTopics.push(ts);
                    }
                }
                return filteredTopics;
            };

            /**
             * Attempts to find and return the first non-empty topic; however, if none are found, it returns the
             * first topic in the array
             * @returns the first non-empty topic
             */
            service.findNonEmptyTopic = function() {
                for(var n = 0; n < service.topics.length; n++) {
                    var topic = service.topics[n];
                    if(topic.totalMessages > 0) {
                        return topic;
                    }
                }
                return service.topics.length ? service.topics[0] : null;
            };

            service.getReplicas = function (topic) {
                return $http.get("/rest/getLeaderAndReplicas/" + topic)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getTopics = function () {
                return $http.get("/rest/getTopicSummaries")
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getTopicsByName = function (topic) {
                return $http.get("/rest/getTopicsByName/" + topic)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getTopicDetails = function (topic) {
                return $http.get("/rest/getTopicDetails/" + topic)
                    .then(function (response) {
                        return response.data;
                    });
            };

            /**
             * {"consumerId":"dev","topic":"shocktrade.keystats.avro","partition":1,"offset":4910,"topicOffset":8388,"lastModified":1419391037744,"messagesLeft":3478}
             * @param updatedConsumers
             */
            service.updateConsumers = function (updatedConsumers) {
                angular.forEach(updatedConsumers, function(u) {
                    var t = service.findTopicByName(u.topic);
                    var consumers = t ? t.consumers : null;
                    if(consumers) {
                        var detail = service.findConsumerDetail(consumers, u);
                        if(detail) {
                            // update the consumer detail
                            detail.deltaT = Math.max(0, u.topicOffset - detail.topicOffset);
                            detail.deltaC = Math.max(0, u.offset - detail.offset);
                            detail.messagesLeft = u.messagesLeft;
                            detail.lastModified = u.lastModified;
                            detail.offset = u.offset;
                            detail.topicOffset = u.topicOffset;
                            detail.messagesLeft = u.messagesLeft;
                            detail.lastModified = u.lastModified;

                            // consumer & topic deltas should exist for 30 seconds (since the last update)
                            //setDeltaTimeOut(detail.deltaT);
                            //setDeltaTimeOut(detail.deltaC);
                        }
                    }
                });
            };

            service.updateTopic = function(updatedTopic) {
                angular.forEach(service.topics, function(t) {
                    if(t.topic == updatedTopic.topic) {
                        // setup the topic update indicator
                        setTopicUpdateIndicator(t);

                        // update the topic with the delta information
                        t.totalMessages = updatedTopic.totalMessages;
                        var p = service.findPartition(t, updatedTopic.partition);
                        if(p) {
                            p.delta = Math.max(0, updatedTopic.endOffset - p.endOffset);
                            p.startOffset = updatedTopic.startOffset;
                            p.endOffset = updatedTopic.endOffset;
                            p.messages = updatedTopic.messages;

                            // deltas should exist for 30 seconds (since the last update)
                            //setDeltaTimeOut(p.delta);
                        }
                    }
                });
            };

            service.updateTopics = function(updatedTopics) {
                angular.forEach(updatedTopics, function (updatedTopic) {
                    service.updateTopic(updatedTopic);
                });
            };

            service.findConsumerDetail = function(consumers, consumerDelta) {
                for(var n = 0; n < consumers.length; n++) {
                    if(consumers[n].consumerId == consumerDelta.consumerId) {
                        var details = consumers[n].details;
                        for(var m = 0; m < details.length; m++) {
                            if(details[m].partition == consumerDelta.partition) return details[m];
                        }
                    }
                }
                return null;
            };

            service.findTopicByName = function(topicName) {
                for(var n = 0; n < service.topics.length; n++) {
                    var t = service.topics[n];
                    if(t.topic == topicName) return t;
                }
                return null;
            };

            service.findPartition = function(topic, partitionId) {
                var partitions = topic.partitions;
                for(var n = 0; n < partitions.length; n++) {
                    var p = partitions[n];
                    if (p.partition == partitionId) return p;
                }
                return null;
            };

            function setDeltaTimeOut(entity) {
                // deltas should be ephemeral
                if(entity.delta) {
                    (function () {
                        var lastDelta = entity.delta;
                        $timeout(function () {
                            if (entity.delta == lastDelta) entity.delta = 0;
                        }, 5000);
                    })();
                }
            }

            function setTopicUpdateIndicator(topic) {
                if(!topic.updatingTopics) topic.updatingTopics = 0;

                topic.updatingTopics++;
                $timeout(function () {
                    topic.updatingTopics--;
                }, 5000);
            }

            // pre-load the topics
            service.getTopics().then(function(topics) {
                $log.info("Retrieved " + topics.length + " topic(s)...");
                var sortedTopics = topics.sort(function(a, b) {
                    var ta = a.topic.toLowerCase();
                    var tb = b.topic.toLowerCase();
                    return ta > tb ? 1 : ta < tb ? -1 : 0;
                });

                angular.forEach(sortedTopics, function(t) {
                    t.consumers = [];
                    service.topics.push(t);
                });
            });

            return service;
        })
})();