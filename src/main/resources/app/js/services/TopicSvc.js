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

            service.updateTopic = function(updatedTopic) {
                angular.forEach(service.topics, function(t) {
                    if(t.topic == updatedTopic.topic) {
                        // setup the loading sequence
                        (function() {
                            t.updating = t.updating ? t.updating + 1 : 1;
                            var count = t.updating;
                            $timeout(function () {
                                if (t.updating == count) t.updating = 0;
                            }, 15000);
                        })();

                        // update the topic with the delta information
                        t.totalMessages = updatedTopic.totalMessages;
                        var p = service.findPartition(t, updatedTopic.partition);
                        if(p) {
                            p.delta = updatedTopic.endOffset - p.endOffset;
                            p.startOffset = updatedTopic.startOffset;
                            p.endOffset = updatedTopic.endOffset;
                            p.messages = updatedTopic.messages;

                            (function() {
                                var lastDelta = p.delta;
                                $timeout(function () {
                                    if(p.delta == lastDelta) p.delta = null;
                                }, 60000);
                            })();
                        }
                    }
                });
            };

            service.updateTopics = function(updatedTopics) {
                angular.forEach(updatedTopics, function (updatedTopic) {
                    service.updateTopic(updatedTopic);
                });
            };

            service.findPartition = function(topic, partitionId) {
                var partitions = topic.partitions;
                for(var n = 0; n < partitions.length; n++) {
                    var p = partitions[n];
                    if (p.partition == partitionId) return p;
                }
                return null;
            };

            // pre-load the topics
            service.getTopics().then(function(topics) {
                $log.info("Retrieved " + topics.length + " topic(s)...");
                service.topics = topics.sort(function(a, b) {
                    var ta = a.topic.toLowerCase();
                    var tb = b.topic.toLowerCase();
                    return ta > tb ? 1 : ta < tb ? -1 : 0;
                });
            });

            return service;
        })
})();