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

            service.updateTopic = function(myTopic) {
                for(var n = 0; n < service.topics.length; n++) {
                    var t = service.topics[n];
                    if(t.topic == myTopic.topic) {
                        var p = service.findPartition(t, myTopic.partition);
                        if(p) {
                            p.loading = p.loading ? p.loading + 1 : 1;
                            p.startOffset = myTopic.startOffset;
                            p.endOffset = myTopic.endOffset;
                            p.messages = myTopic.messages;

                            $timeout(function() {
                                p.loading -= 1;
                            }, 1000);
                        }
                    }
                }

            };

            service.updateTopics = function(topics) {
                angular.forEach(topics, function (topic) {
                    service.updateTopic(topic);
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