/**
 * Trifecta Topic Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.factory('Topics', function($http, $log, $q, $timeout) {
        var service = {
            topics: []
        };
        
        service.getTopics = function() {
            return service.topics;
        };

        service.getFilteredTopics = function(hideEmptyTopics) {
            return service.topics.filter(function(topic) {
                return !hideEmptyTopics || topic.totalMessages > 0;
            });
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
            return service.topics.length > 0 ? service.topics[0] : null;
        };

        service.loadTopics = function() {
            return $http.get("/rest/getTopicSummaries")
                .then(function(response) {
                    return response.data;
                });
        };

        service.loadTopicsByName = function(topic) {
            return $http.get("/rest/getTopicsByName/" + topic)
                .then(function(response) {
                    return response.data;
                });
        };

        service.loadTopicDetails = function(topic) {
            return $http.get("/rest/getTopicDetails/" + topic)
                .then(function(response) {
                    return response.data;
                });
        };

        // load the topics
        service.loadTopics().then(function(topics) {
            $log.info("Retrieved " + topics.length + " topic(s)...");
            service.topics = topics;
        });

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

        service.findPartition = function(topic, partitionId) {
            var partitions = topic.partitions;
            for(var n = 0; n < partitions.length; n++) {
                var p = partitions[n];
                if (p.partition == partitionId) return p;
            }
            return null;
        };

        service.updateTopics = function(topics) {
            angular.forEach(topics, function (topic) {
                service.updateTopic(topic);
            });
        };
        
        return service;
    });

})();