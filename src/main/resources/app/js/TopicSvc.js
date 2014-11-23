/**
 * Trifecta Topic Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.factory('TopicSvc', function($http, $log, $modal, $q) {
        var service = {
            "topics": []
        };
        
        service.getTopics = function(hideEmptyTopics) {
            if(service.topics.length == 0) {
                // load the topics for the REST service
                return service.loadTopics().then(
                    function (topics) {
                        if (topics) service.topics = topics;
                        else console.log("No topic summaries found");
                        return service.getFilteredTopics(hideEmptyTopics);
                    },
                    function (err) {
                        $log.error(err);
                        return service.getFilteredTopics(hideEmptyTopics);
                    });
            }

            return service.getFilteredTopics(hideEmptyTopics);
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
        
        return service;
    });

})();