/**
 * Trifecta Dashboard Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.factory('DashboardSvc', function($http) {
        var service = { };

        service.getConsumers = function() {
            return $http.get("/rest/getConsumers")
                .then(function(response) {
                    return response.data;
                });
        };

        service.getConsumerMapping = function() {
            return $http.get("/rest/getConsumerMapping")
                .then(function(response) {
                    return response.data;
                });
        };

        service.getDecoders = function() {
            return $http.get("/rest/getDecoders")
                .then(function(response) {
                    return response.data;
                });
        };

        service.getMessage = function(topic, partition, offset, decoder) {
            return $http.get("/rest/getMessage/" + topic + "/" + partition + "/" + offset + "/" + decoder)
                .then(function(response) {
                    return response.data;
                });
        };

        service.getTopics = function() {
            return $http.get("/rest/getTopicSummaries")
                .then(function(response) {
                    return response.data;
                });
        };

        service.getTopicsByName = function(topic) {
            return $http.get("/rest/getTopicsByName/" + topic)
                .then(function(response) {
                    return response.data;
                });
        };

        service.getTopicDetails = function(topic) {
            return $http.get("/rest/getTopicDetails/" + topic)
                .then(function(response) {
                    return response.data;
                });
        };

        return service;
    });

})();