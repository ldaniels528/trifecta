/**
 * Trifecta Dashboard Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('DashboardSvc', function ($http) {
            var service = {};

            service.executeQuery = function (queryString) {
                return $http.get("/rest/executeQuery/" + encodeURI(queryString))
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.findOne = function (topic, criteria) {
                return $http.get("/rest/findOne/" + topic + "/" + encodeURI(criteria))
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getConsumers = function () {
                return $http.get("/rest/getConsumers")
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getConsumerMapping = function () {
                return $http.get("/rest/getConsumerSet")
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getMessage = function (topic, partition, offset) {
                return $http.get("/rest/getMessage/" + topic + "/" + partition + "/" + offset)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getLastQuery = function () {
                return $http.get("/rest/getLastQuery")
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

            service.getZkData = function (path, format) {
                return $http.get("/rest/getZkData" + path + "/" + format)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getZkInfo = function (path) {
                return $http.get("/rest/getZkInfo" + path)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getZkPath = function (path) {
                return $http.get("/rest/getZkPath" + path)
                    .then(function (response) {
                        return response.data;
                    });
            };

            return service;
        });

})();