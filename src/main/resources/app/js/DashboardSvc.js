/**
 * Trifecta Dashboard Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.factory('DashboardSvc', function($http, $log) {

        var service = { };

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