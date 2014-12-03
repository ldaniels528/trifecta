/**
 * Trifecta Consumers Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta');
    app.factory('Consumers', function($http) {
        var service = {
            "consumers": []
        };

        service.loadConsumerMapping = function () {
            return $http.get("/rest/getConsumerSet")
                .then(function (response) {
                    return response.data;
                });
        };

        // load the consumer mapping
        service.loadConsumerMapping().then(
            function (consumers) {
                service.consumers = consumers;
            });

        return service;
    });

})();