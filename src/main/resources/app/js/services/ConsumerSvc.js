/**
 * Trifecta Consumer Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('ConsumerSvc', function ($http) {
            var service = {};

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

            return service;
        });

})();