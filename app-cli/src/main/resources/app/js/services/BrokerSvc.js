/**
 * Broker Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('BrokerSvc', function ($http) {
            var service = {
                "brokers": []
            };

            service.getBrokerDetails = function () {
                return $http.get("/rest/getBrokerDetails")
                    .then(function (response) {
                        return response.data;
                    });
            };

            // pre-load the existing brokers
            service.getBrokerDetails().then(function(brokers) {
                service.brokers = brokers;
            });

            return service;
        });

})();