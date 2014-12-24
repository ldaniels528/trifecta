/**
 * Consumer Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('ConsumerSvc', function ($http) {
            var service = {
                "consumers": []
            };

            service.getConsumersByTopic = function (topic) {
                return $http.get("/rest/getConsumersByTopic/" + encodeURI(topic))
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getConsumerDetails = function () {
                return $http.get("/rest/getConsumerDetails")
                    .then(function (response) {
                        return response.data;
                    });
            };

            // pre-load the existing consumers
            service.getConsumerDetails().then(function(consumers) {
                service.consumers = consumers;
            });

            return service;
        });

})();