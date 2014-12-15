/**
 * Message Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('MessageSvc', function ($http) {
            var service = {};

            service.findOne = function (topic, criteria) {
                return $http.get("/rest/findOne/" + topic + "/" + encodeURI(criteria))
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

            service.getMessageKey = function (topic, partition, offset) {
                return $http.get("/rest/getMessageKey/" + topic + "/" + partition + "/" + offset)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.publishMessage = function(topic, key, message, format) {
                return $http({
                    url:"/rest/publishMessage/" + encodeURI(topic),
                    method: "POST",
                    data: { "key": key, "message" :message, "format": format },
                    headers: {'Content-Type': 'application/json'}
                }).then(function (response) {
                    return response.data;
                });
            };

            return service;
        })
})();