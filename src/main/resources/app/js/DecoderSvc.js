/**
 * Decoder Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('DecoderSvc', function ($http) {
            var service = {};

            service.getDecoders = function () {
                return $http.get("/rest/getDecoders")
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.saveSchema = function (schema) {
                return $http({
                    url:"/rest/saveSchema",
                    method: "POST",
                    data: "schemaString=" + encodeURI(schema.schemaString),
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'}
                }).then(function (response) {
                    return response.data;
                });
            };

            return service;
        });
})();