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

            service.getDecoderSchema = function(topic, schemaName) {
                return $http.get("/rest/getDecoderSchemaByName/" + topic + "/" + schemaName)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.downloadDecoderSchema = function(topic, schemaName) {
                return $http.get("/rest/getDecoderSchemaByName/" + topic + "/" + schemaName)
                    .success(function (data, status, headers, config) {
                        var blob = new Blob([data], {type: "application/json"});
                        var objectUrl = URL.createObjectURL(blob);
                        window.open(objectUrl);
                    }).error(function (data, status, headers, config) {
                        alert("Schema download failed")
                    });
            };

            service.saveDecoderSchema = function (schema) {
                return $http({
                    url:"/rest/saveSchema",
                    method: "POST",
                    data: {
                        "topic":schema.topic,
                        "name":schema.name,
                        "schemaString": schema.schemaString
                    },
                    headers: {'Content-Type': 'application/json'}
                }).then(function (response) {
                    return response.data;
                });
            };

              return service;
        });
})();