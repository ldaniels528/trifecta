/**
 * Decoder Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('DecoderSvc', function ($http) {
            var service = {};

            /**
             * Downloads the specified decoder schema
             * @param topic the given topic
             * @param schemaName the given schema name
             * @returns {*}
             */
            service.downloadDecoderSchema = function(topic, schemaName) {
                return $http.get("/rest/getDecoderSchemaByName/" + encodeURI(topic) + "/" + encodeURI(schemaName))
                    .success(function (data, status, headers, config) {
                        var blob = new Blob([data], {type: "application/json"});
                        var objectUrl = URL.createObjectURL(blob);
                        window.open(objectUrl);
                    }).error(function (data, status, headers, config) {
                        alert("Schema download failed")
                    });
            };

            /**
             * Retrieves the decoder associated to the specified topic
             * @param topic the specified topic
             * @returns {*}
             */
            service.getDecoderByTopic = function (topic) {
                return $http.get("/rest/getDecoderByTopic/" + encodeURI(topic))
                    .then(function (response) {
                        return response.data;
                    });
            };

            /**
             * Retrieves all decoders (regardless of topic)
             * @returns {*}
             */
            service.getDecoders = function () {
                return $http.get("/rest/getDecoders")
                    .then(function (response) {
                        var decoders = response.data;
                        return decoders.sort(function(a, b) {
                            var ta = a.topic.toLowerCase();
                            var tb = b.topic.toLowerCase();
                            return ta > tb ? 1 : ta < tb ? -1 : 0;
                        });
                    });
            };

            /**
             * Retrieves the specified schema for the given topic
             * @param topic the specified topic
             * @param schemaName the specified schema
             * @returns {*}
             */
            service.getDecoderSchema = function(topic, schemaName) {
                return $http.get("/rest/getDecoderSchemaByName/" + encodeURI(topic) + "/" + encodeURI(schemaName))
                    .then(function (response) {
                        return response.data;
                    });
            };

            /**
             * Saves the schema to the server
             * @param schema the given schema
             * @returns {*}
             */
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