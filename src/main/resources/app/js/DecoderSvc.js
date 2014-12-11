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

            return service;
        });
})();