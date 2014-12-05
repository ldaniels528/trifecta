/**
 * Trifecta Query Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('QuerySvc', function ($http) {
            var service = {};

            service.executeQuery = function (queryString) {
                return $http.get("/rest/executeQuery/" + encodeURI(queryString))
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getQueries = function () {
                return $http.get("/rest/getQueries")
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.saveQuery = function (name, queryString) {
                return $http({
                    url: "/rest/saveQueries",
                    method: "POST",
                    data: angular.toJson({ "name":name, "queryString":queryString }),
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'}
                }).then(function (response) {
                    return response.data;
                })
            };

            return service;
        });
})();