/**
 * Trifecta Query Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('QuerySvc', function ($http) {
            var service = {};

            service.executeQuery = function (queryString) {
                return $http({
                    url:"/rest/executeQuery",
                    method: "POST",
                    data: "queryString=" + encodeURI(queryString),
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'}
                }).then(function (response) {
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
                    url: "/rest/saveQuery",
                    method: "POST",
                    data: "name=" + encodeURI(name) + "&queryString=" + encodeURI(queryString),
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'}
                }).then(function (response) {
                    return response.data;
                })
            };

            service.transformResultsToCSV = function (queryResults) {
                return $http({
                    url:"/rest/transformResultsToCSV",
                    method: "POST",
                    data: "queryResults=" + encodeURI(angular.toJson(queryResults)),
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                    responseType: 'arraybuffer'
                }).success(function (data, status, headers, config) {
                    var blob = new Blob([data], {type: "text/csv"});
                    var objectUrl = URL.createObjectURL(blob);
                    window.open(objectUrl);
                }).error(function (data, status, headers, config) {
                    alert("CSV Download failed")
                });
            };

            return service;
        });
})();