/**
 * Query Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('QuerySvc', function ($http) {
            var service = {};

            service.executeQuery = function (name, topic, queryString) {
                return $http({
                    url:"/rest/executeQuery",
                    method: "POST",
                    data: { "name": name, "topic": topic, "queryString": queryString },
                    headers: {'Content-Type': 'application/json'}
                }).then(function (response) {
                    return response.data;
                });
            };

            service.getQueriesByTopic = function (topic) {
                return $http.get("/rest/getQueriesByTopic/" + encodeURI(topic))
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.saveQuery = function (name, topic, queryString) {
                return $http({
                    url: "/rest/saveQuery",
                    method: "POST",
                    data: { "name": name, "topic": topic, "queryString": queryString },
                    headers: {'Content-Type': 'application/json'}
                }).then(function (response) {
                    return response.data;
                })
            };

            service.transformResultsToCSV = function (queryResults) {
                return $http({
                    url:"/rest/transformResultsToCSV",
                    method: "POST",
                    data: queryResults,
                    headers: {'Content-Type': 'application/json'},
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