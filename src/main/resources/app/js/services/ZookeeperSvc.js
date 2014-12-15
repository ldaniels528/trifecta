/**
 * Trifecta Zookeeper Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('ZookeeperSvc', function ($http) {
            var service = {};

            service.getZkData = function (path, format) {
                return $http.get("/rest/getZkData" + path + "/" + format)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getZkInfo = function (path) {
                return $http.get("/rest/getZkInfo" + path)
                    .then(function (response) {
                        return response.data;
                    });
            };

            service.getZkPath = function (path) {
                return $http.get("/rest/getZkPath" + path)
                    .then(function (response) {
                        return response.data;
                    });
            };

            return service;
        });

})();