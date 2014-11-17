/**
 * Trifecta Application
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta', ['ngResource']);
    app.config(['$resourceProvider', function($resourceProvider) {
        // Don't strip trailing slashes from calculated URLs
        $resourceProvider.defaults.stripTrailingSlashes = false;
    }]);

    app.run(function($rootScope, DashboardSvc) {
        $rootScope.DashboardSvc = DashboardSvc;
    });

})();