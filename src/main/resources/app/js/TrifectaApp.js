/**
 * Trifecta Angular.js Application
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta', ['hljs', 'ngResource', 'ui.bootstrap']);
    app.config(['$resourceProvider', function($resourceProvider) {
        // Don't strip trailing slashes from calculated URLs
        $resourceProvider.defaults.stripTrailingSlashes = false;
    }]);

    app.run(function($rootScope, Consumers, DashboardSvc, MessageSearchSvc, Topics, WebSockets) {
        $rootScope.Consumers = Consumers;
        $rootScope.DashboardSvc = DashboardSvc;
        $rootScope.MessageSearchSvc = MessageSearchSvc;
        $rootScope.Topics = Topics;
        $rootScope.WebSockets = WebSockets;
    });

})();