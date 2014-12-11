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

    app.run(function($rootScope, $log, $timeout, Consumers, Topics, WebSockets) {
        $rootScope.Consumers = Consumers;
        $rootScope.Topics = Topics;
        $rootScope.WebSockets = WebSockets;

        $rootScope.gloabalMessages = [];

        $rootScope.clearMessages = function () {
            $rootScope.gloabalMessages = [];
        };

        $rootScope.addError = function (err) {
            $rootScope.gloabalMessages.push({
                "type": "error",
                "text": (err.statusText != "")
                    ? "HTTP/" + err.status + " - " + err.statusText
                    : "General fault or communications error"
            });
            scheduleRemoval($rootScope.gloabalMessages);
        };

        $rootScope.addErrorMessage = function (messageText) {
            $rootScope.gloabalMessages.push({
                "type": "error",
                "text": messageText
            });
            scheduleRemoval();
        };

        $rootScope.addInfoMessage = function (messageText) {
            $rootScope.gloabalMessages.push({
                "type": "info",
                "text": messageText
            });
            scheduleRemoval();
        };

        $rootScope.addWarningMessage = function (messageText) {
            $rootScope.gloabalMessages.push({
                "type": "warning",
                "text": messageText
            });
            scheduleRemoval();
        };

        $rootScope.removeMessage = function (index) {
            $rootScope.gloabalMessages.splice(index, 1);
        };

        function scheduleRemoval() {
            var messages = $rootScope.gloabalMessages;
            var message = messages[messages.length - 1];

            $timeout(function() {
                var index = messages.indexOf(message);
                $log.info("Removing message[" + index + "]...");
                if(index != -1) {
                    $rootScope.removeMessage(index);
                }
            }, 10000 + $rootScope.gloabalMessages.length * 500);
        }

    });

})();