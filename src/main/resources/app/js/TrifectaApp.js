/**
 * Trifecta Application
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    var app = angular.module('trifecta', ['hljs', 'ngResource', 'ui.bootstrap']);
    app.config(['$resourceProvider', function($resourceProvider) {
        // Don't strip trailing slashes from calculated URLs
        $resourceProvider.defaults.stripTrailingSlashes = false;
    }]);

    app.run(function($rootScope, $log, $timeout, ConsumerSvc, TopicSvc, WebSockets) {
        $rootScope.version = "0.18.7";
        $rootScope.ConsumerSvc = ConsumerSvc;
        $rootScope.TopicSvc = TopicSvc;
        $rootScope.WebSockets = WebSockets;

        $rootScope.gloabalMessages = [];
        $rootScope.tabs = [
            {
                "name": "Inspect",
                "contentURL" : "/app/views/inspect.htm",
                "imageURL": "/app/images/tabs/main/inspect-24.png",
                "active": false
            }, {
                "name": "Observe",
                "contentURL" : "/app/views/observe.htm",
                "imageURL": "/app/images/tabs/main/observe-24.png",
                "active": false
            }, {
                "name": "Publish",
                "contentURL" : "/app/views/publish.htm",
                "imageURL": "/app/images/tabs/main/publish-24.png",
                "active": false
            },{
                "name": "Query",
                "contentURL" : "/app/views/query.htm",
                "imageURL": "/app/images/tabs/main/query-24.png",
                "active": false
            }, {
                "name": "Decoders",
                "contentURL" : "/app/views/decoders.htm",
                "imageURL": "/app/images/tabs/main/decoders-24.png",
                "active": false
            }
        ];

        // select the default tab and make it active
        $rootScope.tab = $rootScope.tabs[0];
        $rootScope.tab.active = true;

        /**
         * Changes the active tab
         * @param index the given tab index
         * @param event the given click event
         */
        $rootScope.changeTab = function (index, event) {
            // deactivate the current tab
            $rootScope.tab.active = false;

            // activate the new tab
            $rootScope.tab = $rootScope.tabs[index];
            $rootScope.tab.active = true;

            if (event) {
                event.preventDefault();
            }
        };

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