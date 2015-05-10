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

    app.run(function($rootScope, $log, $timeout, BrokerSvc, ConsumerSvc, TopicSvc, WebSockets) {
        $rootScope.version = "0.18.20";
        $rootScope.BrokerSvc = BrokerSvc;
        $rootScope.ConsumerSvc = ConsumerSvc;
        $rootScope.TopicSvc = TopicSvc;
        $rootScope.WebSockets = WebSockets;

        /******************************************************************
         *  Tab-related Methods
         ******************************************************************/

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

        /******************************************************************
         *  Date-related Methods
         ******************************************************************/

        $rootScope.getDateFormat = function(ts) {
            var now = new Date();
            var then = new Date(ts);

            var sameYear = then.getFullYear() == now.getFullYear();
            var sameMonth = then.getMonth() == now.getMonth();
            var sameDay = then.getDay() == now.getDay();

            // did the event occur today?
            if(sameYear && sameMonth && sameDay) return "HH:mm:ss Z";
            else if(sameYear && sameMonth) return "MM-dd HH:mm Z";
            else return "MM-dd-yyyy Z";
        };

    });

})();