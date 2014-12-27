/**
 * Web Socket Singleton Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('WebSockets', function ($location, $log, TopicSvc) {
            var service = {};

            // establish the web socket connection
            if (!window.WebSocket) {
                window.WebSocket = window.MozWebSocket;
            }

            var socket;
            if (window.WebSocket) {
                var endpoint = "ws://" + $location.host() + ":" + $location.port() + "/websocket/";
                $log.info("Connecting to websocket endpoint '" + endpoint + "'...");
                socket = new WebSocket(endpoint);
                // $log.info("socket = " + angular.toJson(socket));

                socket.onopen = function (event) {
                    // $log.info("onOpen: event = " + angular.toJson(event));
                };

                socket.onclose = function (event) {
                    // $log.info("onClose: event = " + angular.toJson(event));
                };

                socket.onmessage = function (event) {
                    handleMessage(event);
                };
            } else {
                alert("Your browser does not support Web Sockets.");
            }

            /**
             * Transmits the message to the server via web-socket
             * @param message the given message
             * @param scope the given scope
             */
            service.send = function (message, scope) {
                if (!window.WebSocket) {
                    scope.addErrorMessage("Web socket closed");
                    return false;
                }
                if (socket.readyState == WebSocket.OPEN) {
                    socket.send(message);
                    return true;
                } else {
                    scope.addErrorMessage("Web socket closed: readyState = " + socket.readyState);
                    return false;
                }
            };

            /**
             * Handles the incoming web socket message event
             * @param event the given web socket message event
             */
            function handleMessage(event) {
                if(event.data) {
                    var message = angular.fromJson(event.data);

                    // is it a sampling action?
                    if(message.action == "sample") {
                        //$log.info("sample: " + angular.toJson(message.data));
                        angular.element('#TrifectaMain').scope().$apply(function ($scope) {
                            $scope.setMessageData(message.data);
                        });
                    }

                    // is it a set of consumer deltas?
                    else if(message.action == "consumerDeltas") {
                        angular.element('#TrifectaMain').scope().$apply(function ($scope) {
                            TopicSvc.updateConsumers(message.data);
                        });
                    }

                    // is it a set of topic deltas?
                    else if (message.action == "topicDeltas") {
                        TopicSvc.updateTopics(message.data);
                    }

                    // unrecognized push event
                    else {
                        $log.warn("Message was unhandled: action = '" + message.action + "', data =" + angular.toJson(message.data));
                    }
                }
            }

            return service;
        });

})();