/**
 * Web Socket Singleton Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('WebSockets', function ($log, Consumers, Topics) {
            var service = {};

            // establish the web socket connection
            if (!window.WebSocket) {
                window.WebSocket = window.MozWebSocket;
            }

            var socket;
            if (window.WebSocket) {
                $log.info("Connecting to websocket...");
                socket = new WebSocket("ws://localhost:8888/websocket/");
                $log.info("socket = " + angular.toJson(socket));

                socket.onopen = function (event) {
                    $log.info("onOpen: event = " + angular.toJson(event));
                };

                socket.onclose = function (event) {
                    $log.info("onClose: event = " + angular.toJson(event));
                };

                socket.onmessage = function (event) {
                    if(event.data) {
                        $log.info("onMessage: event = " + angular.toJson(event.data));
                    }
                };
            } else {
                alert("Your browser does not support Web Sockets.");
            }

            /**
             * Transmits the message to the server via web-socket
             * @param message the given message
             */
            service.send = function (message) {
                if (!window.WebSocket) {
                    return;
                }
                if (socket.readyState == WebSocket.OPEN) {
                    socket.send(message);
                } else {
                    $log.error("The socket is not open: readyState = " + socket.readyState);
                }
            };

            return service;
        });

})();