/**
 * Publish Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('PublishCtrl', ['$scope', '$log','$timeout', 'MessageSvc',
            function ($scope, $log, $timeout, MessageSvc) {

                $scope.keyFormats = ["ASCII", "Hex-notation", "EPOC", "UUID"];
                $scope.messageFormats = ["Avro", "Binary", "JSON"];
                $scope.messageBlob = {
                    "key": null,
                    "message": null,
                    "keyFormat": null,
                    "messageFormat": null
                };

                /**
                 * Publishes the message to the topic
                 * @param topic the destination topic
                 * @param msgBlob the message object
                 */
                $scope.publishMessage = function(topic, msgBlob) {
                    MessageSvc.publishMessage(topic, msgBlob.key, msgBlob.message, msgBlob.format).then(
                        function(response) {
                            resetMessageBlob();
                            $log.info("response = " + angular.toJson(response));
                        },
                        function(err) {
                            $scope.addError(err);
                        }
                    );
                };

                $scope.generateUUID = function() {

                };

                function resetMessageBlob() {
                    $scope.messageBlob = {
                        "message": null,
                        "modified": true
                    };
                }

            }]);

})();