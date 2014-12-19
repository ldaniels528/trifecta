/**
 * Publish Controller
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .controller('PublishCtrl', ['$scope', '$log','$timeout', 'MessageSvc', 'TopicSvc',
            function ($scope, $log, $timeout, MessageSvc, TopicSvc) {

                $scope.keyFormats = ["ASCII", "Hex-Notation", "EPOC", "UUID"];
                $scope.messageFormats = ["ASCII", "Avro", "JSON", "Hex-Notation"];
                $scope.messageBlob = {
                    "topic": null,
                    "key": null,
                    "keyFormat": "UUID",
                    "keyAuto": true,
                    "message": null,
                    "messageFormat": null
                };

                /**
                 * Publishes the message to the topic
                 * @param blob the message object
                 */
                $scope.publishMessage = function(blob) {
                    if(!validatePublishMessage(blob)) {
                        return;
                    }

                    MessageSvc.publishMessage(blob.topic.topic, blob.key, blob.message, blob.keyFormat, blob.messageFormat).then(
                        function(response) {
                            //$scope.messageBlob.message = null;
                            $log.info("response = " + angular.toJson(response));
                            if(response.type == "error") {
                                $scope.addErrorMessage(response.message);
                            }
                            else {
                                $scope.addInfoMessage("Message published");
                            }
                        },
                        function(err) {
                            $scope.addError(err);
                        }
                    );
                };

                /**
                 * Validates the given message blob
                 * @param blob the given message blob
                 * @returns {boolean}
                 */
                function validatePublishMessage(blob) {
                    if(!blob.topic || isBlank(blob.topic.topic)) {
                        $scope.addErrorMessage("No topic specified")
                    }
                    else if(isBlank(blob.keyFormat)) {
                        $scope.addErrorMessage("No message key format specified");
                        return false;
                    }
                    else if(isBlank(blob.message)) {
                        $scope.addErrorMessage("No message body specified");
                        return false;
                    }
                    else if(isBlank(blob.messageFormat)) {
                        $scope.addErrorMessage("No message body format specified");
                        return false;
                    }
                    else return true;
                }

                function isBlank(s) {
                    return !s || s.trim().length == 0;
                }

                /**
                 * Watch for topic changes, then select the first non-empty topic
                 *
                $scope.$watch("TopicSvc.topics", function(topics) {
                    var topic = TopicSvc.findNonEmptyTopic(topics);
                    if(topic) {
                        $scope.messageBlob.topic = topic;
                    }
                });*/

            }]);

})();