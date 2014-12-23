/**
 * Consumer Service
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
(function () {
    angular.module('trifecta')
        .factory('ConsumerSvc', function ($http) {
            var service = {
                "consumers": []
            };

            service.getConsumerDetails = function () {
                return $http.get("/rest/getConsumerDetails")
                    .then(function (response) {
                        var consumers = response.data;
                        return consumers.sort(function(a, b) {
                            var ta = a.topic.toLowerCase();
                            var tb = b.topic.toLowerCase();
                            return ta > tb ? 1 : ta < tb ? -1 : 0;
                        });
                    });
            };

            service.getConsumerMapping = function () {
                return $http.get("/rest/getConsumerSet")
                    .then(function (response) {
                        var consumerSet = response.data;
                        return consumerSet.sort(function(a, b) {
                            var ta = a.topic.toLowerCase();
                            var tb = b.topic.toLowerCase();
                            return ta > tb ? 1 : ta < tb ? -1 : 0;
                        });
                    });
            };

            service.updateConsumer = function(conUpdate) {
                var conTopic = findConsumerTopicByID(conUpdate);
                if(conTopic) {
                    var consumers = conTopic.consumers;
                    var myCon = findConsumerByID(consumers, conUpdate);
                    if(myCon) {
                        var detail = findConsumerDetails(myCon.details, conUpdate);
                        if (detail) {
                            detail.offset = conUpdate.offset;
                            detail.topicOffset = conUpdate.topicOffset;
                            detail.lastModified = conUpdate.lastModified;
                            detail.messagesLeft = conUpdate.messagesLeft;
                        }

                        // if consumer detail object was not found
                        else {
                            if(!myCon.details) {
                                myCon.details = [];
                            }
                            myCon.details.push(makeConsumerDetail(conUpdate));
                        }
                    }

                    // the consumer object was not found
                    else consumers.push(makeConsumer(conUpdate))
                }

                // the consumer topic object was not found, so add the entire structure
                else service.consumers.push(makeConsumerTopic(conUpdate));
            };

            service.updateConsumers = function (consumers) {
                angular.forEach(consumers, function (consumer) {
                    service.updateConsumer(consumer);
                });
            };

            function findConsumerTopicByID (conUpdate) {
                for(var n = 0; n < service.consumers.length; n++) {
                    var conTopic = service.consumers[n];
                    if (conTopic.topic == conUpdate.topic) {
                        if(!conTopic.consumers) {
                            conTopic.consumers = [];
                        }
                        return conTopic;
                    }
                }
                return null;
            }

            function findConsumerByID (consumers, conUpdate) {
                for(var n = 0; n < consumers.length; n++) {
                    var cons = consumers[n];
                    if(cons.consumerId == conUpdate.consumerId) return cons;
                }
                return null;
            }


            function findConsumerDetails (details, conUpdate) {
                for(var n = 0; n < details.length; n++) {
                    var detail = details[n];
                    if(detail.consumerId == conUpdate.consumerId && detail.partition == conUpdate.partition) return detail;
                }
                return null;
            }

            function makeConsumerTopic(conUpdate) {
                return {
                    "topic": conUpdate.topic,
                    "consumers": [
                        makeConsumer(conUpdate)
                    ]
                };
            }

            function makeConsumer(conUpdate) {
                return {
                    "consumerId": conUpdate.consumerId,
                    "details": [
                        makeConsumerDetail(conUpdate)
                    ]
                };
            }

            function makeConsumerDetail(conUpdate) {
                return {
                    "consumerId": conUpdate.consumerId,
                    "topic": conUpdate.topic,
                    "partition": conUpdate.partition,
                    "offset": conUpdate.offset,
                    "topicOffset": conUpdate.topicOffset,
                    "lastModified": conUpdate.lastModified,
                    "messagesLeft": conUpdate.messagesLeft
                };
            }

            // perform the loading of the topic-to-consumer mapping
            service.getConsumerMapping().then(
                function (consumers) {
                    service.consumers = consumers;
                });

            return service;
        });

})();